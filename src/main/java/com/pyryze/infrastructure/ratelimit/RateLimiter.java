/*
 * Copyright 2002-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package com.pyryze.infrastructure.ratelimit;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.AbstractMap.SimpleEntry;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.pyryze.infrastructure.exception.ExitReactPipelineException;

/**
* Reactively provides rate limiting to tasks within or across applications,
* thus ensuring scalability
* 
* @author Onyeche Joshua Blessing
*/
public class RateLimiter{
    
    private RateLimiterClusterProvider rateLimiterClusterProvider;
    
    /**
	 * Creates a new {@code RateLimiter} instance with a
	 * {@link com.pyryze.infrastructure.ratelimit.RateLimiterClusterProvider}
	 * used for cluster operations.
	 *
	 * @param rateLimiterClusterProvider the cluster provider to use
	 */
    public RateLimiter(RateLimiterClusterProvider rateLimiterClusterProvider){
        this.rateLimiterClusterProvider = rateLimiterClusterProvider;
    }
    
    /**
     * Attempts to execute a rate-limited task.
     * Task execution proceeds only if all counters associated with
     * {@code rateLimits} are below their respective limits.
     * 
     * <p>All counters associated with {@code rateLimits} are initially incremented
     * after confirming they are within limit, before task execution.
     * After task execution, these increments may be conditionally reverted
     * (resulting in no effective increase).
     * This design applies locking only during setup (rate check + increment),
     * not during task execution, allowing better parallel execution across
     * reactive contexts.</p>
     * 
     * <p>This approach was chosen over executing the task first and deciding
     * later whether to increment counters, because that would require keeping
     * locks during task execution to avoid race conditions. For heavy tasks,
     * such locking could significantly delay concurrent execution.</p>
     * 
     * <p>No associated counter remains incremented if an exception occurs
     * during task execution.
     * A counter remains incremented only if its {@code RateLimit.name}
     * exists in
     * {@link com.pyryze.infrastructure.ratelimit.RateLimiter.RateActionResponse#shouldIncrementRateLimits()}.
     * </p>
     *
     * @param rateLimits the rate limits whose associated counters are checked
     *                   before execution
     * @param action the task to execute
     * @param clusterQualifier additional qualifier for cluster resolution
     * @param <T> the task response type
     * @return a {@link reactor.core.publisher.Mono} emitting the task result
     * @throws com.pyryze.infrastructure.ratelimit.RateLimitException
     *         if any rate limit is exceeded
    */ 
    public <T> Mono<T> execute(
        Set<RateLimit> rateLimits, 
        Function<Set<PreExecute>,Mono<RateActionResponse<T>>> action,
        String clusterQualifier
    ){
        
        Set<PreExecute>incrementedRateLimits = new HashSet<>();
        
        return Flux.fromIterable(rateLimits)
        .flatMap(rateLimit -> preExecute(rateLimit,clusterQualifier))
        .doOnNext(preExecute -> incrementedRateLimits.add(preExecute))
        .onErrorResume(e -> {
            ExitReactPipelineException exit = new ExitReactPipelineException("FROM_RATE_LIMITER",e);
            return Flux.error(exit);
        })
        .then(Mono.defer(() -> action.apply(new HashSet<>(incrementedRateLimits))))
        .map(rateActionResponse -> new RateActionResponseWrap<T>(rateActionResponse,null))
        .onErrorResume(e -> {
            RateActionResponse<T> rateActionResponse = new RateActionResponse<>(Set.of(),null);
            return Mono.just(new RateActionResponseWrap<T>(rateActionResponse,e));
        })
        .switchIfEmpty(Mono.defer(() -> {
            RateActionResponse<T> rateActionResponse = new RateActionResponse<>(Set.of(),null);
            return Mono.just(new RateActionResponseWrap<T>(rateActionResponse,null));
        }))
        .flatMap(rateActionResponseWrap -> {
            
            RateActionResponse<T> rateActionResponse = rateActionResponseWrap.rateActionResponse();
            
            Set<PreExecute>decrements = incrementedRateLimits.stream()
            .filter(preExecute -> !rateActionResponse.shouldIncrementRateLimits().contains(preExecute.rateLimit().name()))
            .collect(Collectors.toCollection(HashSet::new));
            
            return decrementRateLimits(decrements,clusterQualifier)
            .then(Mono.defer(() -> {
                return rateActionResponseWrap.resultingException()!=null ? 
                Mono.error(rateActionResponseWrap.resultingException()) : 
                Mono.justOrEmpty(rateActionResponseWrap.rateActionResponse.response());
            }));
        })
        .onErrorResume(e -> {
            
            if(e.getMessage().startsWith("FROM_RATE_LIMITER") && e instanceof ExitReactPipelineException exit){
                return Mono.error(exit.<Throwable>getPayLoad());
            }
            return Mono.error(e)
            .thenReturn((T)null);
        });
    }
    
    private Mono<PreExecute> preExecute(RateLimit rateLimit, String clusterQualifier){
        
        RateLimiterCounter counter = getCounter(rateLimit.name(),clusterQualifier);
        
        RateLimiterLock lock = getRateLimiterLock(rateLimit.name(),clusterQualifier);
        
        Map<String,RateLimiterLock.RateLimiterUnlock>rateLimiterUnlockWrap = new HashMap<>();
        
        return lock.tryLock()
        .doOnNext(rateLimiterUnlock -> rateLimiterUnlockWrap.put("rateLimiterUnlock",rateLimiterUnlock))
        .flatMap(rateLimiterUnlock -> counter.get(true))
        .flatMap(count -> {
            Long resultCount = count + rateLimit.countDelta();
            if (Long.compare(resultCount,rateLimit.limit())==1) {
                RateLimitException e = new RateLimitException(String.format("Ratelimit : %s has reached its limit %s",rateLimit.name(), rateLimit.limit()));
                return derivePrexecute(counter,rateLimit,count)
                .doOnNext(e::setPreExecute)
                .then(Mono.justOrEmpty((Long)null))
                .then(Mono.error(e));
            }
            return counter.incrementWithExpirySetIfExpiryAbsent(rateLimit.countDelta(), rateLimit.duration())
            .flatMap(increasedCount -> derivePrexecute(counter,rateLimit,increasedCount));
        })
        .doOnTerminate(() -> {
            RateLimiterLock.RateLimiterUnlock rateLimiterUnlock = rateLimiterUnlockWrap.get("rateLimiterUnlock");
            if(rateLimiterUnlock!=null) rateLimiterUnlock.unlock().subscribe();
        });
    }
    
    private Mono<PreExecute> derivePrexecute(RateLimiterCounter counter, RateLimit rateLimit, Long currentRate){
        return counter.getExpiry()
        .map(expiry -> Instant.now().plusMillis(expiry.toMillis()))
        .map(afresh -> new PreExecute(rateLimit,afresh,currentRate));
    }
    
    private Flux<Long> decrementRateLimits(Set<PreExecute>decrements, String clusterQualifier){
        return Flux.fromIterable(decrements)
        .flatMap(preExecute -> {
            return getCounter(preExecute.rateLimit().name(),clusterQualifier)
            .decrementIfWithinTime(preExecute.rateLimit().countDelta(),preExecute.afresh());
        });
    }
    
    private RateLimiterCounter getCounter(String rateLimitName, String clusterQualifier){
        return rateLimiterClusterProvider.clusterInit(rateLimitName,clusterQualifier)
        .rateLimiterCounter();
    }
    
    private RateLimiterLock getRateLimiterLock(String rateLimitName, String clusterQualifier){
        return rateLimiterClusterProvider.clusterInit(rateLimitName,clusterQualifier)
        .rateLimiterLock();
    }
    
    /**
     * Represents the rate limit state of a task before it gets executed.
     *
     * @param rateLimit the associated {@code rateLimit}
     * @param afresh the time at which the rate counter associated with
     *               {@code rateLimit} will reset
     * @param currentRate the current counter value associated with
     *                    {@code rateLimit} before execution
     */
    public record PreExecute(
        RateLimit rateLimit,
        Instant afresh,
        Long currentRate
    ){}
    
    /**
     * Represents rate limit information for a task.
     *
     * @param name the name of this {@code rateLimit}
     * @param countDelta the increment that would be added to this
     *                   {@code rateLimit}'s associated counter as specified by
     *                   {@link com.pyryze.infrastructure.ratelimit.RateLimiter.RateActionResponse}
     * @param duration the time window during which the counter keeps accumulating
     *                 before being reset
     * @param limit the maximum counter value allowed before task execution is denied
     *              with {@link com.pyryze.infrastructure.ratelimit.RateLimitException}
    */
    public static record RateLimit(
        String name,
        Long countDelta, 
        Duration duration,
        Long limit
    ){}
    
    /**
     * The response of a task after execution.
     *
     * @param shouldIncrementRateLimits contains the names of {@code RateLimit}s
     *                                  whose associated counters should remain incremented
     * @param response the task result
     * @param <T> the task response type
    */
    public static record RateActionResponse<T>(
        Set<String>shouldIncrementRateLimits, 
        T response
    ){}
    
    private static record RateActionResponseWrap<T>(
        RateActionResponse<T> rateActionResponse, 
        Throwable resultingException
    ){}
}