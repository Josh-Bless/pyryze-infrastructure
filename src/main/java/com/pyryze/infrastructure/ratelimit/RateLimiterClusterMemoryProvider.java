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

import reactor.core.publisher.Mono;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.*;

import com.pyryze.util.InMemoryLock;
import com.pyryze.util.ValueBaseObjectProvider;

/**
 * An in-memory implementation of {@link RateLimiterClusterProvider}.
 *
 * <p>{@link RateLimiterClusterProvider} is primarily intended to provide
 * distributed implementations (for example Redis-backed implementations)
 * suitable for use across multiple application instances.
 * This implementation stores all state in memory and therefore provides
 * rate limiting only within the current JVM.</p>
 *
 * <p>As a result, this implementation is mainly intended for testing,
 * local development, or single-instance deployments.</p>
 * 
 * @author Onyeche Joshua Blessing
*/
public class RateLimiterClusterMemoryProvider implements RateLimiterClusterProvider{
    
    private static final Logger logger = LoggerFactory.getLogger(RateLimiterClusterMemoryProvider.class);
    
    private final ValueBaseObjectProvider<RateLimiterCluster> rateLimiterClusterProvider = new ValueBaseObjectProvider();
    
    /**
     * Initializes or retrieves an in-memory rate limiter cluster.
     *
     * @param clusterKey the cluster key identifying the cluster
     * @param clusterQualifier additional qualifier used for cluster differentiation
     * @return the associated {@link RateLimiterCluster}
     */
    public RateLimiterCluster clusterInit(String clusterKey,String clusterQualifier){
        
        String qualifiedName = clusterKey + "_" + clusterQualifier;
        
        return rateLimiterClusterProvider.getObject(qualifiedName,getRateLimiterClusterSupplier());
    }
    
    /**
     * Creates a supplier capable of producing new
     * {@link RateLimiterCluster} instances.
     *
     * @return a cluster supplier
     */
    Supplier<RateLimiterCluster> getRateLimiterClusterSupplier(){
        return () -> {
            InMemoryRateLimiterLock inMemoryRateLimiterLock = new InMemoryRateLimiterLock();
            
            InMemoryRateLimiterCounter inMemoryRateLimiterCounter = new InMemoryRateLimiterCounter();
            
            return new RateLimiterCluster(inMemoryRateLimiterCounter,inMemoryRateLimiterLock);
        };
    }
}

/**
 * In-memory implementation of {@link RateLimiterLock}.
 */
class InMemoryRateLimiterLock implements RateLimiterLock,RateLimiterLock.RateLimiterUnlock{
    
    private final InMemoryLock lock = new InMemoryLock();
    
    @Override
    public Mono<RateLimiterLock.RateLimiterUnlock> tryLock(){
        return lock.acquireLock(this)
        .then(Mono.just(this));
    }
    
    @Override
    public Mono<Boolean> unlock(){
        return lock.releaseLock(this);
    }
}

/**
 * In-memory implementation of {@link RateLimiterCounter}.
 *
 * <p>Counter state is maintained within the current JVM and supports
 * automatic expiration.</p>
 */
class InMemoryRateLimiterCounter implements RateLimiterCounter {
    
    private final InMemoryLock lock = new InMemoryLock();
    private final LongAdder counter = new LongAdder();
    private Instant expiryTime;
    
    @Override
    public Mono<Long> get(boolean absenceAsZero) {
        return Mono.fromSupplier(() -> {
            if (isExpired()) {
                return absenceAsZero ? 0L : null;
            }
            return counter.sum();
        });
    }

    @Override
    public Mono<Long> incrementWithExpirySetIfExpiryAbsent(Long value, Duration expiryDuration) {
        return lock.acquireLock(this)
        .then(Mono.defer(() -> {
            if (isExpired()) {
                reset();
            }
            counter.add(value);
            long newValue = counter.sum();
            if (expiryTime == null && expiryDuration != null) {
                expiryTime = Instant.now().plus(expiryDuration);
            }
            return Mono.just(newValue);
        }))
        .doOnTerminate(() -> lock.releaseLock(this));
    }

    @Override
    public Mono<Duration> getExpiry() {
        return Mono.fromSupplier(() -> {
            if (isExpired() || expiryTime == null) {
                return Duration.ZERO;
            }
            Duration expiry = Duration.between(Instant.now(), expiryTime);
            return expiry.isNegative()
                    ? Duration.ZERO
                    : expiry;
        });
    }

    @Override
    public Mono<Long> decrementIfWithinTime(Long value, Instant timeBarrier) {
        return lock.acquireLock(this)
        .then(Mono.defer(() -> {
            if (isExpired() || Instant.now().isAfter(timeBarrier)) {
                return Mono.just(counter.sum()); 
            }
            counter.add(-value);
            return Mono.just(counter.sum());
        }))
        .doOnTerminate(() -> lock.releaseLock(this));
    }
    
    /**
     * Determines whether the counter has expired.
     *
     * @return {@code true} if expired; otherwise {@code false}
     */
    private boolean isExpired() {
        return expiryTime != null && Instant.now().isAfter(expiryTime);
    }

    /**
     * Resets the counter and clears its expiry.
     */
    private void reset() {
        counter.reset();
        expiryTime = null;
    }
}
