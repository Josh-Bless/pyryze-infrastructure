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
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.time.Duration;

/**
 * Contract defining counting operations for tasks within or across application
 * 
 * @author Onyeche Joshua Blessing
*/ 
public interface RateLimiterCounter{
    
    /**
     * Gets the current counter value.
     *
     * @param absenceAsZero whether absence of a counter should be treated as zero
     * @return a {@link reactor.core.publisher.Mono} emitting the current counter value
     */
    Mono<Long> get(boolean absenceAsZero);
    
    /**
     * Increments the counter by the specified value and sets expiry only if
     * no expiry currently exists.
     *
     * @param value the increment value
     * @param expiryDuration the expiry duration to apply if absent
     * @return a {@link reactor.core.publisher.Mono} emitting the updated counter value
     */
    Mono<Long> incrementWithExpirySetIfExpiryAbsent(Long value, Duration expiryDuration);
    
    /**
     * Gets the remaining expiry duration of the counter.
     *
     * @return a {@link reactor.core.publisher.Mono} emitting the remaining expiry duration
     */
    Mono<Duration> getExpiry();
    
    /**
     * Decrements the counter by the specified value only if the current time
     * is still within the provided time barrier.
     *
     * @param value the decrement value
     * @param timeBarrier the time threshold beyond which decrement should not occur
     * @return a {@link reactor.core.publisher.Mono} emitting the updated counter value
     */
    Mono<Long> decrementIfWithinTime(Long value, Instant timeBarrier);
}