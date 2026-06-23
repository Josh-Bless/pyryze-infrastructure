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

/**
 * Contract defining lock operation for tasks within or across application
 * 
 * @author Onyeche Joshua Blessing
*/ 
public interface RateLimiterLock{
    
    /**
     * Attempts to acquire a lock.
     *
     * @return a {@link reactor.core.publisher.Mono} emitting a lock release handle
     * if lock acquisition succeeds
     */
    Mono<RateLimiterUnlock> tryLock();
    
    /**
     * Contract defining unlock operation for an acquired lock.
     */
    public static interface RateLimiterUnlock{
        
        /**
         * Releases the acquired lock.
         *
         * @return a {@link reactor.core.publisher.Mono} emitting {@code true}
         * if unlock succeeds, otherwise {@code false}
         */
        Mono<Boolean> unlock();
    }
}