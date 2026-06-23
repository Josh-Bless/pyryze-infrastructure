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

package com.pyryze.infrastructure.uniqueid;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import com.pyryze.util.InMemoryLock;

/**
 * Represents a logical ID-generation node used by {@link UniqueId}.
 *
 * <p>Each node maintains:
 * <ul>
 *   <li>A lock used during sequence reset to prevent race conditions</li>
 *   <li>A sequence counter used to generate unique IDs within the same millisecond</li>
 * </ul>
 *
 * <p>Implementations are responsible for providing the node identifier via {@link #getId()}.
 * The node identifier must be unique across all active nodes and must fit within
 * {@link UniqueId#NODE_ID_BITS}.
 *
 * <p>This abstraction allows different storage backends for node metadata,
 * such as in-memory, Redis, database, etc.
 * 
 * @author Onyeche Joshua Blessing
 */
public abstract class UniqueIdNode{
    
    private final InMemoryLock resetSequenceLock = new InMemoryLock();
    private final AtomicInteger sequenceCounter = new AtomicInteger(0);
    
    /**
     * Lock used when resetting the sequence counter after exhaustion.
     *
     * @return in-memory lock for sequence reset synchronization
     */
    public final InMemoryLock getResetSequenceLock(){
        return resetSequenceLock;
    }
    
    /**
     * Sequence counter for this node.
     *
     * <p>The counter increments for each generated ID within the same timestamp unit
     * (millisecond). Once the counter exceeds the maximum allowed sequence value,
     * it must be reset after time advances.
     *
     * @return atomic sequence counter
     */
    public final AtomicInteger getSequenceCounter(){
        return sequenceCounter;
    }
    
    /**
     * Returns the unique identifier of this node.
     *
     * @return node identifier as a reactive publisher
     */
    abstract Mono<Long> getId();
}