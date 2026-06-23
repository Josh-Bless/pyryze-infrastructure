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

import java.util.function.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory implementation of {@link UniqueIdNodeProvider}.
 *
 * <p>This provider assigns node IDs sequentially using an in-memory counter.
 * It is primarily suitable for:
 * <ul>
 *   <li>Testing</li>
 *   <li>Single-JVM applications</li>
 *   <li>Non-distributed environments</li>
 * </ul>
 * 
 * @author Onyeche Joshua Blessing
 */
public class UniqueIdNodeMemoryProvider extends UniqueIdNodeProvider{
    
    private AtomicInteger nodeIds = new AtomicInteger(0);
    
    /**
     * Initializes a new node.
     *
     * <p>This implementation ignores the {@code nodeSelect} parameter.
     *
     * @param nodeSelect node selection key (ignored)
     * @return initialized unique ID node
     */
    public UniqueIdNode nodeInit(String nodeSelect){
        return initUniqueIdNode((long)nodeIds.incrementAndGet());
    }
    
    /**
     * Creates a {@link UniqueIdNode} with the given node ID.
     *
     * @param nodeId node identifier
     * @return initialized node
     */
    UniqueIdNode initUniqueIdNode(Long nodeId){
        return new UniqueIdNode(){
            public Mono<Long> getId(){
                return Mono.just(nodeId);
            }
        };
    }
}
