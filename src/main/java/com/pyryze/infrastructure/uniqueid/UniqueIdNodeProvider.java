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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provider responsible for creating and managing {@link UniqueIdNode} instances.
 *
 * <p>This class acts as a node registry for {@link UniqueId}, ensuring that each
 * logical node selection key maps to a single {@link UniqueIdNode} instance.
 *
 * <p>Nodes are lazily initialized on first access using {@link #nodeInit(String)}
 * and then cached for subsequent reuse.
 *
 * <p>Implementations may back node creation using different storage strategies,
 * such as:
 * <ul>
 *   <li>In-memory storage</li>
 *   <li>Redis</li>
 *   <li>Database-backed allocation</li>
 *   <li>Distributed coordination services</li>
 * </ul>
 * 
 * @author Onyeche Joshua Blessing
 */
public abstract class UniqueIdNodeProvider{
    
    final Map<String,UniqueIdNode>uniqueIdNodes = new ConcurrentHashMap<>();
    
    /**
     * Returns the {@link UniqueIdNode} associated with the given selector.
     *
     * <p>If no node exists for the selector, a new one is created using
     * {@link #nodeInit(String)} and cached.
     *
     * @param nodeSelect logical node selection key
     * @return existing or newly initialized node
     */
    public final UniqueIdNode getUniqueIdNode(String nodeSelect){
        return uniqueIdNodes.computeIfAbsent(nodeSelect,k->nodeInit(nodeSelect));
    }
    
    /**
     * Initializes a new {@link UniqueIdNode} for the given selector.
     *
     * <p>This method is called only once per selector
     *
     * @param nodeSelect logical node selection key
     * @return initialized unique ID node
     */
    abstract UniqueIdNode nodeInit(String nodeSelect);
}