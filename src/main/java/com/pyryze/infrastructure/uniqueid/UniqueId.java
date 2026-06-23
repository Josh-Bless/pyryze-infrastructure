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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.Duration;
import java.time.Clock;

import com.pyryze.util.InMemoryLock;


 /**
 * Distributed reactive unique ID generator.
 *
 * Bit manipulation Algorithmic design draws inspiration from the Snowflake ID generator originally developed by Twitter.
 * @see <a href="https://github.com/twitter/snowflake/tree/snowflake-2010">Twitter Snowflake</a>
 *
 * <p>ID structure (64-bit signed long):
 *
 * <pre>
 * | 1 unused sign bit | 41-bit timestamp | 10-bit node ID | 12-bit sequence |
 * </pre>
 *
 * <ul>
 *   <li><b>1 unused MSB</b>: Left unused to keep generated IDs non-negative</li>
 *   <li><b>41-bit timestamp</b>: Milliseconds elapsed since {@link #rootTimeStamp}</li>
 *   <li><b>10-bit node ID</b>: Distinguishes generator nodes (0–1023)</li>
 *   <li><b>12-bit sequence</b>: Distinguishes IDs generated within same millisecond (0–4095)</li>
 * </ul>
 *
 * <p>This layout supports:
 * <ul>
 *   <li>1024 unique nodes</li>
 *   <li>4096 IDs per millisecond per node</li>
 *   <li>~69 years of timestamp range</li>
 * </ul>
 *
 * <p>IDs are sortable by generation time because timestamp bits occupy the higher-order region.
 *
 * <p>This class is reactive and built for Project Reactor-based systems.
 * 
 * @author Onyeche Joshua Blessing
 */
public class UniqueId{
    
    private static final Logger logger = LoggerFactory.getLogger(UniqueId.class);
    /** Number of unused most-significant bits. */
    public static final int UNUSED_MSB = 1;

    /** Number of bits allocated for timestamp delta. */
    public static final int EPOCH_BITS = 41;

    /** Number of bits allocated for node ID. */
    public static final int NODE_ID_BITS = 10;

    /** Number of bits allocated for per-millisecond sequence. */
    public static final int SEQUENCE_BITS = 12;
    
    
    /** Maximum timestamp delta supported by allocated timestamp bits. */
    public static final long maxEpoch = (1L << EPOCH_BITS) - 1;

    /** Maximum node ID supported. */
    public static final long maxNodeId = (1L << NODE_ID_BITS) - 1;

    /** Maximum sequence number supported per millisecond. */
    public static final long maxSequence = (1L << SEQUENCE_BITS) - 1;
    
    /**
    * Custom epoch timestamp in milliseconds since Unix epoch.
    *
    * June 22, 2026 Noon UTC = 2026-06-22 12:22:03 UTC
    * 
    * <p>All generated IDs store time as:
    *
    * <pre>
    * currentTimeMillis - rootTimeStamp
    * </pre>
    *
    * This reduces the number of bits required for timestamps.
    */
    public static final long rootTimeStamp = 1782130923746L;

    private UniqueIdNodeProvider uniqueIdNodeProvider;
    private final Clock clock;
    private final BiFunction<String, ? super Throwable, Mono<Long>>handleException;
    
    /**
    * Creates a generator using system UTC clock.
    *
    * @param uniqueIdNodeProvider provider for node allocation
    * @param handleException custom exception handler
    */
    public UniqueId(
        UniqueIdNodeProvider uniqueIdNodeProvider,
        BiFunction<String, ? super Throwable, Mono<Long>> handleException
    ){
        this(uniqueIdNodeProvider,Clock.systemUTC(),handleException);
    }
    
    /**
    * Creates a generator.
    *
    * @param uniqueIdNodeProvider provider for node allocation
    * @param clock clock used for timestamp generation
    * @param handleException exception handler; if null, default logger-based handler is used
    */
    public UniqueId(
        UniqueIdNodeProvider uniqueIdNodeProvider,
        Clock clock,
        BiFunction<String, ? super Throwable, Mono<Long>> handleException
    ){
        this.uniqueIdNodeProvider = uniqueIdNodeProvider;
        this.clock = clock;
        this.handleException = handleException!=null ? handleException : (str,e) -> {
            logger.error(str,e);
            return Mono.error(e);
        };
    }
    
    /**
    * Returns elapsed milliseconds since {@link #rootTimeStamp}.
    *
    * @return timestamp delta
    * @throws IllegalStateException if timestamp bit capacity has been exhausted
    */
    long getTimestampDelta(){
        long timestampDelta = clock.millis() - rootTimeStamp;
        if(timestampDelta > maxEpoch){
            throw new IllegalStateException("Timestamp bit allocation has been exhausted. Consider upgrading library version to run with a fresh timestamp bit allocation for the next 69yrs");
        }
        return timestampDelta;
    }
    
    /**
    * Generates a unique ID as a string.
    *
    * @param nodeSelect node selection key
    * @return generated unique ID as string
    */
    public Mono<String> generateId(String nodeSelect){
        return generateIdNumber(nodeSelect)
        .map(String::valueOf);
    }
    
    /**
    * Generates a unique 64-bit numeric ID.
    *
    * <p>Generation steps:
    * <ol>
    *   <li>Resolve node ID</li>
    *   <li>Read current timestamp delta</li>
    *   <li>Increment node sequence counter</li>
    *   <li>Assemble final ID using bit shifting</li>
    * </ol>
    *
    * <p>If sequence capacity for the current millisecond is exhausted,
    * sequence reset logic is triggered.
    *
    * @param nodeSelect node selection key
    * @return generated unique ID
    */
    public Mono<Long> generateIdNumber(String nodeSelect){
        
        return getUniqueIdNodeId(nodeSelect)
        .flatMap(nodeId -> {
            logger.info("Generating unique id for node : {}",nodeId);
            
            long timestampDelta = getTimestampDelta();
            long sequence = uniqueIdNodeProvider.getUniqueIdNode(nodeSelect)
            .getSequenceCounter().incrementAndGet();
            
            if(sequence > maxSequence){
                return resetSequenceAndGenerateId(nodeSelect);
            }
            Long id = timestampDelta << (NODE_ID_BITS + SEQUENCE_BITS)
                | (nodeId << SEQUENCE_BITS)
                | sequence;
            return Mono.just(id);
        })
        .onErrorResume(e -> {
            String msg = "unable to generate uniqueId : ["+e.getMessage()+"]";
            return handleException.apply(msg,e);
        });
    }
    
    /**
    * Resets sequence counter after sequence exhaustion and retries ID generation.
    *
    * <p>This method:
    * <ul>
    *   <li>Acquires reset lock</li>
    *   <li>Waits until clock advances to next millisecond</li>
    *   <li>Resets sequence counter to zero</li>
    *   <li>Retries ID generation</li>
    * </ul>
    *
    * <p>A clock drift check is performed to ensure time moved forward.
    *
    * @param nodeSelect node selection key
    * @return newly generated ID
    */ 
    Mono<Long> resetSequenceAndGenerateId(String nodeSelect){
        logger.info("Reseting UniqueId Sequence Count...");
        
        InMemoryLock resetSequenceLock = uniqueIdNodeProvider.getUniqueIdNode(nodeSelect).getResetSequenceLock();
        AtomicInteger sequenceCounter = uniqueIdNodeProvider.getUniqueIdNode(nodeSelect).getSequenceCounter();
        
        return resetSequenceLock.acquireLock(this)
        .then(Mono.defer(() -> {
            
            Supplier<Mono<Long>>unlockThenGenerateId = () -> 
                resetSequenceLock.releaseLock(this)
                .then(Mono.defer(() -> generateIdNumber(nodeSelect)));
            
            if(sequenceCounter.get() < maxSequence){
                return unlockThenGenerateId.get();
            }
            
            long timestampBeforeDelay = clock.millis();
            
            return Mono.delay(Duration.ofMillis(1))
            .then(Mono.defer(() -> {
                if(timestampBeforeDelay >= clock.millis()){
                    return Mono.error(new IllegalStateException("Suspected clock drift during sequenceCounter reset"));
                }
                sequenceCounter.set(0);
                return unlockThenGenerateId.get();
            }));
        }))
        .onErrorResume(e -> {
            String msg = "unable to reset uniqueId sequence : ["+e.getMessage()+"]";
            return handleException.apply(msg,e);
        })
        .doOnTerminate(() -> resetSequenceLock.releaseLock(this).subscribe());
    }
    
    /**
    * Parses a generated ID into its constituent components.
    *
    * @param id generated unique ID
    * @return array containing:
    *         <ul>
    *           <li>index 0: absolute timestamp in milliseconds</li>
    *           <li>index 1: node ID</li>
    *           <li>index 2: sequence number</li>
    *         </ul>
    */
    public long[] parse(long id) {
        long maskNodeId = ((1L << NODE_ID_BITS) - 1) << SEQUENCE_BITS;
        long maskSequence = (1L << SEQUENCE_BITS) - 1;

        long timestampDelta = (id >> (NODE_ID_BITS + SEQUENCE_BITS)) + rootTimeStamp;
        long nodeId = (id & maskNodeId) >> SEQUENCE_BITS;
        long sequence = id & maskSequence;

        return new long[]{timestampDelta, nodeId, sequence};
    }
    
    Mono<Long> getUniqueIdNodeId(String nodeSelect){
        return uniqueIdNodeProvider.getUniqueIdNode(nodeSelect)
        .getId()
        .map(nodeId -> {
            if(nodeId < 0 || nodeId > maxNodeId) {
                throw new IllegalArgumentException(String.format("NodeId must be between %d and %d", 0, maxNodeId));
            }
            return nodeId;
        });
    }
}