package com.pyryze.infrastructure.uniqueid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.junit.jupiter.api.Assertions.*;

import static org.mockito.Mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.Instant;
import java.time.Duration;
import java.time.Clock;
import java.util.stream.Collectors;
import java.util.*;
import java.util.function.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.pyryze.util.InMemoryLock;

@ExtendWith(MockitoExtension.class)
public class TestUniqueId{
    
    private final UniqueIdNodeMemoryProvider uniqueIdNodeProvider = new UniqueIdNodeMemoryProvider();
    
    @DisplayName("Test on UniqueId#testGenerateIdNumber_timestampDeltaOverflow()")
    @Test
    void testGenerateIdNumber_timestampDeltaOverflow(){
        Clock clock = mock(Clock.class);
        
        long exceed41BitCurrentTimestamp = UniqueId.rootTimeStamp + UniqueId.maxEpoch + 1l;
        
        when(clock.millis()).thenReturn(exceed41BitCurrentTimestamp);
        
        UniqueId uniqueId  = new UniqueId(
            uniqueIdNodeProvider,
            clock,
            null
        );
        
        assertThrows(
            IllegalStateException.class,
            ()->uniqueId.generateIdNumber("nodeSelect").block()
        );
    }
    
    @DisplayName("Test on UniqueId#testGenerateIdNumber()")
    @Test
    void testGenerateIdNumber() {
        Clock clock = mock(Clock.class);
        
        String nodeSelect = "nodeSelect";
        long timestampDelta = 20;
        long timestamp = UniqueId.rootTimeStamp + timestampDelta;
        long expectedNodeId = uniqueIdNodeProvider.getUniqueIdNode(nodeSelect).getId().block();
        long expectedSequenceId = uniqueIdNodeProvider.getUniqueIdNode(nodeSelect).getSequenceCounter().get() + 1;
        
        when(clock.millis()).thenReturn(timestamp);
        
        UniqueId uniqueId  = new UniqueId(
            uniqueIdNodeProvider,
            clock,
            null
        );
        
        long id = uniqueId.generateIdNumber(nodeSelect).block();
        
        long[] constituents = uniqueId.parse(id);
        assertEquals(timestamp, constituents[0]);
        assertEquals(expectedNodeId, constituents[1]);
        assertEquals(expectedSequenceId, constituents[2]);
    }
    
    @DisplayName("Test on UniqueId#testGenerateIdNumber_incrementSequence()")
    @Test
    void testGenerateIdNumber_incrementSequence() {
        Clock clock = mock(Clock.class);
        
        String nodeSelect = "nodeSelect";
        AtomicInteger sequenceCounter = uniqueIdNodeProvider.getUniqueIdNode(nodeSelect).getSequenceCounter();
        sequenceCounter.set((int)UniqueId.maxSequence);//max sequence bit...would reset
        
        long timestampDelta = 20;
        long timestamp = UniqueId.rootTimeStamp + timestampDelta;
        long expectedNodeId = uniqueIdNodeProvider.getUniqueIdNode(nodeSelect).getId().block();
        long expectedSequenceId = 1;//reset...
        
        AtomicInteger callTimes = new AtomicInteger(0);
        when(clock.millis()).thenAnswer(invocation -> {//so that clock#millis  after delay would increase 
            long _callTimes = callTimes.incrementAndGet();
            return _callTimes==1 ? timestamp : (timestamp+_callTimes);
        });
        
        UniqueId uniqueId  = new UniqueId(
            uniqueIdNodeProvider,
            clock,
            null
        );
        
        long id = uniqueId.generateIdNumber(nodeSelect).block();
        
        long[] constituents = uniqueId.parse(id);
        assertEquals((timestamp + callTimes.get()), constituents[0]);
        assertEquals(expectedNodeId, constituents[1]);
        assertEquals(expectedSequenceId, constituents[2]);
    }
    
    @DisplayName("Test on UniqueId#testGenerateIdNumber_incrementSequence_clockDrift_IllegalStateException()")
    @Test
    void testGenerateIdNumber_incrementSequence_clockDrift_IllegalStateException() {
        Clock clock = mock(Clock.class);
        
        String nodeSelect = "nodeSelect";
        AtomicInteger sequenceCounter = uniqueIdNodeProvider.getUniqueIdNode(nodeSelect).getSequenceCounter();
        sequenceCounter.set((int)UniqueId.maxSequence);//max sequence bit...supposed to reset
        
        long expectedTimeDelta = 20;
        long timestamp = UniqueId.rootTimeStamp + expectedTimeDelta;
        
        UniqueId uniqueId  = new UniqueId(
            uniqueIdNodeProvider,
            clock,
            null
        );
        
        assertThrows(
            IllegalStateException.class,
            ()->uniqueId.generateIdNumber(nodeSelect).block()
        );
    }
    
    @DisplayName("Test on UniqueId#testConcurrentGenerateIdProducesUniqueIds()")
    @Test
    void testConcurrentGenerateIdProducesUniqueIds() {
        final int contextsSize = 20;
        Set<Long> ids = Collections.synchronizedSet(new HashSet<>());
        
        UniqueId uniqueId  = new UniqueId(uniqueIdNodeProvider,null);
        
        Mono[] contexts = new Mono[contextsSize];
        for (int i = 0; i < contextsSize; i++) {
            contexts[i] = uniqueId.generateIdNumber("nodeSelect")
            .doOnNext(id -> ids.add(id));
        }
        
        Mono.when(contexts).block();
        // Each ID must be unique
        assertEquals(contextsSize, ids.size());//for Set ids which dont allow duplicates in it to be 20 in number, then that means that all generated uds are unique
    }
    
    @DisplayName("Test on UniqueId#testGenerateId()")
    @Test
    void testGenerateId(){
        String nodeSelect = "nodeSelect";
        Long id = 738383l;
        UniqueId uniqueId  = spy(new UniqueId(uniqueIdNodeProvider,null));
        
        doReturn(Mono.just(id))
        .when(uniqueId)
        .generateIdNumber(nodeSelect);
        
        assertEquals(
            String.valueOf(id),
            uniqueId.generateId(nodeSelect).block()
        );
    }
}