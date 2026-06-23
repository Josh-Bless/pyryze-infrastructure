package com.pyryze.infrastructure.ratelimit;

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
import java.util.stream.Collectors;
import java.util.*;
import java.util.function.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.atomic.AtomicInteger;

import com.pyryze.util.InMemoryLock;
import com.pyryze.util.ValueBaseObjectProvider;
import com.pyryze.infrastructure.ratelimit.RateLimiter.*;

@ExtendWith(MockitoExtension.class)
public class TestRateLimiter{
    
    Map<String,Instant>registerInstantB4Call = new HashMap<>();
    Map<Integer,Set<String>>registerShouldIncrementRateLimits = new HashMap<>();
    
    private static final String CLUSTER_QUALIFIER = "LOCAL_CLUSTER";
    
    private RateLimiterClusterProvider rateLimiterClusterProvider = rateLimiterClusterProvider();
    
    private final RateLimiter rateLimiter = new RateLimiter(rateLimiterClusterProvider);
    
    @DisplayName("Test on RateLimiter#testExecuteSuccess()")
    @Test
    void testExecuteSuccess(){
        
        Integer testId = 1;
        Long delta = 2l;
        Duration duration = Duration.ofSeconds(2l);
        Long limit = 2l;
        
        RateLimit requestRateLimit_1 = new RateLimit(
            "executeOnLimit#1",
            delta, 
            duration,
            limit
        );
        RateLimit requestRateLimit_2 = new RateLimit(
            "executeOnLimit#2",
            delta, 
            duration,
            limit
        );
        
        Set<RateLimit>rateLimits = Set.of(requestRateLimit_1,requestRateLimit_2);
        String response = execute(testId,rateLimits, "testId[1]");
        
        assertEquals("testId[1]", response);
        assertEquals(2l, getCount(requestRateLimit_1.name()));
        assertEquals(2l, getCount(requestRateLimit_2.name()));
    }
    
    @DisplayName("Test on RateLimiter#testExceptionB4Execute()")
    @Test
    void testExceptionB4Execute(){
        
        Integer testId = 2;
        Long delta = 1l;
        Duration duration = Duration.ofSeconds(2l);
        Long limit = 2l;
        
        RateLimit requestRateLimit_3 = new RateLimit(
            "executeOnLimit#3",
            delta, 
            duration,
            limit
        );
        RateLimit requestRateLimit_4 = new RateLimit(
            "executeOnLimit#4",
            delta, 
            duration,
            limit
        );
        
        RateLimiterCounter rateLimiterCounter = rateLimiterClusterProvider
        .clusterInit(
            requestRateLimit_4.name(),
            CLUSTER_QUALIFIER
        ).rateLimiterCounter();
        
        when(rateLimiterCounter.get(anyBoolean()))
        .thenReturn(Mono.error(new RuntimeException("Exception Occured While Incrementing RateLimit 4")));
        
        RateLimit requestRateLimit_5 = new RateLimit(
            "executeOnLimit#5",
            delta, 
            duration,
            limit
        );
        
        Set<RateLimit>rateLimits = Set.of(requestRateLimit_3,requestRateLimit_4,requestRateLimit_5);
        assertThrows(RuntimeException.class, ()->execute(testId,rateLimits, "testId[2]"));
        
        assertEquals(0l, getCount(requestRateLimit_3.name()));
        assertEquals(0l, getCount(requestRateLimit_5.name()));
        //observe none was incremented(possible incremented rateLimits(requestRateLimit_3) was nullified by later decrement)
    }
    
    @DisplayName("Test on RateLimiter#testRateLimitReachB4Execute()")
    @Test
    void testRateLimitReachB4Execute(){
        
        Integer testId = 3;
        Long delta = 2l;
        Duration duration = Duration.ofSeconds(2l);
        Long limit = 2l;
        
        RateLimit requestRateLimit_6 = new RateLimit(
            "executeOnLimit#6",
            delta, 
            duration,
            limit
        );
        RateLimiterCounter rateLimiterCounter = rateLimiterClusterProvider
        .clusterInit(
            requestRateLimit_6.name(),
            CLUSTER_QUALIFIER
        ).rateLimiterCounter();
        rateLimiterCounter.incrementWithExpirySetIfExpiryAbsent(1l,duration).block();//currentValue(1)+delta(2) is greater than limit(2)...hence would trigger a RateLimitException before rateLimt action is executed
        
        RateLimit requestRateLimit_7 = new RateLimit(
            "executeOnLimit#7",
            delta, 
            duration,
            limit
        );
        RateLimit requestRateLimit_8 = new RateLimit(
            "executeOnLimit#8",
            delta, 
            duration,
            limit
        );
        
        Set<RateLimit>rateLimits = Set.of(requestRateLimit_6,requestRateLimit_7,requestRateLimit_8);
        
        RateLimitException rateLimitException = assertThrows(
            RateLimitException.class, 
            ()->execute(testId,rateLimits, "testId[3]")
        );
        assertEquals(requestRateLimit_6, rateLimitException.getPreExecute().rateLimit());
        Instant expectedAfreshThreshold = registerInstantB4Call.get(requestRateLimit_6.name());
        assertTrue( 
            rateLimitException.getPreExecute().afresh().isAfter(expectedAfreshThreshold) || rateLimitException.getPreExecute().afresh().equals(expectedAfreshThreshold),
            String.format("RateLimit[%s] preExecute Instant#afresh must not be below Instant#expectedAfreshThreshold",requestRateLimit_6.name())
        );
        
        assertEquals(1l, getCount(requestRateLimit_6.name()));//not incremented...already has 1
        assertEquals(0l, getCount(requestRateLimit_7.name()));
        assertEquals(0l, getCount(requestRateLimit_8.name()));
        //observe none was incremented(possible incremented rateLimits was nullified by later decrement)
    }
    
    
    @DisplayName("Test on RateLimiter#testExecuteWithSelectiveIncrement()")
    @Test
    void testExecuteWithSelectiveIncrement(){
        
        Integer testId = 4;
        Long delta = 1l;
        Duration duration = Duration.ofSeconds(2l);
        Long limit = 2l;
        
        RateLimit requestRateLimit_9 = new RateLimit(
            "executeOnLimit#9",
            delta, 
            duration,
            limit
        );
        RateLimit requestRateLimit_10 = new RateLimit(
            "executeOnLimit#10",
            delta, 
            duration,
            limit
        );
        
        registerShouldIncrementRateLimits.put(testId, Set.of(requestRateLimit_10.name()));
        //requestRateLimit_9 wont be increased
        
        Set<RateLimit>rateLimits = Set.of(requestRateLimit_9,requestRateLimit_10);
        
        String response = execute(testId,rateLimits, "testId[4]");
        
        assertEquals("testId[4]",response);
        assertEquals(0l, getCount(requestRateLimit_9.name()));//not incremented
        assertEquals(1l, getCount(requestRateLimit_10.name()));//incremented
    }
    
    long getCount(String rateLimitName){
        return rateLimiterClusterProvider
        .clusterInit(
            rateLimitName,
            CLUSTER_QUALIFIER
        ).rateLimiterCounter()
        .get(true).block();
    }
    
    String getQualifiedName(String key, String clusterQualifier){
        return key + "_" + clusterQualifier;
    }
    
    String execute(int testId, Set<RateLimit>rateLimits, String response){
        
        Set<String>shouldIncrementRateLimits = new HashSet<>();
        
        rateLimits.forEach(rateLimit -> {
            registerInstantB4Call.putIfAbsent(rateLimit.name(),Instant.now());
            shouldIncrementRateLimits.add(rateLimit.name());
        });
        registerShouldIncrementRateLimits.putIfAbsent(testId,shouldIncrementRateLimits);
        
        Function<Set<PreExecute>,Mono<RateActionResponse<String>>> action = preExecutes -> {
            
            assertPreExecutes(preExecutes, rateLimits);
            
            RateActionResponse<String>rateActionResponse = new RateActionResponse(
                registerShouldIncrementRateLimits.get(testId),
                response
            );
            return Mono.just(rateActionResponse);
        };
        
        return rateLimiter.execute(
            rateLimits, 
            action,
            CLUSTER_QUALIFIER
        ).block();
    }
    
    private void assertPreExecutes(
        Set<PreExecute>preExecutes,
        Set<RateLimit>rateLimits
    ){
        preExecutes.forEach(preExecute -> {
            
            RateLimit rateLimit = rateLimits.stream()
            .filter(_rateLimit -> preExecute.rateLimit().equals(_rateLimit))
            .findAny()
            .get();
            
            Instant expectedAfreshThreshold = registerInstantB4Call.get(rateLimit.name());
            
            Long currentRate = getCount(rateLimit.name());
            
            assertTrue( 
                preExecute.afresh().isAfter(expectedAfreshThreshold) || preExecute.afresh().equals(expectedAfreshThreshold),
                String.format("RateLimit[%s] preExecute Instant#afresh must not be below Instant#expectedAfreshThreshold",rateLimit.name())
            );
            
            assertEquals(currentRate, preExecute.currentRate());
        });
    }
    
    RateLimiterClusterProvider rateLimiterClusterProvider(){
        
        return new RateLimiterClusterProvider(){
            
            RateLimiterClusterMemoryProvider rateLimiterClusterMemoryProvider = new RateLimiterClusterMemoryProvider();
            
            ValueBaseObjectProvider<RateLimiterCluster> rateLimitClusters = new ValueBaseObjectProvider();
            
            public RateLimiterCluster clusterInit(String clusterKey,String clusterQualifier){
                
                String qualifiedName = getQualifiedName(clusterKey,clusterQualifier);
                return rateLimitClusters.getObject(
                    qualifiedName,
                    ()->createCluster(clusterKey,clusterQualifier)
                );
            }
            
            RateLimiterCluster createCluster(String clusterKey,String clusterQualifier){
                String qualifiedName = getQualifiedName(clusterKey,clusterQualifier);
                RateLimiterCluster rateLimiterCluster = rateLimiterClusterMemoryProvider.clusterInit(clusterKey,clusterQualifier);
                
                return new RateLimiterCluster(
                    mockedRateLimiterCounter(rateLimiterCluster.rateLimiterCounter()),
                    rateLimiterCluster.rateLimiterLock()
                );
            }
        };
    }
    
    RateLimiterCounter mockedRateLimiterCounter(RateLimiterCounter rateLimiterCounter){
        
        RateLimiterCounter mockedRateLimiterCounter = mock(RateLimiterCounter.class);
        
        lenient().when(mockedRateLimiterCounter.get(anyBoolean()))
        .thenAnswer(invocation -> {
            return rateLimiterCounter.get(invocation.<Boolean>getArgument(0));
        });
        
        lenient().when(mockedRateLimiterCounter.incrementWithExpirySetIfExpiryAbsent(any(),any()))
        .thenAnswer(invocation -> {
            return rateLimiterCounter.incrementWithExpirySetIfExpiryAbsent(
                invocation.<Long>getArgument(0),
                invocation.<Duration>getArgument(1)
            );
        });
        
        lenient().when(mockedRateLimiterCounter.getExpiry())
        .thenAnswer(invocation -> {
            return rateLimiterCounter.getExpiry();
        });
        
        lenient().when(mockedRateLimiterCounter.decrementIfWithinTime(any(),any()))
        .thenAnswer(invocation -> {
            return rateLimiterCounter.decrementIfWithinTime(
                invocation.<Long>getArgument(0),
                invocation.<Instant>getArgument(1)
            );
        });
        
        return mockedRateLimiterCounter;
    }
}