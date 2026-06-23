package com.pyryze.infrastructure.ratelimit;

public record RateLimiterCluster(
    RateLimiterCounter rateLimiterCounter,
    RateLimiterLock rateLimiterLock
){}