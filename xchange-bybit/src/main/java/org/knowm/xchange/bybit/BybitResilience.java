package org.knowm.xchange.bybit;

import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import org.knowm.xchange.client.ResilienceRegistries;
import org.knowm.xchange.client.ResilienceUtils;

import java.time.Duration;

import static jakarta.ws.rs.core.Response.Status.TOO_MANY_REQUESTS;

/** Author: Max Gao (gaamox@tutanota.com) Created: 08-06-2021 */
public class BybitResilience {
  public static ResilienceRegistries createRegistries() {
    final ResilienceRegistries registries = new ResilienceRegistries();

    Bybit.publicPathRateLimits.forEach(
        (path, limit) -> {
          registries
              .rateLimiters()
              .rateLimiter(
                  path,
                  RateLimiterConfig.from(registries.rateLimiters().getDefaultConfig())
                      .limitRefreshPeriod(Duration.ofSeconds(limit.get(1)))
                      .limitForPeriod(limit.get(0))
                      .drainPermissionsOnResult(
                          e -> ResilienceUtils.matchesHttpCode(e, TOO_MANY_REQUESTS))
                      .build());
        });

    BybitAuthenticated.privatePathRateLimits.forEach(
        (path, limit) -> {
          registries
              .rateLimiters()
              .rateLimiter(
                  path,
                  RateLimiterConfig.from(registries.rateLimiters().getDefaultConfig())
                      .limitRefreshPeriod(Duration.ofSeconds(limit.get(1)))
                      .limitForPeriod(limit.get(0))
                      .drainPermissionsOnResult(
                          e -> ResilienceUtils.matchesHttpCode(e, TOO_MANY_REQUESTS))
                      .build());
        });

    return registries;
  }
}
