package org.knowm.xchange.hyperliquid;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.exceptions.FundsExceededException;
import org.knowm.xchange.exceptions.NonceException;
import org.knowm.xchange.exceptions.OrderAmountUnderMinimumException;
import org.knowm.xchange.exceptions.OrderNotValidException;

public final class HyperliquidExceptionAdapter {

  private static final Set<String> MINIMUM_NOTIONAL_TYPES =
      lowerCaseSet("MinTradeNtl", "MinTradeSpotNtl");

  private static final Set<String> FUNDS_TYPES =
      lowerCaseSet("PerpMargin", "InsufficientSpotBalance");

  private static final Set<String> INVALID_ORDER_TYPES =
      lowerCaseSet(
          "Tick",
          "ReduceOnly",
          "BadAloPx",
          "IocCancel",
          "BadTriggerPx",
          "MarketOrderNoLiquidity",
          "PositionIncreaseAtOpenInterestCap",
          "PositionFlipAtOpenInterestCap",
          "TooAggressiveAtOpenInterestCap",
          "OpenInterestIncrease",
          "Oracle",
          "PerpMaxPosition",
          "MissingOrder");

  private HyperliquidExceptionAdapter() {}

  /** Maps an explicit Hyperliquid error to the most specific XChange exception. */
  public static ExchangeException adapt(String contextPrefix, String exchangeText) {
    String message = (contextPrefix == null ? "" : contextPrefix) + exchangeText;
    String normalized = normalize(exchangeText);

    if (isNonceMessage(exchangeText)) {
      return new NonceException(message);
    }
    if (MINIMUM_NOTIONAL_TYPES.contains(normalized)
        || normalized.contains("order must have minimum value of")) {
      return new OrderAmountUnderMinimumException(message);
    }
    if (FUNDS_TYPES.contains(normalized)
        || normalized.contains("insufficient margin to place order")
        || normalized.contains("order has insufficient spot balance to trade")) {
      return new FundsExceededException(message);
    }
    if (INVALID_ORDER_TYPES.contains(normalized) || isInvalidOrderMessage(normalized)) {
      return new OrderNotValidException(message);
    }
    return new ExchangeException(message);
  }

  public static NonceException nonceException(Throwable failure) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (isNonceMessage(current.getMessage())) {
        return new NonceException(current.getMessage(), failure);
      }
    }
    return null;
  }

  public static NonceException nonceException(String message) {
    return isNonceMessage(message) ? new NonceException(message) : null;
  }

  private static boolean isInvalidOrderMessage(String normalized) {
    return normalized.contains("price must be divisible by tick size")
        || normalized.contains("reduce only order would increase position")
        || normalized.contains("post only order would have immediately matched")
        || normalized.contains("order could not immediately match against any resting orders")
        || normalized.contains("invalid tp/sl price")
        || normalized.contains("no liquidity available for market order")
        || normalized.contains("order would increase open interest while open interest is capped")
        || normalized.contains(
            "order rejected due to price more aggressive than oracle while at open interest cap")
        || normalized.contains("order would increase open interest too quickly")
        || normalized.contains("order price too far from oracle")
        || normalized.contains(
            "order would cause position to exceed margin tier limit at current leverage")
        || normalized.contains("order was never placed, already canceled, or filled")
        || normalized.contains("empty batch of orders")
        || normalized.contains("non-reduce-only tp/sl")
        || normalized.contains("too far from reference price")
        || normalized.contains("tick size validation");
  }

  private static boolean isNonceMessage(String message) {
    return normalize(message).contains("invalid nonce");
  }

  private static String normalize(String message) {
    return message == null ? "" : message.trim().toLowerCase(Locale.ROOT);
  }

  private static Set<String> lowerCaseSet(String... values) {
    Set<String> result = new HashSet<>();
    Arrays.stream(values).map(HyperliquidExceptionAdapter::normalize).forEach(result::add);
    return result;
  }
}
