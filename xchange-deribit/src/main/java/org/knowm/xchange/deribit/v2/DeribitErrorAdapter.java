package org.knowm.xchange.deribit.v2;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.knowm.xchange.deribit.v2.dto.DeribitError;
import org.knowm.xchange.deribit.v2.dto.DeribitException;
import org.knowm.xchange.exceptions.CurrencyPairNotValidException;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.exceptions.ExchangeSecurityException;
import org.knowm.xchange.exceptions.ExchangeUnavailableException;
import org.knowm.xchange.exceptions.FrequencyLimitExceededException;
import org.knowm.xchange.exceptions.FundsExceededException;
import org.knowm.xchange.exceptions.InstrumentNotValidException;
import org.knowm.xchange.exceptions.InternalServerException;
import org.knowm.xchange.exceptions.MarketSuspendedException;
import org.knowm.xchange.exceptions.OperationTimeoutException;
import org.knowm.xchange.exceptions.OrderAmountUnderMinimumException;
import org.knowm.xchange.exceptions.OrderNotValidException;
import org.knowm.xchange.exceptions.RateLimitExceededException;
import org.knowm.xchange.exceptions.SystemOverloadException;

/** Exact native-code classifier for Deribit REST and WebSocket error payloads. */
final class DeribitErrorAdapter {

  private static final Set<Integer> SECURITY_CODES =
      set(
          10000, 10029, 11042, 11056, 11098, 12005, 13004, 13005, 13006, 13007,
          13009, 13012, 13015, 13021, 13031, 13037, 13040, 13403, 13778, 13792,
          13904, 13912, 13914, 13915, 13919);

  private static final Set<Integer> ORDER_INVALID_CODES =
      set(
          10003, 10004, 10005, 10006, 10007, 10010, 10011, 10013, 10014, 10015,
          10016, 10017, 10018, 10021, 10022, 10023, 10024, 10025, 10026, 10027,
          10031, 10034, 10035, 10036, 10037, 10038, 10043, 10044, 10045, 10046,
          10072, 11008, 11013, 11021, 11022, 11029, 11030, 11035, 11036, 11038,
          11039, 11041, 11043, 11044, 11045, 11047, 11048, 11049, 11050, 11054,
          11055, 11059, 13010, 13011, 13016, 13018, 13030, 13032, 13042, 13043,
          13901, 13902, 13910, 13917, 13918, -32602, -32600, -32700, -32000);

  private static final Set<Integer> RATE_LIMIT_CODES =
      set(10028, 100028, 12003, 12004, 12998, 13035, 13780, 13903, 13905, 13906);
  private static final Set<Integer> UNAVAILABLE_CODES =
      set(10040, 10041, 10048, 11051, 13025, 13028, 13036, 13503, 13793);
  private static final Set<Integer> SECURITY_MARKET_CODES = set(10012, 10019, 13019);
  private static final Set<Integer> INSTRUMENT_CODES = set(10020, 11037, 11046, 13020);
  private static final Set<Integer> FUNDS_CODES = set(10009, 10039);
  private static final Set<Integer> MINIMUM_AMOUNT_CODES = set(10002, 10032);

  private DeribitErrorAdapter() {}

  static ExchangeException adapt(DeribitException exception) {
    if (exception == null) {
      return new ExchangeException("Deribit error unknown: No error details provided");
    }
    DeribitError error = exception.getError();
    if (error == null) {
      return new ExchangeException("Deribit error unknown: No error details provided", exception);
    }

    int code = error.getCode();
    String rawMessage = normalizeMessage(error.getMessage());
    String normalized = rawMessage.toLowerCase(Locale.ROOT);
    String canonical = "Deribit error " + code + ": " + rawMessage;
    if (error.getData() != null && !error.getData().toString().trim().isEmpty()) {
      canonical += " - " + error.getData();
    }

    // Preserve the legacy Deribit invalid-params classification.
    if (code == -32602) {
      return new CurrencyPairNotValidException(canonical, exception);
    }
    if (code == 13907) {
      if (normalized.startsWith("not_fully_filled")
          || normalized.startsWith("too_many_open_block_rfqs")) {
        return new OrderNotValidException(canonical, exception);
      }
      return new ExchangeException(canonical, exception);
    }
    if (RATE_LIMIT_CODES.contains(code)
        || normalized.equals("too_many_requests")
        || normalized.contains("http status code: 429")
        || (exception.getMessage() != null
        && exception.getMessage().toLowerCase(Locale.ROOT).contains("http status code: 429"))) {
      return new RateLimitExceededException(canonical, exception);
    }
    if (code == 10047) {
      return new SystemOverloadException(canonical, exception);
    }
    if (code == 10066) {
      return withCause(new FrequencyLimitExceededException(canonical), exception);
    }
    if (code == 11094) {
      return new InternalServerException(canonical, exception);
    }
    if (code == 13888) {
      return new OperationTimeoutException(canonical, exception);
    }
    if (SECURITY_CODES.contains(code)) {
      return new ExchangeSecurityException(canonical, exception);
    }
    if (UNAVAILABLE_CODES.contains(code)) {
      return new ExchangeUnavailableException(canonical, exception);
    }
    if (SECURITY_MARKET_CODES.contains(code)) {
      return new MarketSuspendedException(canonical, exception);
    }
    if (INSTRUMENT_CODES.contains(code)) {
      return new InstrumentNotValidException(canonical, exception);
    }
    if (FUNDS_CODES.contains(code)) {
      return new FundsExceededException(canonical, exception);
    }
    if (MINIMUM_AMOUNT_CODES.contains(code)) {
      return new OrderAmountUnderMinimumException(canonical, exception);
    }
    if (ORDER_INVALID_CODES.contains(code)) {
      return new OrderNotValidException(canonical, exception);
    }
    return new ExchangeException(canonical, exception);
  }

  private static String normalizeMessage(String message) {
    return message == null || message.trim().isEmpty()
        ? "No error details provided"
        : message.trim();
  }

  private static Set<Integer> set(Integer... values) {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(values)));
  }

  private static <T extends ExchangeException> T withCause(T exception, Throwable cause) {
    exception.initCause(cause);
    return exception;
  }
}
