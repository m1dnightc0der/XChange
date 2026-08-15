package org.knowm.xchange.binance;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.knowm.xchange.binance.dto.BinanceException;
import org.knowm.xchange.exceptions.CurrencyPairNotValidException;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.exceptions.ExchangeSecurityException;
import org.knowm.xchange.exceptions.ExchangeUnavailableException;
import org.knowm.xchange.exceptions.FrequencyLimitExceededException;
import org.knowm.xchange.exceptions.FundsExceededException;
import org.knowm.xchange.exceptions.InstrumentNotValidException;
import org.knowm.xchange.exceptions.InternalServerException;
import org.knowm.xchange.exceptions.MarketSuspendedException;
import org.knowm.xchange.exceptions.NonceException;
import org.knowm.xchange.exceptions.OperationTimeoutException;
import org.knowm.xchange.exceptions.OrderAmountUnderMinimumException;
import org.knowm.xchange.exceptions.OrderNotValidException;
import org.knowm.xchange.exceptions.RateLimitExceededException;
import org.knowm.xchange.exceptions.SystemOverloadException;

/** Adapts native Binance errors without allowing undocumented codes to inherit a classification. */
public final class BinanceErrorAdapter {

  private static final Set<Integer> SECURITY_CODES =
      set(
          -1004, -1005, -1011, -1022, -1109, -1125, -2014, -2015, -2017, -4027,
          -4056, -4057, -4080, -4088, -4109, -4192, -4202, -4203, -4205, -4206,
          -4402, -4403);

  private static final Set<Integer> ORDER_INVALID_CODES =
      set(
          -1013, -1014, -1023, -1100, -1101, -1102, -1103, -1104, -1105, -1106,
          -1111, -1113, -1114, -1115, -1116, -1117, -1118, -1119, -1120, -1127,
          -1128, -1130, -1136, -2010, -2013, -2020, -2021, -2022, -2025, -2026,
          -4000, -4001, -4002, -4003, -4005, -4006, -4007, -4008, -4009, -4010,
          -4011, -4012, -4013, -4014, -4015, -4016, -4017, -4018, -4019, -4020,
          -4021, -4023, -4024, -4025, -4026, -4028, -4029, -4030, -4031, -4032,
          -4044, -4045, -4046, -4047, -4048, -4049, -4052, -4053, -4055, -4058,
          -4059, -4060, -4061, -4062, -4063, -4064, -4065, -4066, -4067, -4068,
          -4069, -4070, -4071, -4072, -4073, -4074, -4075, -4076, -4077, -4079,
          -4082, -4085, -4086, -4087, -4105, -4106, -4107, -4114, -4115, -4116,
          -4117, -4120, -4131, -4135, -4137, -4138, -4139, -4142, -4161, -4165,
          -4167, -4168, -4170, -4171, -4183, -4184, -4189, -4208, -4209, -4210,
          -4211, -4400, -4401, -5021, -5022, -5025, -5026, -5027, -5037, -5038,
          -5039, -5040, -5041, -5043);

  private static final Set<Integer> FUNDS_CODES =
      set(
          -2018, -2019, -2023, -2024, -2027, -2028, -4050, -4051, -4054, -4118,
          -4169, -4172);

  private static final Set<Integer> INSTRUMENT_CODES =
      set(-1108, -1110, -1126, -4081, -4104);
  private static final Set<Integer> CURRENCY_PAIR_CODES = set(-1121, -4144);
  private static final Set<Integer> MARKET_SUSPENDED_CODES =
      set(-2016, -4022, -4140, -4141, -5024);
  private static final Set<Integer> MINIMUM_AMOUNT_CODES = set(-4004, -4164, -5029);
  private static final Set<Integer> NONCE_CODES = set(-5028);

  private BinanceErrorAdapter() {}

  public static ExchangeException adapt(BinanceException exception) {
    if (exception == null) {
      return new ExchangeException("Binance error unknown: No error details provided");
    }

    int code = exception.getCode();
    String rawMessage = normalizeMessage(exception.getRawMessage());
    String canonicalMessage = "Binance error " + code + ": " + rawMessage;
    String normalized = rawMessage.toLowerCase(Locale.ROOT);

    if (code == -1001) {
      if (normalized.startsWith("internal error; unable to process your request")) {
        return new ExchangeUnavailableException(canonicalMessage, exception);
      }
      if (normalized.startsWith("you are not authorized to execute this request")) {
        return new ExchangeSecurityException(canonicalMessage, exception);
      }
      return new ExchangeException(canonicalMessage, exception);
    }

    // Preserve legacy classifications before applying newly documented mappings.
    if (code == -1002 || code == -1122) {
      return new ExchangeSecurityException(canonicalMessage, exception);
    }
    if (code == -1021) {
      return new OperationTimeoutException(canonicalMessage, exception);
    }
    if (code == -1010 || code == -2010 || code == -2011) {
      if (normalized.contains("insufficient balance")) {
        return new FundsExceededException(canonicalMessage, exception);
      }
      if (code == -2011 && normalized.startsWith("unknown order sent")) {
        return new OrderNotValidException(canonicalMessage, exception);
      }
      return new ExchangeException(canonicalMessage, exception);
    }

    if (code == -1013 && isMinimumNotionalMessage(normalized)) {
      return new OrderAmountUnderMinimumException(canonicalMessage, exception);
    }
    if (code == -1003) {
      return new RateLimitExceededException(canonicalMessage, exception);
    }
    if (code == -1007) {
      return new OperationTimeoutException(canonicalMessage, exception);
    }
    if (code == -1008) {
      return new SystemOverloadException(canonicalMessage, exception);
    }
    if (code == -1015) {
      return withCause(new FrequencyLimitExceededException(canonicalMessage), exception);
    }
    if (code == -1016) {
      return new ExchangeUnavailableException(canonicalMessage, exception);
    }
    if (code == -4078) {
      return new InternalServerException(canonicalMessage, exception);
    }
    if (SECURITY_CODES.contains(code)) {
      return new ExchangeSecurityException(canonicalMessage, exception);
    }
    if (NONCE_CODES.contains(code)) {
      return new NonceException(canonicalMessage, exception);
    }
    if (CURRENCY_PAIR_CODES.contains(code)) {
      return new CurrencyPairNotValidException(canonicalMessage, exception);
    }
    if (INSTRUMENT_CODES.contains(code)) {
      return new InstrumentNotValidException(canonicalMessage, exception);
    }
    if (MARKET_SUSPENDED_CODES.contains(code)) {
      return new MarketSuspendedException(canonicalMessage, exception);
    }
    if (FUNDS_CODES.contains(code)) {
      return new FundsExceededException(canonicalMessage, exception);
    }
    if (MINIMUM_AMOUNT_CODES.contains(code)) {
      return new OrderAmountUnderMinimumException(canonicalMessage, exception);
    }
    if (ORDER_INVALID_CODES.contains(code)) {
      return new OrderNotValidException(canonicalMessage, exception);
    }
    return new ExchangeException(canonicalMessage, exception);
  }

  private static boolean isMinimumNotionalMessage(String normalizedMessage) {
    return normalizedMessage.equals("min_notional")
        || normalizedMessage.startsWith("min_notional ")
        || normalizedMessage.startsWith("filter failure: min_notional");
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
