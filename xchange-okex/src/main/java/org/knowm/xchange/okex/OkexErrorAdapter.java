package org.knowm.xchange.okex;

import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.exceptions.ExchangeSecurityException;
import org.knowm.xchange.exceptions.FundsExceededException;
import org.knowm.xchange.exceptions.InstrumentNotValidException;
import org.knowm.xchange.exceptions.NonceException;
import org.knowm.xchange.exceptions.OrderAmountUnderMinimumException;
import org.knowm.xchange.exceptions.OrderNotValidException;
import org.knowm.xchange.okex.dto.OkexException;

/** Adapts OKX error codes to the most specific deterministic XChange exception type. */
public final class OkexErrorAdapter {

  private OkexErrorAdapter() {}

  public static ExchangeException adapt(OkexException exception) {
    if (exception == null) {
      return new ExchangeException("Unknown OKX error (no details available)");
    }

    return adapt(Integer.toString(exception.getCode()), exception.getMessage(), exception);
  }

  public static ExchangeException adapt(String rawCode, String message) {
    return adapt(rawCode, message, null);
  }

  /**
   * Returns whether adaptation produced a deterministic exception more specific than the fallback.
   */
  public static boolean isSpecialized(ExchangeException exception) {
    return exception != null && exception.getClass() != ExchangeException.class;
  }

  static ExchangeException adapt(String rawCode, String message, Throwable cause) {
    String renderedMessage = "OKX error " + rawCode + ": " + message;
    Integer code = baseCode(rawCode);

    if (code == null || isIndeterminateCode(code)) {
      return new ExchangeException(renderedMessage, cause);
    }

    if (isNonceCode(code)) {
      return new NonceException(renderedMessage, cause);
    }
    if (isSecurityCode(code)) {
      return new ExchangeSecurityException(renderedMessage, cause);
    }
    if (code == 51001) {
      return new InstrumentNotValidException(renderedMessage, cause);
    }
    if (code == 50122 || code == 51007 || code == 51120) {
      return new OrderAmountUnderMinimumException(renderedMessage, cause);
    }
    if (code == 51008 || code == 51127 || code == 51131 || code == 51502) {
      return new FundsExceededException(renderedMessage, cause);
    }
    if (isOrderNotValidCode(code)) {
      return new OrderNotValidException(renderedMessage, cause);
    }

    return new ExchangeException(renderedMessage, cause);
  }

  private static Integer baseCode(String rawCode) {
    if (rawCode == null) {
      return null;
    }

    int separator = rawCode.indexOf('_');
    String base = separator >= 0 ? rawCode.substring(0, separator) : rawCode;
    try {
      return Integer.valueOf(base.trim());
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private static boolean isIndeterminateCode(int code) {
    switch (code) {
      case 50001:
      case 50004:
      case 50005:
      case 50011:
      case 50013:
      case 50026:
      case 50061:
      case 51113:
      case 51115:
      case 51412:
      case 52000:
      case 60014:
      case 60023:
      case 63999:
        return true;
      default:
        return false;
    }
  }

  private static boolean isNonceCode(int code) {
    return code == 50102 || code == 50112 || code == 60004 || code == 60006;
  }

  private static boolean isSecurityCode(int code) {
    return code == 50100
        || code == 50101
        || between(code, 50103, 50111)
        || between(code, 50113, 50114)
        || between(code, 50119, 50121)
        || code == 60005
        || code == 60007
        || code == 60009
        || code == 60011
        || code == 60024
        || code == 60031
        || code == 60032;
  }

  private static boolean isOrderNotValidCode(int code) {
    return code == 51000
        || between(code, 51002, 51006)
        || between(code, 51009, 51112)
        || between(code, 51116, 51119)
        || between(code, 51121, 51178)
        || between(code, 51400, 51411)
        || between(code, 51413, 51501)
        || between(code, 51503, 51543)
        || between(code, 51600, 51603);
  }

  private static boolean between(int code, int minimum, int maximum) {
    return code >= minimum && code <= maximum;
  }
}
