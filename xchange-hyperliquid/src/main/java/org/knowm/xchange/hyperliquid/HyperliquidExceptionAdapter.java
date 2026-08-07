package org.knowm.xchange.hyperliquid;

import java.util.Locale;
import org.knowm.xchange.exceptions.NonceException;

public final class HyperliquidExceptionAdapter {

  private HyperliquidExceptionAdapter() {}

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

  private static boolean isNonceMessage(String message) {
    return message != null
        && message.toLowerCase(Locale.ROOT).contains("invalid nonce");
  }
}
