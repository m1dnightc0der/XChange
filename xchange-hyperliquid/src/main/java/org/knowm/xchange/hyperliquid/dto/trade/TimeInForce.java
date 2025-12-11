package org.knowm.xchange.hyperliquid.dto.trade;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.knowm.xchange.dto.Order.IOrderFlags;

public enum TimeInForce implements IOrderFlags {
  Gtc,
  Ioc,
  Alo;

  @JsonCreator
  public static TimeInForce parseTimeInForce(String s) {
    try {
      return TimeInForce.valueOf(s);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to parse time_in_force: \"" + s + "\"");
    }
  }
}
