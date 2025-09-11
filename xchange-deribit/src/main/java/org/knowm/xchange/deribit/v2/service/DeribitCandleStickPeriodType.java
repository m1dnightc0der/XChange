package org.knowm.xchange.deribit.v2.service;

public enum DeribitCandleStickPeriodType {
  CANDLE_STICK_1M(1, "1"),
  CANDLE_STICK_3M(3, "3"),
  CANDLE_STICK_5M(5, "5"),
  CANDLE_STICK_15M(15, "15"),
  CANDLE_STICK_30M(30, "30"),
  CANDLE_STICK_1H(60, "60"),
  CANDLE_STICK_2H(2 * 60, "120"),
  CANDLE_STICK_3H(3 * 60, "180"),
  CANDLE_STICK_6H(6 * 60, "360"),
  CANDLE_STICK_12H(12 * 60, "720"),
  CANDLE_STICK_1D(24 * 60, "1D");
  private final long periodInSecs;
  private final String fieldValue;

  DeribitCandleStickPeriodType(long periodInMinutes, String fieldValue) {
    this.periodInSecs = periodInMinutes * 60;
    this.fieldValue = fieldValue;
  }

  static DeribitCandleStickPeriodType getPeriodTypeFromSecs(long periodInSecs) {
    DeribitCandleStickPeriodType result = null;
    for (DeribitCandleStickPeriodType period : DeribitCandleStickPeriodType.values()) {
      if (period.periodInSecs == periodInSecs) {
        result = period;
        break;
      }
    }
    return result;
  }

  public static long[] getSupportedPeriodsInSecs() {
    long[] result = new long[DeribitCandleStickPeriodType.values().length];
    int index = 0;
    for (DeribitCandleStickPeriodType period : DeribitCandleStickPeriodType.values()) {
      result[index++] = period.periodInSecs;
    }
    return result;
  }

  public String getFieldValue() {
    return fieldValue;
  }
}
