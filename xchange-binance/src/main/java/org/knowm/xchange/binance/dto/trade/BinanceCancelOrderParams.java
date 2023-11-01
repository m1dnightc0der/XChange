package org.knowm.xchange.binance.dto.trade;

import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.service.trade.params.CancelOrderByIdParams;
import org.knowm.xchange.service.trade.params.CancelOrderByInstrument;

public class BinanceCancelOrderParams implements CancelOrderByIdParams, CancelOrderByInstrument {
  private final String orderId;
  private final Instrument pair;
  private Boolean isMarginOrder;
  public BinanceCancelOrderParams(Instrument pair, String orderId) {
    this.pair = pair;
    this.orderId = orderId;
  }


  public BinanceCancelOrderParams(Instrument pair, String orderId, Boolean isMarginOrder) {
    this.pair = pair;
    this.orderId = orderId;
    this.isMarginOrder=isMarginOrder;
  }
  @Override
  public Instrument getInstrument() {
    return pair;
  }

  @Override
  public String getOrderId() {
    return orderId;
  }


  public Boolean getIsMarginOrder() {
    return isMarginOrder;
  }

  public void setIsMarginOrder(Boolean isMarginOrder) {
    this.isMarginOrder = isMarginOrder;
  }
}


