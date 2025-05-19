package org.knowm.xchange.bybit.dto.trade;

import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.service.trade.params.CancelOrderByIdParams;
import org.knowm.xchange.service.trade.params.CancelOrderByInstrument;


public class BybitTradeParams {
  public static class BybitCancelOrderParams
      implements CancelOrderByIdParams, CancelOrderByInstrument {
    public final Instrument instrument;
    public final String orderId;
    private final Boolean isAlgoOrder;

    public BybitCancelOrderParams(Instrument instrument, String orderId) {
      this.instrument = instrument;
      this.orderId = orderId;
      this.isAlgoOrder=false;
    }

    public BybitCancelOrderParams(Instrument instrument, String orderId, Boolean isAlgoOrder) {
      this.instrument = instrument;
      this.orderId = orderId;
      this.isAlgoOrder=isAlgoOrder;
    }

    @Override
    public String getOrderId() {
      return orderId;
    }

    @Override
    public Instrument getInstrument() {
      return instrument;
    }
    public Boolean getIsAlgoOrder() {
      return isAlgoOrder;
    }


  }
}
