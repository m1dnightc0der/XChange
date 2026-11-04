package org.knowm.xchange.hyperliquid.dto.trade;

import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.service.trade.params.CancelOrderByIdParams;
import org.knowm.xchange.service.trade.params.CancelOrderByInstrument;

/** Author: Max Gao (gaamox@tutanota.com) Created: 10-06-2021 */
public class HyperliquidTradeParams {
  /** Caller-supplied classification; never inferred from an order ID. */
  public enum OrderKind {
    NON_TRIGGER, TRIGGER, UNKNOWN
  }

  public static class HyperliquidCancelOrderParams
      implements CancelOrderByIdParams, CancelOrderByInstrument {
    public final Instrument instrument;
    public final String orderId;
    public final boolean fastRequested;
    public final OrderKind orderKind;

    public HyperliquidCancelOrderParams(Instrument instrument, String orderId) {
      this(instrument, orderId, false, OrderKind.UNKNOWN);
    }

    public HyperliquidCancelOrderParams(Instrument instrument, String orderId, Boolean isAlgoOrder) {
      // This legacy argument remains ignored; it does not establish fast-cancel eligibility.
      this(instrument, orderId);
    }

    /** Unknown and trigger orders remain cancellable, using ordinary cancellation. */
    public HyperliquidCancelOrderParams(
        Instrument instrument, String orderId, boolean fastRequested, OrderKind orderKind) {
      this.instrument = instrument;
      this.orderId = orderId;
      this.fastRequested = fastRequested;
      this.orderKind = orderKind == null ? OrderKind.UNKNOWN : orderKind;
    }

    public boolean isFast() {
      return fastRequested && orderKind == OrderKind.NON_TRIGGER;
    }

    @Override
    public String getOrderId() {
      return orderId;
    }

    @Override
    public Instrument getInstrument() {
      return instrument;
    }



  }
}
