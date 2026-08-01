package org.knowm.xchange.dto.trade;

import org.knowm.xchange.dto.Order;

public final class CancelOrderResult {
    private final boolean cancelled;
    private final Order order;

    public CancelOrderResult(boolean cancelled, Order order) {
        this.cancelled = cancelled;
        this.order = order;
    }

    public boolean isCancelled() {
        return cancelled;
    }

    public Order getOrder() {
        return order;
    }
}
