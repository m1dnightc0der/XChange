package org.knowm.xchange.bybit.dto.trade;

import org.knowm.xchange.dto.Order;

public enum BybitOrderFlags implements Order.IOrderFlags {
    POST_ONLY,
    IOC,
    FOK
}
