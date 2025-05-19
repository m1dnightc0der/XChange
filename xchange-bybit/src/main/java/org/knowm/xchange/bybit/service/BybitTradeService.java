package org.knowm.xchange.bybit.service;

import org.knowm.xchange.bybit.BybitAdapters;
import org.knowm.xchange.bybit.BybitExchange;
import org.knowm.xchange.bybit.dto.BybitCategory;
import org.knowm.xchange.bybit.dto.BybitResult;
import org.knowm.xchange.bybit.dto.trade.BybitCancelOrderRequest;
import org.knowm.xchange.bybit.dto.trade.BybitOrderResponse;
import org.knowm.xchange.bybit.dto.trade.BybitTradeParams;
import org.knowm.xchange.bybit.dto.trade.details.BybitOrderDetail;
import org.knowm.xchange.bybit.dto.trade.details.BybitOrderDetails;
import org.knowm.xchange.client.ResilienceRegistries;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.account.OpenPositions;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.MarketOrder;
import org.knowm.xchange.dto.trade.OpenOrders;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.service.trade.TradeService;
import org.knowm.xchange.service.trade.params.CancelOrderByIdParams;
import org.knowm.xchange.service.trade.params.CancelOrderByInstrument;
import org.knowm.xchange.service.trade.params.CancelOrderParams;
import org.knowm.xchange.service.trade.params.orders.OrderQueryParamInstrument;
import org.knowm.xchange.service.trade.params.orders.OrderQueryParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.knowm.xchange.bybit.BybitAdapters.*;

public class BybitTradeService extends BybitTradeServiceRaw implements TradeService {

  public BybitTradeService(BybitExchange exchange, ResilienceRegistries resilienceRegistries) {
    super(exchange, resilienceRegistries);
  }

  @Override public String placeMarketOrder(MarketOrder marketOrder) throws IOException {
    BybitResult<BybitOrderResponse> orderResponseBybitResult = placeMarketOrder(BybitAdapters.getCategory(marketOrder.getInstrument()),
        BybitAdapters.convertToBybitSymbol(marketOrder.getInstrument()), BybitAdapters.getSideString(marketOrder.getType()),
        marketOrder.getOriginalAmount(), marketOrder.getUserReference());

    return orderResponseBybitResult.getResult().getOrderId();
  }

  @Override public Collection<Order> getOrder(OrderQueryParams... orderQueryParams) throws IOException {
    ArrayList<Order> result = new ArrayList<>();
    for (OrderQueryParams orderQueryParam : orderQueryParams) {
      Order order = getOrder(orderQueryParam);
      if (order != null) {
        result.add(order);
      }
    }
    return result;
  }

  public Order getOrder(OrderQueryParams orderQueryParams) throws IOException {
    Order result = null;
    if (orderQueryParams instanceof OrderQueryParamInstrument) {
      Instrument instrument = ((OrderQueryParamInstrument) orderQueryParams).getInstrument();
      String orderId = orderQueryParams.getOrderId();
      BybitCategory category = getCategory(instrument);

      BybitResult<BybitOrderDetails<BybitOrderDetail>> bybitOrder = getBybitOrder(category, orderId);

      if (bybitOrder.getResult() != null && bybitOrder.getResult().getList() != null && !bybitOrder.getResult().getList()
          .isEmpty() && bybitOrder.getResult().getCategory().equals(category)) {
        BybitOrderDetail bybitOrderDetail = bybitOrder.getResult().getList().get(0);
        result = adaptBybitOrderDetails(bybitOrderDetail);
      }

    } else {
      throw new IOException("OrderQueryParams must implement OrderQueryParamInstrument interface.");
    }
    return result;
  }

  @Override public OpenOrders getOpenOrders() throws IOException {

    List<LimitOrder> results = new ArrayList<>();
    //SETTLE_CURRENCIES.=

    for (BybitCategory category : BybitCategory.values()) {
      BybitResult<BybitOrderDetails<BybitOrderDetail>> bybitOrder = null;
      if (category.equals(BybitCategory.LINEAR)) {
        for (String settleCoin : SETTLE_CURRENCIES) {
          if (bybitOrder == null) {
            bybitOrder = getBybitPendingOrder(category, null, settleCoin, null, null);
          } else {
            bybitOrder.getResult().getList().addAll(getBybitPendingOrder(category, null, settleCoin, null, null).getResult().getList());
          }
        }
      } else {
        bybitOrder = getBybitPendingOrder(category, null, null, null, null);
      }
      if (bybitOrder.getResult() != null && bybitOrder.getResult().getList() != null && !bybitOrder.getResult().getList()
          .isEmpty() && bybitOrder.getResult().getCategory().equals(category)) {

        for (BybitOrderDetail bybitOrderDetail : bybitOrder.getResult().getList()) {
          LimitOrder order = (LimitOrder) adaptBybitOrderDetails(bybitOrderDetail);
          results.add(order);
        }
      }
    }
    return new OpenOrders(results);
  }

  @Override public boolean cancelOrder(CancelOrderParams params) throws IOException {

    if (params instanceof BybitTradeParams.BybitCancelOrderParams) {
      BybitTradeParams.BybitCancelOrderParams bybitCancelOrderParams = (BybitTradeParams.BybitCancelOrderParams) params;
      String id = ((CancelOrderByIdParams) params).getOrderId();
      String symbol = BybitAdapters.convertToBybitSymbol(((CancelOrderByInstrument) params).getInstrument());
      BybitCategory category = getCategory(((CancelOrderByInstrument) params).getInstrument());
      BybitCancelOrderRequest req = BybitCancelOrderRequest.builder().symbol(symbol).orderId(id).category(category.getValue().toLowerCase()).build();

      return cancelBybitOrder(req).getResult().getOrderId() != null;

    } else if (params instanceof CancelOrderByIdParams && params instanceof CancelOrderByInstrument) {

      String id = ((CancelOrderByIdParams) params).getOrderId();
      String symbol = BybitAdapters.convertToBybitSymbol(((CancelOrderByInstrument) params).getInstrument());
      BybitCategory category = getCategory(((CancelOrderByInstrument) params).getInstrument());
      BybitCancelOrderRequest req = BybitCancelOrderRequest.builder().symbol(symbol).orderId(id).category(category.getValue().toLowerCase()).build();

      return cancelBybitOrder(req).getResult().getOrderId() != null;
    } else {
      throw new IOException("CancelOrderParams must implement CancelOrderByIdParams and CancelOrderByInstrument interface.");
    }
  }

  @Override public String placeLimitOrder(LimitOrder limitOrder) throws IOException {

    BybitResult<BybitOrderResponse> orderResponseBybitResult = placeLimitOrder(BybitAdapters.getCategory(limitOrder.getInstrument()),
        BybitAdapters.convertToBybitSymbol(limitOrder.getInstrument()), BybitAdapters.getSideString(limitOrder.getType()),
        limitOrder.getOriginalAmount(), limitOrder.getLimitPrice(), limitOrder.getUserReference(), BybitAdapters.getTimeInForce(limitOrder));

    return orderResponseBybitResult.getResult().getOrderId();
  }

  @Override public Collection<Order> getOrder(String... orderIds) throws IOException {
    List<Order> results = new ArrayList<>();

    for (String orderId : orderIds) {
      for (BybitCategory category : BybitCategory.values()) {

        BybitResult<BybitOrderDetails<BybitOrderDetail>> bybitOrder = getBybitOrder(category, orderId);

        if (bybitOrder.getResult() != null && bybitOrder.getResult().getList() != null && !bybitOrder.getResult().getList()
            .isEmpty() && bybitOrder.getResult().getCategory().equals(category)) {
          BybitOrderDetail bybitOrderDetail = bybitOrder.getResult().getList().get(0);
          Order order = adaptBybitOrderDetails(bybitOrderDetail);
          results.add(order);
        }
      }
    }

    return results;
  }

  @Override public OpenPositions getOpenPositions() throws IOException {
    OpenPositions linearPositions = BybitAdapters.adaptOpenPositions(getPositions(BybitCategory.LINEAR, null, "USDC", null).getResult().getList(),BybitCategory.LINEAR);
    linearPositions.getOpenPositions()
        .addAll(BybitAdapters.adaptOpenPositions(getPositions(BybitCategory.LINEAR, null, "USDT", null).getResult().getList(),BybitCategory.LINEAR).getOpenPositions());
    linearPositions.getOpenPositions()
        .addAll(BybitAdapters.adaptOpenPositions(getPositions(BybitCategory.INVERSE, null, null, null).getResult().getList(),BybitCategory.INVERSE).getOpenPositions());
    return linearPositions;
  }

  public String amendOrder(Order order) throws IOException {
    BybitCategory category = BybitAdapters.getCategory(order.getInstrument());
    BybitResult<BybitOrderResponse> response = null;
    if (order instanceof LimitOrder) {
      response = amendOrder(category, convertToBybitSymbol(order.getInstrument()), order.getId(), order.getUserReference(), null,
          order.getOriginalAmount().toString(), ((LimitOrder) order).getLimitPrice().toString(), null, null, null, null, null, null, null, null);
    }
    //Todo order instanceof StopOrder
    if (response != null) {
      return response.getResult().getOrderId();
    }
    return "";
  }

}
