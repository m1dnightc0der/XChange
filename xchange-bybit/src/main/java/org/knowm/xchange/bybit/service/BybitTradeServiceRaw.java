package org.knowm.xchange.bybit.service;

import static org.knowm.xchange.bybit.BybitAdapters.createBybitExceptionFromResult;

import java.io.IOException;
import java.math.BigDecimal;

import org.knowm.xchange.bybit.BybitExchange;
import org.knowm.xchange.bybit.dto.BybitCategory;
import org.knowm.xchange.bybit.dto.account.BybitPosition;
import org.knowm.xchange.bybit.dto.account.BybitPositionDetails;
import org.knowm.xchange.bybit.dto.trade.*;
import org.knowm.xchange.bybit.dto.BybitResult;
import org.knowm.xchange.bybit.dto.trade.details.BybitOrderDetail;
import org.knowm.xchange.bybit.dto.trade.details.BybitOrderDetails;
import org.knowm.xchange.client.ResilienceRegistries;
import org.knowm.xchange.exceptions.ExchangeException;

public class BybitTradeServiceRaw extends BybitBaseService {

  public BybitTradeServiceRaw (   BybitExchange exchange, ResilienceRegistries resilienceRegistries) {
    super(exchange, resilienceRegistries);
  }

  public BybitResult<BybitOrderDetails<BybitOrderDetail>> getBybitOrder(
      BybitCategory category, String orderId) throws IOException {
    BybitResult<BybitOrderDetails<BybitOrderDetail>> order =
        bybitAuthenticated.getOpenOrders(
            apiKey, signatureCreator, nonceFactory, category.getValue(), orderId);
    if (!order.isSuccess()) {
      throw createBybitExceptionFromResult(order);
    }
    return order;
  }

  public BybitResult<BybitOrderResponse> placeMarketOrder(
      BybitCategory category, String symbol, BybitSide side, BigDecimal qty, String orderLinkId)
      throws IOException {
    BybitPlaceOrderPayload payload = new BybitPlaceOrderPayload(category.getValue(),
        symbol, side.getValue(), BybitOrderType.MARKET.getValue(), qty, orderLinkId);
    BybitResult<BybitOrderResponse> placeOrder =
        bybitAuthenticated.placeMarketOrder(
            apiKey,
            signatureCreator,
            nonceFactory,
            payload);
    if (!placeOrder.isSuccess()) {
      throw createBybitExceptionFromResult(placeOrder);
    }
    return placeOrder;
  }

  public BybitResult<BybitOrderResponse> placeLimitOrder(
      BybitCategory category, String symbol, BybitSide side, BigDecimal qty, BigDecimal limitPrice,
      String orderLinkId, String timeInForce)
      throws IOException {
    BybitPlaceOrderPayload payload = new BybitPlaceOrderPayload(category.getValue(),
        symbol, side.getValue(), BybitOrderType.LIMIT.getValue(), qty, orderLinkId, limitPrice,timeInForce);
    BybitResult<BybitOrderResponse> placeOrder =
        bybitAuthenticated.placeLimitOrder(
            apiKey,
            signatureCreator,
            nonceFactory,
            payload);
    if (!placeOrder.isSuccess()) {
      throw createBybitExceptionFromResult(placeOrder);
    }
    return placeOrder;
  }

  public BybitResult<BybitPositionDetails<BybitPosition>> getPositions(
      BybitCategory category, String symbol, String settleCoin, String cursor       )
      throws BybitException, IOException {
    try {

      BybitResult<BybitPositionDetails<BybitPosition>> response = decorateApiCall(
          () -> bybitAuthenticated.getPositions(apiKey, signatureCreator, nonceFactory, category.getValue(), settleCoin, symbol,
              cursor,200)).withRateLimiter(rateLimiter(bybitAuthenticated.positionsPath)).call();

      if (response.getResult()!=null &&  response.getResult().getNextPageCursor()!=null){
        String nextCursor = response.getResult().getNextPageCursor();
        String priorCursor=null;
        while (nextCursor != null && !nextCursor.isEmpty() && !nextCursor.equals(priorCursor)) {
          priorCursor=nextCursor;
          String finalNextCursor = nextCursor;
          BybitResult<BybitPositionDetails<BybitPosition>>  loopResponse = decorateApiCall(
              () -> bybitAuthenticated.getPositions(apiKey, signatureCreator, nonceFactory, category.getValue(), settleCoin, symbol,
                  finalNextCursor,200)).withRateLimiter(rateLimiter(bybitAuthenticated.positionsPath)).call();
          response.getResult().getList().addAll(loopResponse.getResult().getList());

          if (loopResponse.getResult() != null && loopResponse.getResult().getNextPageCursor() != null) {
            nextCursor = loopResponse.getResult().getNextPageCursor();
          } else {
            nextCursor = null;

          }

        }
      }
      return response;
    } catch (BybitException e) {
      throw new ExchangeException(e);
    }
  }

  public BybitResult<BybitOrderResponse> cancelBybitOrder(BybitCancelOrderRequest order)
      throws IOException {
    try {
      return decorateApiCall(
          () ->
              bybitAuthenticated.cancelOrder(apiKey, signatureCreator, nonceFactory,
                  order))
          .withRateLimiter(rateLimiter(bybitAuthenticated.cancelOrderPath))
          .call();
    } catch (BybitException e) {
      throw new ExchangeException(e);
    }
  }

  public  BybitResult<BybitOrderDetails<BybitOrderDetail>> getBybitPendingOrder(
      BybitCategory category, String symbol, String settleCoin, String cursor ,String order       )
      throws BybitException, IOException {
    try {

      String finalCursor = cursor;
      BybitResult<BybitOrderDetails<BybitOrderDetail>> response = decorateApiCall(
          () -> bybitAuthenticated.getPendingOrders(apiKey, signatureCreator, nonceFactory, category.getValue(), settleCoin, symbol, finalCursor, order)).withRateLimiter(rateLimiter(bybitAuthenticated.positionsPath)).call();

      if (response.getResult() != null && response.getResult().getNextPageCursor() != null) {
        String nextCursor = response.getResult().getNextPageCursor();
        String priorCursor=null;
        while (nextCursor != null && !nextCursor.isEmpty() && !nextCursor.equals(priorCursor)) {
          priorCursor=nextCursor;
          String finalNextCursor = nextCursor;
          BybitResult<BybitOrderDetails<BybitOrderDetail>> loopResponse = decorateApiCall(
              () -> bybitAuthenticated.getPendingOrders(apiKey, signatureCreator, nonceFactory, category.getValue(), settleCoin, symbol, finalNextCursor, order)).withRateLimiter(rateLimiter(bybitAuthenticated.positionsPath)).call();
          response.getResult().getList().addAll(loopResponse.getResult().getList());
          if (loopResponse.getResult() != null && loopResponse.getResult().getNextPageCursor() != null) {
            nextCursor = loopResponse.getResult().getNextPageCursor();
          } else {
            nextCursor = null;

          }
        }
      }
      return response;
    } catch (BybitException e) {
      throw new ExchangeException(e);
    }
  }




  public BybitResult<BybitOrderResponse> amendOrder(BybitCategory category, String symbol, String orderId,
      String orderLinkId, String triggerPrice, String qty, String price, String tpslMode, String takeProfit,
      String stopLoss, String tpTriggerBy,String slTriggerBy,String triggerBy,String tpLimitPrice,
  String slLimitPrice) throws IOException {
    //if only userId is used, don't need to send id
    if(orderId!= null && orderId.isEmpty())
      orderId = null;
    BybitAmendOrderPayload payload = new BybitAmendOrderPayload(category, symbol,orderId,orderLinkId,triggerPrice,qty,price,
    tpslMode, takeProfit, stopLoss, tpTriggerBy, slTriggerBy, triggerBy, tpLimitPrice, slLimitPrice);
    BybitResult<BybitOrderResponse> amendOrder =
    bybitAuthenticated.amendOrder(
        apiKey,
        signatureCreator,
        nonceFactory,
        payload);
    if (!amendOrder.isSuccess()) {
      throw createBybitExceptionFromResult(amendOrder);
    }
    return amendOrder;
  }

}
