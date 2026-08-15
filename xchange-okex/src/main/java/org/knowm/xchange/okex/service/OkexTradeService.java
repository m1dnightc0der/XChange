package org.knowm.xchange.okex.service;

import static org.knowm.xchange.okex.OkexAdapters.*;
import static org.knowm.xchange.okex.OkexExchange.PARAM_CONVERT_QUANTITIES;
import static org.knowm.xchange.okex.OkexExchange.PARAM_USE_AWS;

import jakarta.ws.rs.NotSupportedException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.knowm.xchange.client.ResilienceRegistries;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.derivative.OptionsContract;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.account.OpenPositions;
import org.knowm.xchange.dto.trade.*;
import org.knowm.xchange.exceptions.FundsExceededException;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.okex.OkexAdapters;
import org.knowm.xchange.okex.OkexExchange;
import org.knowm.xchange.okex.dto.trade.*;
import org.knowm.xchange.okex.dto.OkexException;
import org.knowm.xchange.okex.dto.OkexResponse;
import org.knowm.xchange.service.trade.TradeService;
import org.knowm.xchange.service.trade.params.CancelOrderByIdParams;
import org.knowm.xchange.service.trade.params.CancelOrderByInstrument;
import org.knowm.xchange.service.trade.params.CancelOrderParams;
import org.knowm.xchange.service.trade.params.TradeHistoryParamInstrument;
import org.knowm.xchange.service.trade.params.TradeHistoryParams;
import org.knowm.xchange.service.trade.params.orders.*;


/** Author: Max Gao (gaamox@tutanota.com) Created: 08-06-2021 */
public class OkexTradeService extends OkexTradeServiceRaw implements TradeService {
  public OkexTradeService(OkexExchange exchange, ResilienceRegistries resilienceRegistries) {
    super(exchange, resilienceRegistries);
  }

  @Override
  public OpenPositions getOpenPositions() throws IOException {
    return OkexAdapters.adaptOpenPositions(
        getPositions(null, null, null), Boolean.TRUE.equals(exchange.getExchangeSpecification().getExchangeSpecificParametersItem(PARAM_CONVERT_QUANTITIES)) ? exchange.getExchangeMetaData() : null);
  }

  @Override
  public UserTrades getTradeHistory(TradeHistoryParams params) throws IOException {
    if (params instanceof TradeHistoryParamInstrument) {
      Instrument instrument = ((TradeHistoryParamInstrument) params).getInstrument();

      String instrumentType = SPOT;
      if (instrument instanceof FuturesContract) {
        instrumentType = SWAP;
      } else if (instrument instanceof OptionsContract) {
        instrumentType = OPTION;
      }

      return OkexAdapters.adaptUserTrades(
          getOrderHistory(
                  instrumentType,
                  OkexAdapters.adaptInstrument(
                      ((TradeHistoryParamInstrument) params).getInstrument()),
                  null,
                  null,
                  null,
                  null)
              .getData(),
          null);
    } else {
      throw new NotSupportedException(
          "TradeHistoryParams must implement " + TradeHistoryParamInstrument.class.getSimpleName());
    }
  }

  @Override
  public OpenOrders getOpenOrders() throws IOException {
    return OkexAdapters.adaptOpenOrders(
        getOkexPendingOrder(null, null, null, null, null, null, null, null).getData(),
        null);
  }

  public OkexPriceLimit getFuturesPriceLimits(Instrument instrument) throws IOException {
    return getOkexPriceLimits(OkexAdapters.adaptInstrument(instrument));
  }

  @Override public OpenOrders getOpenOrders(OpenOrdersParams params) throws IOException {
    if (params instanceof OpenOrdersParamInstrument) {
      return OkexAdapters.adaptOpenOrders(
          getOkexPendingOrder(
                  null,
                  null,
                  OkexAdapters.adaptInstrument(
                      ((OpenOrdersParamInstrument) params).getInstrument()),
                  null,
                  null,
                  null,
                  null,
                  null)
              .getData(),
          Boolean.TRUE.equals(exchange.getExchangeSpecification().getExchangeSpecificParametersItem(PARAM_CONVERT_QUANTITIES)) ? exchange.getExchangeMetaData() : null);
    } else {
      throw new NotSupportedException(
          "OpenOrdersParam must implement " + OpenOrdersParamInstrument.class.getSimpleName());
    }
  }

  @Override
  public Class getRequiredOrderQueryParamClass() {
    return OrderQueryParamInstrument.class;
  }

  public Order getOrder(OrderQueryParams orderQueryParams) throws IOException {
    Order result = null;

    if (orderQueryParams instanceof OrderQueryParamInstrument) {
      Instrument instrument = ((OrderQueryParamInstrument) orderQueryParams).getInstrument();
      String orderId = orderQueryParams.getOrderId();

      OkexResponse<List<OkexOrderDetails>> okexResponse = getOkexOrder(adaptInstrument(instrument), orderId);
      if (!okexResponse.isSuccess()) {
        throw OkexAdapters.adaptError(okexResponse.getCode(), okexResponse.getMsg());
      }
      List<OkexOrderDetails> orderResults = okexResponse.getData();

      if (orderResults != null && !orderResults.isEmpty()) {
        result = OkexAdapters.adaptOrder(orderResults.get(0), Boolean.TRUE.equals(exchange.getExchangeSpecification().getExchangeSpecificParametersItem(PARAM_CONVERT_QUANTITIES)) ? exchange.getExchangeMetaData() : null);
      }
    } else if (orderQueryParams instanceof ClientOrderIdQueryParamInstrument){
      Instrument instrument = ((ClientOrderIdQueryParamInstrument) orderQueryParams).getInstrument();
      String orderId = orderQueryParams.getOrderId();

      OkexResponse<List<OkexOrderDetails>> okexResponse =
          getOkexClientOrder(OkexAdapters.adaptInstrument(instrument), orderId);
      if (!okexResponse.isSuccess()) {
        throw OkexAdapters.adaptError(okexResponse.getCode(), okexResponse.getMsg());
      }
      List<OkexOrderDetails> orderResults = okexResponse.getData();

      if (orderResults != null && !orderResults.isEmpty()) {
        result = OkexAdapters.adaptOrder(orderResults.get(0), Boolean.TRUE.equals(exchange.getExchangeSpecification().getExchangeSpecificParametersItem(PARAM_CONVERT_QUANTITIES)) ? exchange.getExchangeMetaData() : null);
      }
    }


    else {
      throw new IOException("OrderQueryParams must implement OrderQueryParamInstrument interface.");
    }
    return result;
  }

  @Override
  public Collection<Order> getOrder(OrderQueryParams... orderQueryParams) throws IOException {
    ArrayList<Order> result = new ArrayList<>();
    for (OrderQueryParams orderQueryParam : orderQueryParams) {
      Order order = getOrder(orderQueryParam);
      if (order != null) {
        result.add(order);
      }
    }
    return result;
  }

  @Override
  public String placeMarketOrder(MarketOrder marketOrder) throws IOException {
    OkexResponse<List<OkexOrderResponse>> okexResponse =
        placeOkexOrder(
            OkexAdapters.adaptOrder(
                marketOrder, (Boolean.TRUE.equals(exchange.getExchangeSpecification().getExchangeSpecificParametersItem(PARAM_CONVERT_QUANTITIES)) ? exchange.getExchangeMetaData() : null), exchange.accountLevel));
    return successfulOrderIdOrThrow(okexResponse, false, "market order");
  }

  @Override
  public String placeLimitOrder(LimitOrder limitOrder) throws IOException, FundsExceededException {
    OkexResponse<List<OkexOrderResponse>> okexResponse =
        placeOkexOrder(
            OkexAdapters.adaptOrder(
                limitOrder,(Boolean.TRUE.equals(exchange.getExchangeSpecification().getExchangeSpecificParametersItem(PARAM_CONVERT_QUANTITIES)) ? exchange.getExchangeMetaData() : null), exchange.accountLevel));
    return successfulOrderIdOrThrow(okexResponse, false, "limit order");
  }

  @Override public String placeStopOrder(StopOrder order) throws IOException {
    OkexResponse<List<OkexOrderResponse>> okexResponse = placeOkexAlgoOrder(OkexAdapters.adaptOrder(order, (Boolean.TRUE.equals(exchange.getExchangeSpecification().getExchangeSpecificParametersItem(PARAM_CONVERT_QUANTITIES)) ? exchange.getExchangeMetaData() : null)));
    return successfulOrderIdOrThrow(okexResponse, true, "stop order");
  }

  private String successfulOrderIdOrThrow(
      OkexResponse<List<OkexOrderResponse>> response, boolean algoOrder, String context) {
    if (response == null) {
      throw OkexAdapters.adaptError("unknown", "OKX " + context + " response missing");
    }
    List<OkexOrderResponse> data = response.getData();
    OkexOrderResponse first = data == null || data.isEmpty() ? null : data.get(0);
    if (!response.isSuccess()) {
      String rawCode = first != null && first.getCode() != null ? first.getCode() : response.getCode();
      String message = first != null && first.getMessage() != null ? first.getMessage() : response.getMsg();
      throw OkexAdapters.adaptError(rawCode, message);
    }
    if (first == null) {
      throw OkexAdapters.adaptError("0", "OKX successful " + context + " response missing data");
    }
    String orderId = algoOrder ? first.getAlgoOrderId() : first.getOrderId();
    if (orderId == null || orderId.isEmpty()) {
      throw OkexAdapters.adaptError("0", "OKX successful " + context + " response missing order ID");
    }
    return orderId;
  }

  public List<String> placeLimitOrder(List<LimitOrder> limitOrders)
      throws IOException, FundsExceededException {
    return placeOkexOrder(
        limitOrders.stream()
            .map(
                order ->
                    OkexAdapters.adaptOrder(
                        order, (Boolean.TRUE.equals(exchange.getExchangeSpecification().getExchangeSpecificParametersItem(PARAM_CONVERT_QUANTITIES)) ? exchange.getExchangeMetaData() : null), exchange.accountLevel))
            .collect(Collectors.toList()))
        .getData()
        .stream()
        .map(OkexOrderResponse::getOrderId)
        .collect(Collectors.toList());
  }

  @Override
  public String changeOrder(LimitOrder limitOrder) throws IOException, FundsExceededException {

    OkexResponse<List<OkexOrderResponse>> okexResponse = amendOkexOrder(adaptAmendOrder(limitOrder, (Boolean.TRUE.equals(exchange.getExchangeSpecification().getExchangeSpecificParametersItem(PARAM_CONVERT_QUANTITIES)) ? exchange.getExchangeMetaData() : null)));
    return validateAmendOrderResponse(okexResponse, limitOrder.getId());
  }

  private String validateAmendOrderResponse(
      OkexResponse<List<OkexOrderResponse>> okexResponse, String expectedOrderId) {
    return validateAmendOrderResponses(okexResponse, java.util.Collections.singletonList(expectedOrderId))
        .get(0)
        .getOrderId();
  }

  private List<OkexOrderResponse> validateAmendOrderResponses(
      OkexResponse<List<OkexOrderResponse>> okexResponse, List<String> expectedOrderIds) {
    validateAmendResponseEnvelope(okexResponse);
    List<OkexOrderResponse> data = okexResponse.getData();
    if (data == null || data.isEmpty()) {
      throw new OkexException(
          "OKX amend order response missing data", parseOkexCode(okexResponse.getCode()));
    }
    for (int index = 0; index < data.size(); index++) {
      String expectedOrderId =
          expectedOrderIds != null && index < expectedOrderIds.size() ? expectedOrderIds.get(index) : null;
      OkexOrderResponse result = data.get(index);
      validateAmendOrderResult(result, expectedOrderId);
      String orderId = result.getOrderId();
      if (orderId == null || orderId.isEmpty()) {
        throw new OkexException(
            "OKX amend order response missing ordId", parseOkexCode(result.getCode()));
      }
    }
    return data;
  }

  private void validateAmendResponseEnvelope(OkexResponse<List<OkexOrderResponse>> okexResponse) {
    if (okexResponse == null) {
      throw new OkexException("OKX amend order response missing", 0);
    }
    if (!okexResponse.isSuccess()) {
      OkexOrderResponse firstFailure = firstAmendFailureRow(okexResponse.getData());
      int exceptionCode = firstFailure != null && firstFailure.getCode() != null
          ? parseOkexCode(firstFailure.getCode())
          : parseOkexCode(okexResponse.getCode());
      throw new OkexException(buildAmendFailureMessage(okexResponse), exceptionCode);
    }
  }

  private OkexOrderResponse firstAmendFailureRow(List<OkexOrderResponse> data) {
    if (data == null) {
      return null;
    }
    for (OkexOrderResponse row : data) {
      if (row != null && row.getCode() != null && !"0".equals(row.getCode())) {
        return row;
      }
    }
    return null;
  }

  private String buildAmendFailureMessage(OkexResponse<List<OkexOrderResponse>> okexResponse) {
    StringBuilder message = new StringBuilder("OKX amend order failed");

    List<OkexOrderResponse> data = okexResponse.getData();
    if (data != null && !data.isEmpty()) {
      for (int index = 0; index < data.size(); index++) {
        OkexOrderResponse row = data.get(index);
        if (row == null) {
          message.append(" row[").append(index).append("]=null");
          continue;
        }
        message
            .append(" row[").append(index).append("]")
            .append(" ordId=").append(row.getOrderId())
            .append(" sCode=").append(row.getCode())
            .append(" sMsg=").append(row.getMessage());
      }
      return message.toString();
    }

    message.append(" code=").append(okexResponse.getCode());
    message.append(" msg=").append(okexResponse.getMsg());
    return message.toString();
  }

  private void validateAmendOrderResult(OkexOrderResponse result, String expectedOrderId) {
    if (result == null) {
      throw new OkexException("OKX amend order response missing result", 0);
    }
    String resultCode = result.getCode();
    String resultMessage = result.getMessage();
    String orderId = result.getOrderId();
    if (!"0".equals(resultCode)) {
      throw new OkexException(
          "OKX amend order failed sCode="
              + resultCode
              + " sMsg="
              + resultMessage
              + " ordId="
              + orderId,
          parseOkexCode(resultCode));
    }
    if (expectedOrderId != null
        && !expectedOrderId.isEmpty()
        && orderId != null
        && !orderId.isEmpty()
        && !expectedOrderId.equals(orderId)) {
      throw new OkexException(
          "OKX amend order returned unexpected ordId="
              + orderId
              + " expectedOrdId="
              + expectedOrderId,
          parseOkexCode(resultCode));
    }
  }

  private int parseOkexCode(String code) {
    if (code == null || code.isEmpty()) {
      return 0;
    }
    try {
      return Integer.parseInt(code);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  public List<String> changeOrder(List<LimitOrder> limitOrders)
      throws IOException, FundsExceededException {
    OkexResponse<List<OkexOrderResponse>> okexResponse = amendOkexOrder(
            limitOrders.stream()
                .map(order -> OkexAdapters.adaptAmendOrder(order, (Boolean.TRUE.equals(exchange.getExchangeSpecification().getExchangeSpecificParametersItem(PARAM_CONVERT_QUANTITIES)) ? exchange.getExchangeMetaData() : null)))
                .collect(Collectors.toList()));
    return validateAmendOrderResponses(
            okexResponse, limitOrders.stream().map(LimitOrder::getId).collect(Collectors.toList()))
        .stream()
        .map(OkexOrderResponse::getOrderId)
        .collect(Collectors.toList());
  }

  @Override public boolean cancelOrder(CancelOrderParams params) throws IOException {
    if (params instanceof OkexTradeParams.OkexCancelOrderParams) {
      OkexTradeParams.OkexCancelOrderParams okexCancelOrderParams = (OkexTradeParams.OkexCancelOrderParams) params;
      String id = ((CancelOrderByIdParams) params).getOrderId();
      String instrumentId = OkexAdapters.adaptInstrument(((CancelOrderByInstrument) params).getInstrument());
      boolean isAlgo = okexCancelOrderParams.getIsAlgoOrder();
      OkexCancelOrderRequest req = OkexCancelOrderRequest.builder().instrumentId(instrumentId).orderId(id).build();
      if (isAlgo) {
        return "0".equals(cancelOkexAlgoOrder(req).getData().get(0).getCode());
      } else {
        return "0".equals(cancelOkexOrder(req).getData().get(0).getCode());
      }
    } else if (params instanceof CancelOrderByIdParams && params instanceof CancelOrderByInstrument) {

      String id = ((CancelOrderByIdParams) params).getOrderId();
      String instrumentId = OkexAdapters.adaptInstrument(((CancelOrderByInstrument) params).getInstrument());

      OkexCancelOrderRequest req = OkexCancelOrderRequest.builder().instrumentId(instrumentId).orderId(id).build();

      return "0".equals(cancelOkexOrder(req).getData().get(0).getCode());
    } else {
      throw new IOException("CancelOrderParams must implement CancelOrderByIdParams and CancelOrderByInstrument interface.");
    }
  }

  @Override public Class[] getRequiredCancelOrderParamClasses() {
    return new Class[]{CancelOrderByIdParams.class, CancelOrderByInstrument.class};
  }

  public List<Boolean> cancelOrder(List<CancelOrderParams> params) throws IOException {
    return cancelOkexOrder(
            params.stream()
                .map(
                    param ->
                        OkexCancelOrderRequest.builder()
                            .orderId(((CancelOrderByIdParams) param).getOrderId())
                            .instrumentId(
                                OkexAdapters.adaptInstrument(
                                    ((CancelOrderByInstrument) param).getInstrument()))
                            .build())
                .collect(Collectors.toList()))
        .getData()
        .stream()
        .map(result -> "0".equals(result.getCode()))
        .collect(Collectors.toList());
  }
}