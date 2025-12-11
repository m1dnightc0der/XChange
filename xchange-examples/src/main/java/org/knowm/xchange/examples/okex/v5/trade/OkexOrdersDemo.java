package org.knowm.xchange.examples.okex.v5.trade;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.Order.OrderType;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.OpenOrders;
import org.knowm.xchange.okex.OkexExchange;
import org.knowm.xchange.okex.dto.trade.OkexTradeParams;
import org.knowm.xchange.okex.dto.trade.OkexTradeParams.OkexCancelOrderParams;
import org.knowm.xchange.service.trade.TradeService;
import org.knowm.xchange.service.trade.params.orders.ClientOrderIdQueryParamInstrument;
import org.knowm.xchange.service.trade.params.orders.DefaultQueryClientOrderIDParamInstrument;
import org.knowm.xchange.service.trade.params.orders.DefaultQueryOrderParamInstrument;
import org.knowm.xchange.service.trade.params.orders.OrderQueryParamInstrument;

public class OkexOrdersDemo {

  public static void main(String[] args) throws IOException {

    ExchangeSpecification exSpec = new ExchangeSpecification(OkexExchange.class);
    exSpec.setSecretKey("");
    exSpec.setApiKey("");
    exSpec.setExchangeSpecificParametersItem("passphrase", "");
    Exchange okexExchange = ExchangeFactory.INSTANCE.createExchange(exSpec);

    generic(okexExchange);
  }

  private static void generic(Exchange okexExchange) throws IOException {

    TradeService tradeService = okexExchange.getTradeService();
    FuturesContract contract = new FuturesContract(CurrencyPair.BTC_USDT, "251024");

    OpenOrders openOrders = tradeService.getOpenOrders();
    System.out.println(openOrders);

    /*    OpenPositions futuresPosition = tradeService.getOpenPositions();

    List<OpenPosition> positions = futuresPosition.getOpenPositions();

    for (OpenPosition position : positions) {
      System.out.println(position);
    }*/
    // Place 1 lot buy limit order at price 200 for the BTC_UST Sept 24th Contact
    try {

      LimitOrder limitOrder = new LimitOrder.Builder(Order.OrderType.BID, contract)
              .originalAmount(new BigDecimal("1"))        // Size: 0.2 ETH
              .limitPrice(new BigDecimal("99000"))
              .userReference("77dd55")// Price: $1100 (very low to ensure it rests)
              .build();


      String placeLimitOrder =
          tradeService.placeLimitOrder(limitOrder
             );
      System.out.println(placeLimitOrder);
      List<OrderQueryParamInstrument> params = new ArrayList<OrderQueryParamInstrument>();
      params.add(new DefaultQueryOrderParamInstrument(limitOrder.getInstrument(), placeLimitOrder));

      Collection<Order> orders = tradeService.getOrder(params.toArray(new OrderQueryParamInstrument[params.size()]));

      List<ClientOrderIdQueryParamInstrument> clientOidparams = new ArrayList<ClientOrderIdQueryParamInstrument>();
      clientOidparams.add(new DefaultQueryClientOrderIDParamInstrument(limitOrder.getInstrument(), limitOrder.getUserReference()));

      orders = tradeService.getOrder(clientOidparams.toArray(new ClientOrderIdQueryParamInstrument[clientOidparams.size()]));




      OkexCancelOrderParams req =
          new OkexTradeParams.OkexCancelOrderParams(contract, placeLimitOrder);

      boolean cancelOrder = tradeService.cancelOrder(req);
      System.out.println("Cancelled " + cancelOrder);
    } catch (Exception | Error ex) {
      System.out.println("Unable to place order due to  " + ex);
    }
  }
}
