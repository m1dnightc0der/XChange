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
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.okex.v5.OkexExchange;
import org.knowm.xchange.okex.v5.dto.trade.OkexPriceLimit;
import org.knowm.xchange.okex.v5.dto.trade.OkexTradeParams;
import org.knowm.xchange.okex.v5.dto.trade.OkexTradeParams.OkexCancelOrderParams;
import org.knowm.xchange.okex.v5.service.OkexTradeService;
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

    OkexTradeService tradeService = (OkexTradeService) okexExchange.getTradeService();
    Instrument contract = new FuturesContract(CurrencyPair.BTC_USDT, "SWAP");
    OkexPriceLimit prcelimits = tradeService.getFuturesPriceLimits(contract);
    contract = new CurrencyPair("BTC", "USDT");
    prcelimits = tradeService.getFuturesPriceLimits(contract);

    /*    OpenPositions futuresPosition = tradeService.getOpenPositions();

    List<OpenPosition> positions = futuresPosition.getOpenPositions();

    for (OpenPosition position : positions) {
      System.out.println(position);
    }*/
    // Place 1 lot buy limit order at price 200 for the BTC_UST Sept 24th Contact
    try {
      String placeLimitOrder =
          tradeService.placeLimitOrder(
              new LimitOrder(
                  OrderType.EXIT_ASK,
                  new BigDecimal("0.00001"),
                  // new BigDecimal("1"),
                  contract,
                  "0",
                  new Date(),
                  new BigDecimal("57300")));
      System.out.println(placeLimitOrder);
      List<OrderQueryParamInstrument> params = new ArrayList<OrderQueryParamInstrument>();

      params.add(new DefaultQueryOrderParamInstrument(contract, placeLimitOrder));
      Collection<Order> openOrders =
          tradeService.getOrder(params.toArray(new OrderQueryParamInstrument[params.size()]));
      System.out.println(openOrders);

      OkexCancelOrderParams req =
          new OkexTradeParams.OkexCancelOrderParams(contract, placeLimitOrder);

      boolean cancelOrder = tradeService.cancelOrder(req);
      System.out.println("Cancelled " + cancelOrder);
    } catch (Exception | Error ex) {
      System.out.println("Unable to place order due to  " + ex);
    }
  }
}
