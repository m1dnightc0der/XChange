package org.knowm.xchange.binance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import org.junit.Test;
import org.knowm.xchange.binance.dto.account.AssetDividendResponse;
import org.knowm.xchange.binance.dto.trade.BinanceNewOrder;
import org.knowm.xchange.binance.dto.trade.BinanceOrder;
import org.knowm.xchange.binance.dto.trade.OrderStatus;
import org.knowm.xchange.binance.dto.trade.TimeInForce;
import org.knowm.xchange.binance.service.BinanceTradeService.BinanceOrderFlags;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.MarketOrder;
import org.knowm.xchange.utils.ObjectMapperHelper;

public class BinanceAdaptersTest {

  @Test
  public void testUsdMarginedFuturesOrderParsesPlainStringTimeInForce() throws IOException {
    String json =
        "{\"orderId\":1024466142987,\"symbol\":\"BTCUSDT\",\"status\":\"CANCELED\",\"clientOrderId\":\"5ecf3a\",\"price\":\"70000.00\",\"avgPrice\":\"0.00\",\"origQty\":\"0.010\",\"executedQty\":\"0.000\",\"cumQuote\":\"0.00000\",\"timeInForce\":\"GTC\",\"type\":\"LIMIT\",\"reduceOnly\":false,\"closePosition\":false,\"side\":\"BUY\",\"positionSide\":\"BOTH\",\"stopPrice\":\"0.00\",\"workingType\":\"CONTRACT_PRICE\",\"priceProtect\":false,\"origType\":\"LIMIT\",\"priceMatch\":\"NONE\",\"selfTradePreventionMode\":\"EXPIRE_MAKER\",\"goodTillDate\":0,\"time\":1780051688232,\"updateTime\":1780051701501}";

    BinanceOrder binanceOrder = ObjectMapperHelper.readValue(json, BinanceOrder.class);

    assertThat(binanceOrder.orderId).isEqualTo(1024466142987L);
    assertThat(binanceOrder.clientOrderId).isEqualTo("5ecf3a");
    assertThat(binanceOrder.getTimeInForceEnum()).isEqualTo(TimeInForce.GTC);
    assertThat(binanceOrder.status).isEqualTo(OrderStatus.CANCELED);
  }

  @Test
  public void testNewOrderParsesPlainStringTimeInForce() throws IOException {
    String json =
        "{\"symbol\":\"BTCUSDT\",\"orderId\":1024466142987,\"clientOrderId\":\"5ecf3a\",\"transactTime\":1780051688232,\"price\":\"70000.00\",\"origQty\":\"0.010\",\"executedQty\":\"0.000\",\"status\":\"NEW\",\"timeInForce\":\"GTC\",\"type\":\"LIMIT\",\"side\":\"BUY\",\"fills\":[]}";

    BinanceNewOrder binanceNewOrder = ObjectMapperHelper.readValue(json, BinanceNewOrder.class);

    assertThat(binanceNewOrder.orderId).isEqualTo(1024466142987L);
    assertThat(binanceNewOrder.clientOrderId).isEqualTo("5ecf3a");
    assertThat(binanceNewOrder.getTimeInForceEnum()).isEqualTo(TimeInForce.GTC);
    assertThat(binanceNewOrder.status).isEqualTo(OrderStatus.NEW);
  }

  @Test
  public void testFilledMarketOrder() throws IOException {

    BinanceOrder binanceOrder =
        ObjectMapperHelper.readValue(
            BinanceAdaptersTest.class.getResource(
                "/org/knowm/xchange/binance/filled-market-order.json"),
            BinanceOrder.class);
    Order order = BinanceAdapters.adaptOrder(binanceOrder, false);
    assertThat(order).isInstanceOf(MarketOrder.class);
    MarketOrder marketOrder = (MarketOrder) order;
    assertThat(marketOrder.getStatus()).isEqualByComparingTo(Order.OrderStatus.FILLED);
    assertThat(marketOrder.getOriginalAmount()).isEqualByComparingTo("0.10700000");
    assertThat(marketOrder.getCumulativeAmount()).isEqualByComparingTo("0.10700000");
    assertThat(marketOrder.getRemainingAmount()).isEqualByComparingTo(BigDecimal.ZERO);
    assertThat(marketOrder.getAveragePrice()).isEqualByComparingTo("0.01858383");
    assertThat(marketOrder.getOrderFlags())
        .contains(BinanceOrderFlags.withClientId("gzcLIkn86ag3FycOCEl6Vi"));

    MarketOrder copy = ObjectMapperHelper.viaJSON(marketOrder);
    assertThat(copy).usingRecursiveComparison().isEqualTo(marketOrder);
  }

  @Test
  public void testAssetDividendList() throws Exception {

    AssetDividendResponse assetDividendList =
        ObjectMapperHelper.readValue(
            BinanceAdaptersTest.class.getResource(
                "/org/knowm/xchange/binance/asset-dividend-list.json"),
            AssetDividendResponse.class);

    assertThat(assetDividendList.getTotal()).isEqualByComparingTo(BigDecimal.ONE);

    AssetDividendResponse.AssetDividend assetDividend = assetDividendList.getData().get(0);
    assertThat(assetDividend.getAmount()).isEqualByComparingTo(BigDecimal.valueOf(10L));
    assertThat(assetDividend.getAsset()).isEqualTo("BHFT");
    assertThat(assetDividend.getDivTime()).isEqualByComparingTo(1563189166000L);
    assertThat(assetDividend.getEnInfo()).isEqualTo("BHFT distribution");
    assertThat(assetDividend.getTranId()).isEqualByComparingTo(2968885920L);
  }

  // Tests that the conversion from Date/time String to Date is done for time zone UTC
  // regardless of the time zone of the system
  @Test
  public void testToDate() {
    String applyTimeUTC = "2018-10-09 07:56:10";
    assertThat(BinanceAdapters.toDate(applyTimeUTC).getTime()).isEqualByComparingTo(1539071770000L);
  }
}
