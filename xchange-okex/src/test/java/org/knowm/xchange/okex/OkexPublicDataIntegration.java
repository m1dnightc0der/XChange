package org.knowm.xchange.okex;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.dto.marketdata.FundingRate;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.marketdata.Trades;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.okex.dto.OkexInstType;
import org.knowm.xchange.okex.dto.OkexResponse;
import org.knowm.xchange.okex.dto.marketdata.OkexCandleStick;
import org.knowm.xchange.okex.service.OkexMarketDataService;

public class OkexPublicDataIntegration {

  Exchange exchange;
  private final Instrument currencyPair = new CurrencyPair("BTC/USDT");
  private final Instrument instrument = new FuturesContract("BTC/USDT/SWAP");

  @Before
  public void setUp() {
    exchange = ExchangeFactory.INSTANCE.createExchange(OkexExchange.class);
  }

  @Test
  public void checkInstrumentMetaData() {
    exchange
        .getExchangeMetaData()
        .getInstruments()
        .forEach(
            (instrument1, instrumentMetaData) -> {
              System.out.println(instrument1 + "||" + instrumentMetaData);
              assertThat(instrumentMetaData.getMinimumAmount()).isGreaterThan(BigDecimal.ZERO);
              assertThat(instrumentMetaData.getPriceScale()).isGreaterThanOrEqualTo(0);
              assertThat(instrumentMetaData.getVolumeScale()).isNotNull();
              if (instrument1 instanceof FuturesContract) {
                // OKX supports both linear (USDT) and inverse (USD) perpetuals
                // Linear contracts: BTC-USDT-SWAP (counter = USDT)
                // Inverse contracts: BTC-USD-SWAP (counter = USD)
                Currency counter = instrument1.getCounter();
                assertTrue("FuturesContract counter should be USDT or USD",
                    counter.equals(Currency.USDT) || counter.equals(Currency.USD));
              }
            });
  }

  @Test
  public void checkOrderBook() throws IOException {
    LimitOrder spotOrder =
        exchange.getMarketDataService().getOrderBook(currencyPair).getBids().get(0);
    LimitOrder swapOrder =
        exchange.getMarketDataService().getOrderBook(instrument).getBids().get(0);

    assertThat(spotOrder.getInstrument()).isEqualTo(currencyPair);
    assertThat(swapOrder.getInstrument()).isEqualTo(instrument);
  }

  @Test
  public void checkTicker() throws IOException {
    Ticker spotTicker = exchange.getMarketDataService().getTicker(currencyPair);
    Ticker swapTicker = exchange.getMarketDataService().getTicker(instrument);

    assertThat(spotTicker.getInstrument()).isEqualTo(currencyPair);
    assertThat(swapTicker.getInstrument()).isEqualTo(instrument);
  }

  @Test
  public void checkTickers() throws IOException {
    List<Ticker> spotTickers = exchange.getMarketDataService().getTickers(OkexInstType.SPOT);
    List<Ticker> swapTickers = exchange.getMarketDataService().getTickers(OkexInstType.SWAP);

    assertTrue(spotTickers.stream().anyMatch(f->f.getInstrument().equals(new CurrencyPair("BTC/USDT"))));
    assertTrue(swapTickers.stream().anyMatch(f -> f.getInstrument().equals(new FuturesContract("BTC/USDT/SWAP"))));
  }

  @Test
  public void checkTrades() throws IOException {
    Trades spotTrades = exchange.getMarketDataService().getTrades(currencyPair);
    Trades swapTrades = exchange.getMarketDataService().getTrades(instrument);

    assertThat(spotTrades.getTrades().get(0).getInstrument()).isEqualTo(currencyPair);
    assertThat(swapTrades.getTrades().get(0).getInstrument()).isEqualTo(instrument);
    assertThat(swapTrades.getTrades().get(0).getTimestamp())
        .isBeforeOrEqualTo(swapTrades.getTrades().get(5).getTimestamp());
  }

  @Test
  @Ignore
  public void testCandleHist() throws IOException {
    OkexResponse<List<OkexCandleStick>> barHistDtos =
        ((OkexMarketDataService) exchange.getMarketDataService())
            .getHistoryCandle("BTC-USDT", null, null, null, null);
    assertTrue(Objects.nonNull(barHistDtos) && !barHistDtos.getData().isEmpty());
  }

  @Test
  public void checkFundingRate() throws IOException {
    FundingRate fundingRate = exchange.getMarketDataService().getFundingRate(instrument);
    System.out.println(fundingRate);
    assertThat(fundingRate.getFundingRateDate()).isNotNull();
  }

  @Test
  public void testInstrumentOkexConvertions() {
    assertThat(OkexAdapters.adaptOkexInstrumentId("BTC-USDT-SWAP"))
        .isEqualTo(new FuturesContract("BTC/USDT/SWAP"));
    assertThat(OkexAdapters.adaptInstrument(new FuturesContract("BTC/USDT/SWAP")))
        .isEqualTo("BTC-USDT-SWAP");
    assertThat(OkexAdapters.adaptOkexInstrumentId("BTC-USDT"))
        .isEqualTo(new CurrencyPair("BTC/USDT"));
    assertThat(OkexAdapters.adaptInstrument(new CurrencyPair("BTC/USDT"))).isEqualTo("BTC-USDT");
  }

  @Test
  public void xchangeInverseFuturesInstrumentMappedToOkxInstIdCode() {
    Map<String, Integer> map = ((OkexExchange) exchange).getInstrumentCodeMap();
    Instrument instrument = new FuturesContract(CurrencyPair.BTC_USD, "260206");
    Integer instIdCode = OkexAdapters.adaptInstrumentCode(instrument, map);
    assertThat(instIdCode).isEqualTo(245697);
  }

  @Test
  public void xchangeLinearFuturesInstrumentMappedToOkxInstIdCode() {
    Map<String, Integer> map = ((OkexExchange) exchange).getInstrumentCodeMap();
    Instrument instrument = new FuturesContract(CurrencyPair.ETH_USDT, "260327");
    Integer instIdCode = OkexAdapters.adaptInstrumentCode(instrument, map);
    assertThat(instIdCode).isEqualTo(227100);
  }

  @Test
  public void xchangeSpotInstrumentMappedToOkxInstIdCode() {
    Map<String, Integer> map = ((OkexExchange) exchange).getInstrumentCodeMap();
    Instrument instrument = new CurrencyPair("HYPE", "USDT");
    Integer instIdCode = OkexAdapters.adaptInstrumentCode(instrument, map);
    assertThat(instIdCode).isEqualTo(234453);
  }

  @Test
  public void xchangeLinearSwapInstrumentMappedToOkxInstIdCode() {
    Map<String, Integer> map = ((OkexExchange) exchange).getInstrumentCodeMap();
    Instrument instrument = new FuturesContract(CurrencyPair.BTC_USDT, "SWAP");
    Integer instIdCode = OkexAdapters.adaptInstrumentCode(instrument, map);
    assertThat(instIdCode).isEqualTo(10459);
  }

  @Test
  public void xchangeInverseSwapInstrumentMappedToOkxInstIdCode() {
    Map<String, Integer> map = ((OkexExchange) exchange).getInstrumentCodeMap();
    Instrument instrument = new FuturesContract(CurrencyPair.BTC_USD, "SWAP");
    Integer instIdCode = OkexAdapters.adaptInstrumentCode(instrument, map);
    assertThat(instIdCode).isEqualTo(10458);
  }

  @Test(expected = IllegalArgumentException.class)
  public void xchangeInvalidSwapInstrumentNotMappedToOkxInstIdCode() {
    Map<String, Integer> map = ((OkexExchange) exchange).getInstrumentCodeMap();
    Instrument instrument = new FuturesContract(new CurrencyPair("DMC", "USDT"), "SWAP");
    OkexAdapters.adaptInstrumentCode(instrument, map);
  }

  @Test(expected = IllegalArgumentException.class)
  public void xchangeNullInstrumentNotMappedToOkxInstIdCode() {
    Map<String, Integer> map = ((OkexExchange) exchange).getInstrumentCodeMap();
    OkexAdapters.adaptInstrumentCode(null, map);
  }
}
