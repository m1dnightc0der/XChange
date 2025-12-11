package info.bitrich.xchangestream.hyperliquid;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;

import java.util.concurrent.TimeUnit;
import io.reactivex.rxjava3.disposables.Disposable;
import org.junit.Before;
import org.junit.Test;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.instrument.Instrument;

public class HyperliquidStreamingPublicDataIntegration {

  private StreamingExchange exchange;
  private final Instrument instrument = CurrencyPair.BTC_USD;

  @Before
  public void setUp() {
    exchange = StreamingExchangeFactory.INSTANCE.createExchange(HyperliquidStreamingExchange.class);
  }

  @Test
  public void testTicker() throws InterruptedException {
    exchange.connect().blockingAwait();
    
    Disposable dis = exchange
        .getStreamingMarketDataService()
        .getTicker(instrument)
        .subscribe(ticker -> {
          System.out.println("Ticker: " + ticker);
          assertThat(ticker.getInstrument()).isEqualTo(instrument);
        });
    
    TimeUnit.SECONDS.sleep(10);
    dis.dispose();
    exchange.disconnect().blockingAwait();
  }

  @Test
  public void testOrderBook() throws InterruptedException {
    exchange.connect().blockingAwait();
    
    Disposable dis = exchange
        .getStreamingMarketDataService()
        .getOrderBook(instrument)
        .subscribe(orderBook -> {
          System.out.println("Order Book: " + orderBook);
          if (!orderBook.getBids().isEmpty() && !orderBook.getAsks().isEmpty()) {
            assertThat(orderBook.getBids().get(0).getLimitPrice())
                .isLessThan(orderBook.getAsks().get(0).getLimitPrice());
          }
        });
    
    TimeUnit.SECONDS.sleep(10);
    dis.dispose();
    exchange.disconnect().blockingAwait();
  }

  @Test
  public void testTrades() throws InterruptedException {
    exchange.connect().blockingAwait();
    
    Disposable dis = exchange
        .getStreamingMarketDataService()
        .getTrades(instrument)
        .subscribe(trade -> {
          System.out.println("Trade: " + trade);
          assertThat(trade.getInstrument()).isEqualTo(instrument);
        });
    
    TimeUnit.SECONDS.sleep(10);
    dis.dispose();
    exchange.disconnect().blockingAwait();
  }

  @Test
  public void testTestnetConnection() throws InterruptedException {
    // Test testnet connection
    StreamingExchange testnetExchange = StreamingExchangeFactory.INSTANCE.createExchange(HyperliquidStreamingExchange.class);
    testnetExchange.getExchangeSpecification().setExchangeSpecificParametersItem("Use_Sandbox", true);
    
    testnetExchange.connect().blockingAwait();
    
    Disposable dis = testnetExchange
        .getStreamingMarketDataService()
        .getTicker(instrument)
        .subscribe(ticker -> {
          System.out.println("Testnet Ticker: " + ticker);
        });
    
    TimeUnit.SECONDS.sleep(5);
    dis.dispose();
    testnetExchange.disconnect().blockingAwait();
  }
}