package info.bitrich.xchangestream.deribit;

import com.fasterxml.jackson.core.JsonProcessingException;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import info.bitrich.xchangestream.deribit.DeribitStreamingExchange;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.deribit.v2.DeribitExchange;

@Ignore
public class DeribitStreamingPrivateDataIntegtration {

  StreamingExchange exchange;
  private final Instrument instrument = new FuturesContract("BTC/USD/PERPETUAL");

  @Before
  public void setUp() {
    Properties properties = new Properties();


    // Enter your authentication details here to run private endpoint tests
    final String API_KEY = "6NXzzcio";

    final String SECRET_KEY = "hKMKKeC7VhtY-fhqHkNmbl95dMZUpDXa2OfkkQCCxEk";

    ExchangeSpecification spec = new DeribitStreamingExchange().getDefaultExchangeSpecification();
    spec.setApiKey(API_KEY);
    spec.setSecretKey(SECRET_KEY);

    exchange = StreamingExchangeFactory.INSTANCE.createExchange(DeribitStreamingExchange.class);
    exchange.applySpecification(spec);

    exchange.connect().blockingAwait();
  }

  @Test
  public void checkSendMessage() throws Exception {
    Instrument instrument = new FuturesContract("BTC/USD/PERPETUAL");
    BigDecimal size = BigDecimal.TEN;
    BigDecimal price = BigDecimal.valueOf(80000);
    TimeUnit.SECONDS.sleep(10);
    String orderId = exchange
            .getStreamingTradeService()
            .placeLimitOrder(
                    new LimitOrder.Builder(Order.OrderType.BID, instrument)
                            .originalAmount(size)
                            .limitPrice(price)
                            .build());

    String orderId2 = exchange
            .getStreamingTradeService()
            .placeLimitOrder(
                    new LimitOrder.Builder(Order.OrderType.BID, instrument)
                            .originalAmount(size)
                            .limitPrice(price)
                            .build());
    TimeUnit.SECONDS.sleep(10);
    size = BigDecimal.ONE;
  }
  @Test
  public void checkUserTradesStream() throws InterruptedException {
    Disposable dis = null;
    try {
      dis = exchange
          .getStreamingTradeService()
          .getOrderChanges(instrument, new Object[0])
          .subscribe(System.out::println);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    TimeUnit.SECONDS.sleep(300000);

    dis.dispose();
  }
}
