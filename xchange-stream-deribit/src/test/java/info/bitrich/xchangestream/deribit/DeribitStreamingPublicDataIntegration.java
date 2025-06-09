package info.bitrich.xchangestream.deribit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import io.reactivex.rxjava3.disposables.Disposable;
import org.junit.Before;
import org.junit.Test;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.derivative.OptionsContract;
import org.knowm.xchange.instrument.Instrument;

public class DeribitStreamingPublicDataIntegration {

  private StreamingExchange exchange;
  private final Instrument instrument = new FuturesContract("BTC/USD/4OCT24");

  @Before
  public void setUp() {
    exchange = StreamingExchangeFactory.INSTANCE.createExchange(DeribitStreamingExchange.class);

  }

  @Test
  public void testTrades() throws InterruptedException {
      exchange.connect().blockingAwait();
    Disposable dis =
        exchange
            .getStreamingMarketDataService()
            .getTrades(instrument)
            .subscribe(
                trade -> {
                  System.out.println(trade);
                  //assertThat(trade.getInstrument()).isEqualTo(instrument);
                });

    TimeUnit.SECONDS.sleep(3000000);
    dis.dispose();

  }

  @Test
  public void testTicker() throws InterruptedException {
      exchange.connect().blockingAwait();
    Disposable dis =
        exchange
            .getStreamingMarketDataService()
            .getTicker(instrument)
            .subscribe(System.out::println);
    Disposable dis2 =
        exchange
            .getStreamingMarketDataService()
            .getTicker(instrument)
            .subscribe(System.out::println);
    TimeUnit.SECONDS.sleep(3);
    dis.dispose();
    dis2.dispose();
  }

  @Test
  public void testMassTicker() throws InterruptedException {
      //exchange.connect().blockingAwait();

    List<Disposable> disposables=new ArrayList<Disposable>();
List<OptionsContract> outrights=new ArrayList<OptionsContract>();
    outrights.add(new OptionsContract("BTC/USD/250131/25000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/25000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/30000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/30000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/35000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/35000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/40000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/40000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/45000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/45000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/50000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/50000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/52000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/52000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/54000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/54000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/55000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/55000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/56000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/56000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/58000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/58000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/60000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/60000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/61000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/61000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/62000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/62000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/63000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/63000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/64000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/64000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/65000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/65000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/66000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/66000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/67000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/67000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/68000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/68000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/69000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/69000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/70000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/70000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/71000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/71000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/72000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/72000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/73000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/73000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/74000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/74000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/75000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/75000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/76000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/76000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/77000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/77000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/78000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/78000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/79000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/79000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/80000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/80000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/81000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/81000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/82000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/82000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/83000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/83000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/84000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/84000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/85000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/85000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/86000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/86000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/87000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/87000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/88000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/88000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/89000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/89000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/90000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/90000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/91000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/91000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/92000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/92000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/93000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/93000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/94000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/94000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/95000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/95000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/96000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/96000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/97000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/97000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/98000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/98000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/99000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/99000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/100000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/100000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/101000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/101000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/102000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/102000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/104000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/104000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/105000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/105000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/106000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/106000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/108000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/108000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/110000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/110000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/112000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/112000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/114000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/114000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/115000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/115000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/116000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/116000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/118000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/118000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/120000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/120000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/122000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/122000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/125000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/125000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/130000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/130000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/135000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/135000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/140000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/140000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/145000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/145000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/150000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/150000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/160000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/160000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/180000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/180000/P"));
    outrights.add(new OptionsContract("BTC/USD/250131/200000/C"));
    outrights.add(new OptionsContract("BTC/USD/250131/200000/P"));
 int loopCounter=0;

      StreamingExchange deribitExchange=null;
      while (loopCounter< outrights.size()) {
          if(deribitExchange==null) {
              deribitExchange = StreamingExchangeFactory.INSTANCE.createExchange(DeribitStreamingExchange.class);
              deribitExchange.connect().blockingAwait();

          }
     for (int i = 0; i < 10; ++i) {
         loopCounter++;

         disposables.add(deribitExchange
                 .getStreamingMarketDataService()
                 .getTicker(outrights.get(i))
                 .subscribe(System.out::println));
     }
     if(loopCounter<outrights.size()){
         deribitExchange=null;
     }
 }



    TimeUnit.SECONDS.sleep(300);
    for (Disposable disposable : disposables){
      disposable.dispose();
    }


  }
    @Test
    public void testMassTickerSubscription() throws InterruptedException {


        List<Disposable> disposables=new ArrayList<Disposable>();
        List<OptionsContract> outrights=new ArrayList<OptionsContract>();
        outrights.add(new OptionsContract("BTC/USD/250131/25000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/25000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/30000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/30000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/35000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/35000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/40000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/40000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/45000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/45000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/50000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/50000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/52000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/52000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/54000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/54000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/55000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/55000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/56000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/56000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/58000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/58000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/60000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/60000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/61000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/61000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/62000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/62000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/63000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/63000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/64000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/64000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/65000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/65000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/66000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/66000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/67000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/67000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/68000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/68000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/69000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/69000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/70000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/70000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/71000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/71000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/72000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/72000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/73000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/73000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/74000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/74000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/75000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/75000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/76000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/76000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/77000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/77000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/78000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/78000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/79000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/79000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/80000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/80000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/81000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/81000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/82000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/82000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/83000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/83000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/84000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/84000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/85000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/85000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/86000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/86000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/87000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/87000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/88000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/88000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/89000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/89000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/90000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/90000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/91000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/91000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/92000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/92000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/93000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/93000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/94000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/94000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/95000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/95000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/96000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/96000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/97000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/97000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/98000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/98000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/99000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/99000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/100000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/100000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/101000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/101000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/102000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/102000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/104000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/104000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/105000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/105000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/106000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/106000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/108000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/108000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/110000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/110000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/112000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/112000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/114000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/114000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/115000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/115000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/116000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/116000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/118000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/118000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/120000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/120000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/122000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/122000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/125000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/125000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/130000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/130000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/135000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/135000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/140000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/140000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/145000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/145000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/150000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/150000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/160000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/160000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/180000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/180000/P"));
        outrights.add(new OptionsContract("BTC/USD/250131/200000/C"));
        outrights.add(new OptionsContract("BTC/USD/250131/200000/P"));


        ProductSubscription.ProductSubscriptionBuilder subscription = ProductSubscription.create();


        for(int i = 0; i < 100; ++i){
            subscription.addTicker(outrights.get(i));


        }
        exchange.connect(subscription.build()).blockingAwait();

        TimeUnit.SECONDS.sleep(300);
        for (Disposable disposable : disposables){
            disposable.dispose();
        }


    }

  @Test
  public void testFundingRateStream() throws InterruptedException {
    Disposable dis =
        exchange
            .getStreamingMarketDataService()
            .getFundingRate(instrument)
            .subscribe(System.out::println);
    TimeUnit.SECONDS.sleep(3);
    dis.dispose();
  }

  @Test
  public void testOrderBook() throws InterruptedException {
    //books50-l2-tbt
    String[] args = {"100ms"};
    Disposable dis =
        exchange
            .getStreamingMarketDataService()
            .getOrderBook(instrument,args)
            .subscribe(
                orderBook -> {
                  System.out.println(orderBook);
                  assertThat(orderBook.getBids().get(0).getLimitPrice())
                      .isLessThan(orderBook.getAsks().get(0).getLimitPrice());
                 // assertThat(orderBook.getBids().get(0).getInstrument()).isEqualTo(instrument);
                });

    TimeUnit.SECONDS.sleep(3000000);
    dis.dispose();

  }
}
