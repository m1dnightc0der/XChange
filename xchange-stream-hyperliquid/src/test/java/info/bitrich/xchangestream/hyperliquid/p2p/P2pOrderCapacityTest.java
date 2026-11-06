package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assert.*;

import io.reactivex.rxjava3.observers.TestObserver;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/** Counter/guard regression only: the synthetic boundary is not a real memory/capacity test. */
public class P2pOrderCapacityTest {
  private static final int LIMIT = 2_000_000;

  @Test(timeout = 15000)
  public void aggregateLimitAllowsResizeAndRemovalButOverflowRequiresColdSeed() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    P2pStreamingServiceTest.Harness service = new P2pStreamingServiceTest.Harness((coins, min) -> {
      fetches.incrementAndGet();
      return SyntheticWire.seed(100, SyntheticWire.order(1, true, "BTC", "10", "1"));
    }, 32);
    TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
    TestObserver<P2pStreamingService.Observation> observations = service.observations().test();
    try {
      service.start();
      service.negotiate();
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      assertAccounting(service, 1, 1);
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      books.assertValueCount(1);

      // First prove real snapshot and runtime accounting, without a synthetic counter.
      service.frame(SyntheticWire.diff(1, 101, SyntheticWire.order(2, false, "BTC", "20", "1")));
      assertAccounting(service, 2, 2);
      books.assertValueCount(2);
      observations.assertValueCount(1);

      // Only the aggregate counter is synthetic. Perform the injection on its sole writer;
      // every following mutation still traverses EmbeddedChannel -> codec -> service worker.
      onWorker(service, () -> {
        field("totalOrders").setInt(service, LIMIT - 1);
        return null;
      });
      service.frame(SyntheticWire.diff(2, 101, SyntheticWire.order(3, true, "BTC", "9", "1")));
      assertAccounting(service, LIMIT, 3); // Exactly at the limit is legal.
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      books.assertValueCount(3);

      service.frame(SyntheticWire.diff(3, 101, SyntheticWire.order(3, true, "BTC", "9", "4")));
      assertAccounting(service, LIMIT, 3); // Existing oid resize must not consume capacity.
      books.assertValueCount(4);
      assertEquals(0, new BigDecimal("4").compareTo(books.values().get(3).bids.get(new BigDecimal("9"))));

      service.frame(SyntheticWire.diff(4, 101,
          SyntheticWire.change(BigInteger.valueOf(3), true, false, "BTC", "9", "0")));
      assertAccounting(service, LIMIT - 1, 2);
      books.assertValueCount(5);
      assertFalse(books.values().get(4).bids.containsKey(new BigDecimal("9")));

      service.frame(SyntheticWire.diff(5, 101, SyntheticWire.order(4, true, "BTC", "8", "1")));
      assertAccounting(service, LIMIT, 3); // Removal makes exactly one slot available again.
      books.assertValueCount(6);
      observations.assertValueCount(5);
      P2pStreamingService.Cursor cursor = service.getCursor();
      assertEquals(BigInteger.valueOf(101), cursor.getHeight());
      assertEquals(5, cursor.getFramesAtHeight());
      long generation = service.getGeneration();

      service.frame(SyntheticWire.diff(6, 102, SyntheticWire.order(5, false, "BTC", "21", "1")));
      onWorker(service, () -> null); // Drain the apply task before asserting absence of output.
      assertEquals(P2pStreamingService.Readiness.CATCHING_UP, service.getReadiness());
      assertTrue(service.getGeneration() > generation);
      assertNotNull(service.getLastFailure());
      assertEquals("Runtime order limit", service.getLastFailure().getMessage());
      assertSame("Rejected frame must not advance the cursor", cursor, service.getCursor());
      books.assertValueCount(6).assertNoErrors();
      observations.assertValueCount(5).assertNoErrors();
      TestObserver<P2pOrderBook.View> late = service.book("BTC").test();
      try {
        late.assertNoValues(); // The retained BehaviorSubject value is invalidated too.
      } finally {
        late.dispose();
      }

      // A fresh snapshot, not retained warm state, proves the overflow forced cold recovery.
      assertEquals(1, fetches.get());
      service.start();
      assertEquals(2, fetches.get());
      assertAccounting(service, 1, 1);
      assertEquals(BigInteger.valueOf(101), service.getCursor().getHeight());
      assertEquals(0, service.getCursor().getFramesAtHeight());
    } finally {
      books.dispose();
      observations.dispose();
      service.stop();
    }
  }

  private static void assertAccounting(P2pStreamingService service, int aggregate, int actual)
      throws Exception {
    onWorker(service, () -> {
      assertEquals("Service-wide retained-order counter", aggregate, field("totalOrders").getInt(service));
      Map<?, ?> books = (Map<?, ?>) field("booksByCoin").get(service);
      int retained = 0;
      for (Object book : books.values()) retained += ((P2pOrderBook) book).getOrderCount();
      assertEquals("Actual tiny fixture allocation (not the injected aggregate)", actual, retained);
      return null;
    });
  }

  private static <T> T onWorker(P2pStreamingService service, Callable<T> action) throws Exception {
    ExecutorService worker = (ExecutorService) field("work").get(service);
    return worker.submit(action).get(3, TimeUnit.SECONDS);
  }

  private static Field field(String name) throws Exception {
    Field field = P2pStreamingService.class.getDeclaredField(name);
    field.setAccessible(true);
    return field;
  }
}
