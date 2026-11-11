package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.ContinuationWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.util.ReferenceCountUtil;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.observers.TestObserver;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.Test;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;

/** Real provider handler + EmbeddedChannel ownership; no network or inherited global Rx hooks. */
public class P2pStreamingServiceTest {
  private static final String INFO = "{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"book\":\"on\"}}";
  private static final ThreadLocal<List<Harness>> ACTIVE_HARNESSES =
      ThreadLocal.withInitial(ArrayList::new);

  @Test(timeout = 10000) public void sameBlockDiffsPublishExchangeTimeAndKeepDistinctCursors() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100,
        SyntheticWire.order(1, true, "BTC", "99", "1"),
        SyntheticWire.order(2, false, "BTC", "101", "1")), 32);
    FuturesContract instrument = new FuturesContract("BTC/USDC/PERP");
    List<P2pStreamingService.Cursor> cursors = new CopyOnWriteArrayList<>();
    List<Ticker> tickers = new CopyOnWriteArrayList<>();
    long blockTimeMicros = 1_700_000_000_123_456L;
    byte[] firstBytes = diffAtBlockTime(1, 101, blockTimeMicros,
        SyntheticWire.order(1, true, "BTC", "99", "2"));
    byte[] secondBytes = diffAtBlockTime(2, 101, blockTimeMicros,
        SyntheticWire.order(1, true, "BTC", "99", "3"));
    P2pWireCodec.Diff firstDiff = (P2pWireCodec.Diff) new P2pWireCodec().decode(firstBytes);
    P2pWireCodec.Diff secondDiff = (P2pWireCodec.Diff) new P2pWireCodec().decode(secondBytes);
    try {
      TestObserver<OrderBook> books = service.book("BTC").skip(1).doOnNext(view -> {
        // Capture the validated cursor at publication, not after both frames have been applied.
        cursors.add(service.getCursor());
        tickers.add(view.toTicker(instrument));
      }).map(view -> view.toOrderBook(instrument)).test();
      TestObserver<P2pStreamingService.Observation> observations = service.observations().test();
      service.start(); service.negotiate();
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      awaitWorker(service);
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      books.assertNoValues(); // Skip only the initial snapshot, which has no block time.
      service.frame(firstBytes);
      awaitWorker(service);
      service.frame(secondBytes);
      awaitWorker(service);
      books.assertValueCount(2).assertNoErrors();
      observations.assertValueCount(2).assertNoErrors();
      assertEquals(firstDiff.height, secondDiff.height);
      assertEquals(firstDiff.blockTimeMicros, secondDiff.blockTimeMicros);
      assertNotEquals(firstDiff.sequence, secondDiff.sequence);
      assertCursor(cursors.get(0), 101, 1);
      assertCursor(cursors.get(1), 101, 2);
      assertEquals(new BigDecimal("2"), books.values().get(0).getBids().get(0).getOriginalAmount());
      assertEquals(new BigDecimal("3"), books.values().get(1).getBids().get(0).getOriginalAmount());
      P2pWireCodec.Diff[] diffs = {firstDiff, secondDiff};
      for (int i = 0; i < diffs.length; i++) {
        P2pStreamingService.Observation observation = observations.values().get(i);
        assertEquals(diffs[i].sequence, observation.getSequence());
        assertEquals(diffs[i].height, observation.getHeight());
        assertEquals(diffs[i].blockTimeMicros, observation.getBlockTimeMicros());
        long expectedMillis = diffs[i].blockTimeMicros.divide(BigInteger.valueOf(1000)).longValueExact();
        assertNotEquals(expectedMillis, observation.getIngressEpochMicros() / 1000);
        assertBookTimestamps(books.values().get(i), tickers.get(i), new Date(expectedMillis));
        assertBookProvenance(books.values().get(i), diffs[i].blockTimeMicros, 101, i + 1);
      }
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void snapshotOnlyPublicationHasNoExchangeTimestamp() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100,
        SyntheticWire.order(1, true, "BTC", "99", "1"),
        SyntheticWire.order(2, false, "BTC", "101", "1")), 32);
    FuturesContract instrument = new FuturesContract("BTC/USDC/PERP");
    try {
      TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
      service.start(); service.negotiate();
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      awaitWorker(service);
      books.assertValueCount(1).assertNoErrors();
      assertCursor(service.getCursor(), 101, 0);
      P2pOrderBook.View view = books.values().get(0);
      assertNull(view.createdAtMicros);
      // Neither the HTTP snapshot height nor a live status supplies block_time_us.
      assertBookTimestamps(view.toOrderBook(instrument), view.toTicker(instrument), null);
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void catchUpAndWarmReplayRetainLastAppliedBlockTime() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = new Harness((coins, min) -> {
      fetches.incrementAndGet();
      return SyntheticWire.seed(100, SyntheticWire.order(1, true, "BTC", "99", "1"),
          SyntheticWire.order(2, false, "BTC", "101", "1"));
    }, 32);
    FuturesContract instrument = new FuturesContract("BTC/USDC/PERP");
    long firstBlockTimeMicros = 1_700_000_000_123_456L;
    byte[] covered = diffAtBlockTime(1, 101, firstBlockTimeMicros,
        SyntheticWire.order(1, true, "BTC", "99", "2"));
    try {
      TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
      service.start(); service.negotiate();
      service.frame(covered); // Applied during catch-up, before there can be a publication.
      service.frame(SyntheticWire.status(0, 1, 101, 0));
      awaitWorker(service);
      books.assertValueCount(1).assertNoErrors();
      P2pOrderBook.View caughtUp = books.values().get(0);
      Date firstTime = new Date(firstBlockTimeMicros / 1000);
      assertBookTimestamps(caughtUp.toOrderBook(instrument), caughtUp.toTicker(instrument), firstTime);
      assertEquals(BigInteger.valueOf(firstBlockTimeMicros), caughtUp.createdAtMicros);
      assertBookProvenance(caughtUp.toOrderBook(instrument), caughtUp.createdAtMicros, 101, 1);
      long generation = service.getGeneration();
      service.channel.close(); service.channel.runPendingTasks();
      assertNotEquals(generation, service.getGeneration());
      service.book("BTC").test().assertNoValues();
      service.start(); service.negotiate();
      service.frame(covered); // Verified same-height replay must not become a new book timestamp.
      service.frame(SyntheticWire.status(0, 1, 103, 0));
      awaitWorker(service);
      books.assertValueCount(2).assertNoErrors();
      assertEquals(1, fetches.get());
      assertCursor(service.getCursor(), 103, 0); // Status advances recovery, not coin provenance.
      P2pOrderBook.View resumed = books.values().get(1);
      assertEquals(new BigDecimal("2"), resumed.bids.firstEntry().getValue());
      assertBookTimestamps(resumed.toOrderBook(instrument), resumed.toTicker(instrument), firstTime);
      assertBookProvenance(resumed.toOrderBook(instrument), caughtUp.createdAtMicros, 101, 1);
      long nextBlockTimeMicros = firstBlockTimeMicros + 2_000_000L;
      service.frame(diffAtBlockTime(2, 103, nextBlockTimeMicros,
          SyntheticWire.order(1, true, "BTC", "99", "3")));
      awaitWorker(service);
      books.assertValueCount(3).assertNoErrors();
      assertCursor(service.getCursor(), 103, 1);
      P2pOrderBook.View updated = books.values().get(2);
      assertBookTimestamps(updated.toOrderBook(instrument), updated.toTicker(instrument),
          new Date(nextBlockTimeMicros / 1000));
      assertBookProvenance(updated.toOrderBook(instrument), BigInteger.valueOf(nextBlockTimeMicros), 103, 1);
      assertBookTimestamps(caughtUp.toOrderBook(instrument), caughtUp.toTicker(instrument), firstTime);
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void statusAndOtherCoinProgressPreservePerCoinProvenance() throws Exception {
    Map<String, List<P2pWireCodec.Change>> seed = new LinkedHashMap<>();
    seed.put("BTC", Collections.singletonList(SyntheticWire.order(1, true, "BTC", "99", "1")));
    seed.put("ETH", Collections.singletonList(SyntheticWire.order(2, true, "ETH", "9", "1")));
    Harness service = new Harness(new LinkedHashSet<>(Arrays.asList("BTC", "ETH")),
        Collections.emptySet(), (coins, min) -> new P2pWireCodec.Snapshot(BigInteger.valueOf(100), seed), 32);
    FuturesContract btc = new FuturesContract("BTC/USDC/PERP");
    FuturesContract eth = new FuturesContract("ETH/USDC/PERP");
    long btcMicros = 1_700_000_000_123_456L, ethMicros = btcMicros + 2_000_000L;
    try {
      TestObserver<P2pOrderBook.View> btcBooks = service.book("BTC").test();
      TestObserver<P2pOrderBook.View> ethBooks = service.book("ETH").test();
      service.start(); service.negotiate();
      service.frame(diffAtBlockTime(1, 101, btcMicros,
          SyntheticWire.order(1, true, "BTC", "99", "2")));
      service.frame(SyntheticWire.status(0, 1, 101, 0));
      awaitWorker(service);
      btcBooks.assertValueCount(1).assertNoErrors();
      ethBooks.assertValueCount(1).assertNoErrors();
      assertBookProvenance(btcBooks.values().get(0).toOrderBook(btc), BigInteger.valueOf(btcMicros), 101, 1);
      P2pOrderBook.View untouched = ethBooks.values().get(0);
      assertNull(untouched.createdAtMicros);
      assertBookTimestamps(untouched.toOrderBook(eth), untouched.toTicker(eth), null);
      service.frame(diffAtBlockTime(2, 102, ethMicros,
          SyntheticWire.order(2, true, "ETH", "9", "2")));
      awaitWorker(service);
      btcBooks.assertValueCount(1); // Another coin's diff does not create a new BTC update.
      ethBooks.assertValueCount(2);
      service.frame(SyntheticWire.status(0, 1, 103, 0));
      awaitWorker(service);
      assertCursor(service.getCursor(), 103, 0);
      btcBooks.assertValueCount(2).assertNoErrors();
      ethBooks.assertValueCount(3).assertNoErrors();
      assertBookProvenance(btcBooks.values().get(1).toOrderBook(btc), BigInteger.valueOf(btcMicros), 101, 1);
      assertBookProvenance(ethBooks.values().get(2).toOrderBook(eth), BigInteger.valueOf(ethMicros), 102, 1);
      assertNull(untouched.createdAtMicros);
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void coldSnapshotClearsPreviousBlockProvenance() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = new Harness((coins, min) -> {
      fetches.incrementAndGet();
      return SyntheticWire.seed(100, SyntheticWire.order(1, true, "BTC", "99", "1"));
    }, 32);
    FuturesContract instrument = new FuturesContract("BTC/USDC/PERP");
    long micros = 1_700_000_000_123_456L;
    try {
      TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
      service.start(); service.negotiate();
      service.frame(diffAtBlockTime(1, 101, micros, SyntheticWire.order(1, true, "BTC", "99", "2")));
      service.frame(SyntheticWire.status(0, 1, 101, 0));
      awaitWorker(service);
      books.assertValueCount(1).assertNoErrors();
      assertBookProvenance(books.values().get(0).toOrderBook(instrument), BigInteger.valueOf(micros), 101, 1);
      service.frame(SyntheticWire.status(0, 2, 102, 103));
      waitFor(() -> !service.channel.isOpen());
      service.start(); service.negotiate();
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      awaitWorker(service);
      books.assertValueCount(2).assertNoErrors();
      assertEquals(2, fetches.get());
      assertCursor(service.getCursor(), 101, 0);
      P2pOrderBook.View replacement = books.values().get(1);
      assertNull(replacement.createdAtMicros);
      assertBookTimestamps(replacement.toOrderBook(instrument), replacement.toTicker(instrument), null);
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void negotiationSeedStatusAndActualIngressDiagnosticsGateReadiness() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = new Harness((coins, min) -> { fetches.incrementAndGet(); return SyntheticWire.seed(100); }, 32);
    try {
      TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
      TestObserver<P2pStreamingService.Observation> observations = service.observations().test();
      service.start();
      assertEquals(Collections.singletonList("{\"method\":\"info\"}"), service.sent);
      assertEquals(P2pStreamingService.Readiness.CATCHING_UP, service.getReadiness());
      service.negotiate();
      assertEquals(3, service.sent.size()); // No invented esp/subscribe acknowledgement.
      assertTrue(service.sent.get(2).contains("\"from_height\":101"));
      service.frame(SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "10", "2")));
      waitFor(() -> service.getCursor().getFramesAtHeight() == 1);
      books.assertNoValues();
      service.frame(SyntheticWire.status(0, 1, 100, 0)); // L=H below H+1 is legal.
      waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.LIVE);
      waitFor(() -> books.values().size() == 1);
      service.frame(SyntheticWire.diff(2, 101, SyntheticWire.order(1, false, "BTC", "999", "3")));
      waitFor(() -> observations.values().size() == 1);
      P2pStreamingService.Observation observation = observations.values().get(0);
      assertEquals("BTC", observation.getCoin());
      assertEquals(service.getGeneration(), observation.getGeneration());
      assertEquals(BigInteger.valueOf(101), observation.getHeight());
      assertEquals(BigInteger.valueOf(2), observation.getSequence());
      assertEquals(BigInteger.valueOf(1_000_000), observation.getBlockTimeMicros());
      assertTrue(observation.getIngressEpochMicros() > 0);
      assertTrue(observation.getDecodedNanos() >= observation.getIngressNanos());
      assertTrue(observation.getAppliedNanos() >= observation.getDecodedNanos());
      assertTrue(observation.getReadyNanos() >= observation.getAppliedNanos());
      assertEquals(1, fetches.get());
      assertEquals(0, books.values().get(books.values().size() - 1).bids.firstEntry().getValue().compareTo(new BigDecimal("3")));
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void warmResumeSkipsExactlyNAtHeightStillChecksSequenceAndKeepsObservers() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = new Harness((coins, min) -> { fetches.incrementAndGet(); return SyntheticWire.seed(100); }, 32);
    try {
      TestObserver<P2pOrderBook.View> observer = service.book("BTC").test();
      service.start(); service.negotiate();
      service.frame(SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "10", "1")));
      service.frame(SyntheticWire.diff(2, 101, SyntheticWire.order(1, true, "BTC", "10", "2")));
      service.frame(SyntheticWire.status(0, 1, 101, 0));
      waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.LIVE);
      long old = service.getGeneration();
      service.channel.close(); service.channel.runPendingTasks();
      assertNotEquals(old, service.getGeneration());
      assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      service.book("BTC").test().assertNoValues(); // Retained stale view cannot replay.
      service.start(); service.negotiate();
      assertTrue(service.sent.get(service.sent.size() - 1).contains("\"from_height\":101"));
      service.binary(old, SyntheticWire.diff(3, 999, SyntheticWire.order(9, true, "BTC", "1", "9")), 1, System.nanoTime());
      service.frame(SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "10", "1")));
      service.frame(SyntheticWire.diff(2, 101, SyntheticWire.order(1, true, "BTC", "10", "2")));
      service.frame(SyntheticWire.status(0, 1, 101, 0));
      service.frame(SyntheticWire.diff(3, 101, SyntheticWire.order(1, true, "BTC", "10", "5")));
      waitFor(() -> service.getCursor().getFramesAtHeight() == 3);
      waitFor(() -> observer.values().get(observer.values().size() - 1).bids.firstEntry().getValue().compareTo(new BigDecimal("5")) == 0);
      assertEquals(1, fetches.get());
      assertEquals(BigInteger.valueOf(101), service.getCursor().getHeight());
      service.frame(SyntheticWire.diff(5, 102)); // Sequence gap => warm, no snapshot.
      waitFor(() -> service.getReadiness() != P2pStreamingService.Readiness.LIVE);
      service.start(); service.negotiate();
      assertEquals(1, fetches.get());
      service.frame(SyntheticWire.diff(2, 101)); // Replay frames also require seq=1.
      waitFor(() -> !service.channel.isOpen());
      assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void liveFirstSnapshotUsesB0ReadsWhileHttpWaitsAndDiscardsWholeSeedHeight() throws Exception {
    CountDownLatch requested = new CountDownLatch(1), release = new CountDownLatch(1);
    List<BigInteger> minima = new CopyOnWriteArrayList<>();
    Harness service = new Harness((coins, min) -> {
      minima.add(min);
      if (min.signum() == 0) return SyntheticWire.seed(100);
      requested.countDown();
      assertTrue(release.await(3, TimeUnit.SECONDS));
      return SyntheticWire.seed(200, SyntheticWire.order(1, true, "BTC", "10", "4"));
    }, 32);
    try {
      service.start(); service.negotiate();
      service.text("{\"channel\":\"error\",\"data\":\"history_unavailable\"}");
      waitFor(() -> !service.channel.isOpen());
      service.start(); service.negotiate();
      assertFalse(service.sent.get(service.sent.size() - 1).contains("from_height"));
      service.frame(SyntheticWire.diff(1, 200, SyntheticWire.order(1, true, "BTC", "10", "999")));
      assertTrue(requested.await(3, TimeUnit.SECONDS));
      service.frame(SyntheticWire.diff(2, 201, SyntheticWire.order(1, true, "BTC", "10", "7")));
      service.frame(SyntheticWire.status(0, 1, 201, 0));
      service.frame(SyntheticWire.block(BigInteger.ZERO, BigInteger.valueOf(999),
          BigInteger.ZERO, BigInteger.ZERO, BigInteger.ZERO).bytes); // Metadata cannot complete books.
      assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
      books.assertNoValues();
      release.countDown();
      waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.LIVE);
      waitFor(() -> !books.values().isEmpty());
      assertEquals(Arrays.asList(BigInteger.ZERO, BigInteger.valueOf(200)), minima);
      assertEquals(BigInteger.valueOf(201), service.getCursor().getHeight());
      assertEquals(1, service.getCursor().getFramesAtHeight());
      assertEquals(0, books.values().get(0).bids.firstEntry().getValue().compareTo(new BigDecimal("7")));
      service.frame(SyntheticWire.diff(3, 201, SyntheticWire.order(1, true, "BTC", "10", "8")));
      waitFor(() -> service.getCursor().getFramesAtHeight() == 2);
    } finally { release.countDown(); service.stop(); }
  }

  @Test(timeout = 10000) public void providerGapColdReseedsAndOtherChannelsNeverAdvanceBookSequence() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = new Harness((coins, min) -> { fetches.incrementAndGet(); assertEquals(BigInteger.ZERO, min); return SyntheticWire.seed(100); }, 32);
    try {
      service.start(); service.negotiate();
      service.frame(new SyntheticWire.Bytes().u8(10).u64(0).bytes());
      service.frame(SyntheticWire.trades(BigInteger.ONE, BigInteger.ZERO, BigInteger.ZERO,
          BigInteger.valueOf(999), BigInteger.ZERO).bytes);
      service.frame(SyntheticWire.status(3, 1, 999, 0));
      assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      service.frame(SyntheticWire.diff(1, 105)); // Filtered heights can skip.
      service.frame(SyntheticWire.status(0, 1, 109, 0));
      waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.LIVE);
      assertEquals(BigInteger.valueOf(109), service.getCursor().getHeight());
      assertEquals(0, service.getCursor().getFramesAtHeight());
      service.frame(SyntheticWire.diff(2, 109)); // live(L) isn't block completion.
      waitFor(() -> service.getCursor().getFramesAtHeight() == 1);
      service.frame(SyntheticWire.status(0, 2, 110, 111));
      waitFor(() -> !service.channel.isOpen());
      service.start(); service.negotiate();
      assertEquals(2, fetches.get());
      assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void binaryContinuationCopiesBuffersAndPingDoesNotBecomeText() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
    try {
      service.start(); service.negotiate();
      byte[] bytes = SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "10", "1"));
      BinaryWebSocketFrame first = new BinaryWebSocketFrame(false, 0, Unpooled.wrappedBuffer(Arrays.copyOfRange(bytes, 0, 20)));
      ContinuationWebSocketFrame last = new ContinuationWebSocketFrame(true, 0, Unpooled.wrappedBuffer(Arrays.copyOfRange(bytes, 20, bytes.length)));
      service.channel.writeInbound(first);
      service.channel.writeInbound(new PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[] {1})));
      Object pong = service.channel.readOutbound();
      assertTrue(pong instanceof PongWebSocketFrame);
      ReferenceCountUtil.release(pong);
      service.channel.writeInbound(last);
      service.frame(SyntheticWire.status(0, 1, 101, 0));
      waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.LIVE);
      assertEquals(0, first.refCnt()); assertEquals(0, last.refCnt());
      assertEquals(1, service.getCursor().getFramesAtHeight());
      service.channel.writeInbound(new ContinuationWebSocketFrame(true, 0, Unpooled.wrappedBuffer(new byte[] {6})));
      waitFor(() -> !service.channel.isOpen());
      assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void capacityOneBurstHasOnlyOneWorkerHandoffAndDrainsWithoutOverflow() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 1);
    List<BinaryWebSocketFrame> frames = new ArrayList<>();
    try {
      service.start(); service.negotiate();
      long token = service.getGeneration();
      try (WorkerBlock blocked = new WorkerBlock(service)) {
        for (int i = 1; i <= 400; i++) {
          BinaryWebSocketFrame frame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(SyntheticWire.diff(i, 101)));
          frames.add(frame);
          service.channel.writeInbound(frame);
        }
        assertEquals(token, service.getGeneration());
        assertFalse(service.channel.config().isAutoRead());
        assertEquals(1, ((ThreadPoolExecutor) worker(service)).getQueue().size());
        assertEquals(400, ingressCount(service, "queuedFrames"));
        assertEquals(0, frames.get(0).refCnt()); // Copied, but its reservation still belongs to the worker.
        assertEquals(1, frames.get(399).refCnt()); // Raw frame remains in FlowControlHandler.
        assertTrue(field(handler(service), "handoff").getBoolean(handler(service)));
      }
      awaitWorker(service);
      assertCursor(service.getCursor(), 101, 400);
      assertEquals(token, service.getGeneration());
      assertReleased(service, frames);
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void frame4097InvalidatesAtTheWholeIngressBoundAndReleasesQueuedFrames() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
    List<BinaryWebSocketFrame> frames = new ArrayList<>();
    try {
      service.start(); service.negotiate();
      long token = service.getGeneration();
      assertEquals(4096, field(service, "MAX_PENDING_FRAMES").getInt(service));
      try (WorkerBlock blocked = new WorkerBlock(service)) {
        for (int i = 0; i < 4096; i++) {
          BinaryWebSocketFrame frame = heartbeatFrame();
          frames.add(frame);
          service.channel.writeInbound(frame);
        }
        assertEquals(token, service.getGeneration());
        assertEquals(4096, ingressCount(service, "queuedFrames"));
        assertEquals(4096 * 9L, ingressCount(service, "queuedBytes"));
        BinaryWebSocketFrame rejected = heartbeatFrame();
        frames.add(rejected);
        service.channel.writeInbound(rejected);
        assertEquals(0, rejected.refCnt());
        assertNotEquals(token, service.getGeneration());
        assertTrue(service.getLastFailure().getMessage().contains("frame limit"));
        assertFalse(service.channel.isOpen());
        service.book("BTC").test().assertNoValues();
      }
      awaitWorker(service);
      assertReleased(service, frames);
      assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void byteLimitIncludesFragmentHandoffAndFlowQueueWithoutDoubleReservation() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
    List<io.netty.handler.codec.http.websocketx.WebSocketFrame> frames = new ArrayList<>();
    try {
      service.start(); service.negotiate();
      long limit = 32L * 1024 * 1024;
      assertEquals(limit, field(service, "MAX_PENDING_BYTES").getLong(service));
      long token = service.getGeneration();
      try (WorkerBlock blocked = new WorkerBlock(service)) {
        byte[] payload = SyntheticWire.diff(1, 101);
        BinaryWebSocketFrame first = new BinaryWebSocketFrame(false, 0,
            Unpooled.wrappedBuffer(Arrays.copyOfRange(payload, 0, 3)));
        ContinuationWebSocketFrame last = new ContinuationWebSocketFrame(true, 0,
            Unpooled.wrappedBuffer(Arrays.copyOfRange(payload, 3, payload.length)));
        frames.add(first); frames.add(last);
        service.channel.writeInbound(first);
        pumpChannels();
        assertEquals(3, ingressCount(service, "queuedBytes"));
        assertEquals(1, ingressCount(service, "queuedFrames"));
        service.channel.writeInbound(last);
        assertEquals(payload.length, ingressCount(service, "queuedBytes"));
        assertEquals(2, ingressCount(service, "queuedFrames"));
        long remaining = limit - payload.length;
        while (remaining > 0) {
          int size = (int) Math.min(1024 * 1024, remaining);
          BinaryWebSocketFrame frame = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[size]));
          frames.add(frame);
          service.channel.writeInbound(frame);
          remaining -= size;
        }
        assertEquals(token, service.getGeneration());
        assertEquals(limit, ingressCount(service, "queuedBytes"));
        BinaryWebSocketFrame rejected = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new byte[] {0}));
        frames.add(rejected);
        service.channel.writeInbound(rejected);
        assertEquals(0, rejected.refCnt());
        assertNotEquals(token, service.getGeneration());
        assertTrue(service.getLastFailure().getMessage().contains("byte limit"));
        assertFalse(service.channel.isOpen());
      }
      awaitWorker(service);
      assertReleased(service, frames);
      assertCursor(service.getCursor(), 101, 0);
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void queuedArrivalAgeIsNotRefreshedWhenWorkerDemandResumes() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
    List<BinaryWebSocketFrame> frames = new ArrayList<>();
    try {
      service.start(); service.negotiate();
      assertEquals(TimeUnit.SECONDS.toNanos(5), field(service, "MAX_QUEUE_AGE_NANOS").getLong(service));
      long token = service.getGeneration();
      try (WorkerBlock blocked = new WorkerBlock(service)) {
        BinaryWebSocketFrame first = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(blockHeight(101)));
        BinaryWebSocketFrame queued = new BinaryWebSocketFrame(Unpooled.wrappedBuffer(
            SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "10", "1"))));
        frames.add(first); frames.add(queued);
        service.channel.writeInbound(first);
        service.channel.writeInbound(queued);
        List<?> credits = new ArrayList<>((java.util.Collection<?>) field(handler(service), "credits").get(handler(service)));
        assertEquals(2, credits.size());
        // Alter only the queued envelope's arrival, not a limit or the worker's execution clock.
        field(credits.get(1), "nanos").setLong(credits.get(1), System.nanoTime() - TimeUnit.SECONDS.toNanos(6));
        assertEquals(1, queued.refCnt());
      }
      waitFor(() -> !service.channel.isOpen());
      awaitWorker(service);
      assertNotEquals(token, service.getGeneration());
      Throwable failure = service.getLastFailure();
      assertNotNull("Expected an original-arrival age rejection", failure);
      assertTrue("Expected an original-arrival age rejection, got: " + failure,
          "P2P ingress queue age limit".equals(failure.getMessage())
              || "P2P work queue age limit".equals(failure.getMessage()));
      assertCursor(service.getCursor(), 101, 0);
      assertReleased(service, frames);
      service.book("BTC").test().assertNoValues();
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void oldestHandoffExpiresBeforeBlockedObserverReturnsWithoutReadyResurrection() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    try {
      service.book("BTC").subscribe(view -> {
        entered.countDown();
        assertTrue(release.await(3, TimeUnit.SECONDS));
      });
      service.start(); service.negotiate();
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      assertTrue(entered.await(3, TimeUnit.SECONDS));
      long token = service.getGeneration();
      Object credit = ((java.util.Queue<?>) field(handler(service), "credits").get(handler(service))).peek();
      assertNotNull(credit);
      field(credit, "nanos").setLong(credit, System.nanoTime() - TimeUnit.SECONDS.toNanos(6));
      field(service, "executingSinceNanos").setLong(service, System.nanoTime());
      checkHealth(service); // The original admission age wins even though execution looks fresh.
      assertNotEquals(token, service.getGeneration());
      assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      service.book("BTC").test().assertNoValues();
      release.countDown();
      waitFor(() -> !service.channel.isOpen());
      awaitWorker(service);
      assertEquals(0, ingressCount(service, "queuedFrames"));
      assertEquals(0, ingressCount(service, "queuedBytes"));
      assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
    } finally { release.countDown(); service.stop(); }
  }

  @Test(timeout = 10000) public void fragmentPingPongAndControlProgressPreserveFirstArrivalAndCredits() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
    try {
      service.start(); service.negotiate();
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      awaitWorker(service);
      TestObserver<P2pStreamingService.Observation> observations = service.observations().test();
      byte[] payload = SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "10", "1"));
      BinaryWebSocketFrame first = new BinaryWebSocketFrame(false, 0,
          Unpooled.wrappedBuffer(Arrays.copyOfRange(payload, 0, 3)));
      ContinuationWebSocketFrame last = new ContinuationWebSocketFrame(true, 0,
          Unpooled.wrappedBuffer(Arrays.copyOfRange(payload, 3, payload.length)));
      service.channel.writeInbound(first);
      pumpChannels();
      Object credit = ((java.util.Queue<?>) field(handler(service), "credits").get(handler(service))).peek();
      assertNotNull(credit);
      long nanos = field(credit, "nanos").getLong(credit);
      long micros = field(credit, "micros").getLong(credit);
      PingWebSocketFrame ping = new PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[] {7}));
      PongWebSocketFrame incomingPong = new PongWebSocketFrame(Unpooled.wrappedBuffer(new byte[] {8}));
      service.channel.writeInbound(ping);
      pumpChannels();
      Object response = service.channel.readOutbound();
      assertTrue(response instanceof PongWebSocketFrame);
      assertEquals(7, ((PongWebSocketFrame) response).content().getByte(0));
      ReferenceCountUtil.release(response);
      service.channel.writeInbound(incomingPong);
      pumpChannels();
      assertEquals(1, ingressCount(service, "queuedFrames"));
      assertEquals(3, ingressCount(service, "queuedBytes"));
      assertEquals(0, ((ThreadPoolExecutor) worker(service)).getQueue().size());
      service.channel.writeInbound(last);
      awaitWorker(service);
      observations.assertValueCount(1);
      assertEquals(nanos, observations.values().get(0).getIngressNanos());
      assertEquals(micros, observations.values().get(0).getIngressEpochMicros());
      assertEquals(0, first.refCnt()); assertEquals(0, last.refCnt());
      assertEquals(0, ping.refCnt()); assertEquals(0, incomingPong.refCnt());
      assertEquals(0, ingressCount(service, "queuedFrames"));
      assertEquals(0, ingressCount(service, "queuedBytes"));
      long token = service.getGeneration();
      // A text control behind completed binary work must receive its own demand and invalidate.
      service.text("{\"channel\":\"error\",\"stream\":\"book\",\"data\":\"history_busy\"}");
      waitFor(() -> !service.channel.isOpen());
      awaitWorker(service);
      assertNotEquals(token, service.getGeneration());
      assertTrue(service.getLastFailure().getMessage().contains("history busy"));
      assertEquals(0, ingressCount(service, "queuedFrames"));
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void stopAndHandlerRemovalReleaseBothHandoffAndFlowOwnedFrames() throws Exception {
    for (String removal : Arrays.asList("stop", "p2p-flow", "p2p-ingress", "binary")) {
      for (boolean incompleteFragment : Arrays.asList(false, true)) {
        Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
        List<BinaryWebSocketFrame> frames = new ArrayList<>();
        try {
          service.start(); service.negotiate();
          try (WorkerBlock blocked = new WorkerBlock(service)) {
            int count = incompleteFragment ? 1 : 4;
            for (int i = 0; i < count; i++) {
              BinaryWebSocketFrame frame = incompleteFragment
                  ? new BinaryWebSocketFrame(false, 0, Unpooled.wrappedBuffer(new byte[] {6}))
                  : heartbeatFrame();
              frames.add(frame);
              service.channel.writeInbound(frame);
            }
            assertEquals(count, ingressCount(service, "queuedFrames"));
            assertEquals(incompleteFragment ? 1 : 36, ingressCount(service, "queuedBytes"));
            if ("stop".equals(removal)) service.disconnect().blockingAwait(3, TimeUnit.SECONDS);
            else if ("binary".equals(removal)) service.channel.pipeline().remove(handler(service));
            else service.channel.pipeline().remove(removal);
            pumpChannels();
            assertFalse(removal, service.channel.isOpen());
            for (BinaryWebSocketFrame frame : frames) assertEquals(removal, 0, frame.refCnt());
          }
          if (!"stop".equals(removal)) awaitWorker(service);
          pumpChannels();
          assertReleased(service, frames);
          assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
        } finally { service.stop(); }
      }
    }
  }

  @Test(timeout = 10000) public void workerRejectionReleasesTransferredFragmentCreditsAndDoesNotResumeReads() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 1);
    try {
      service.start(); service.negotiate();
      long token = service.getGeneration();
      try (WorkerBlock blocked = new WorkerBlock(service)) {
        worker(service).execute(() -> {}); // Real executor rejection, not a smaller ingress limit.
        byte[] payload = SyntheticWire.diff(1, 101);
        BinaryWebSocketFrame first = new BinaryWebSocketFrame(false, 0,
            Unpooled.wrappedBuffer(Arrays.copyOfRange(payload, 0, 3)));
        ContinuationWebSocketFrame last = new ContinuationWebSocketFrame(true, 0,
            Unpooled.wrappedBuffer(Arrays.copyOfRange(payload, 3, payload.length)));
        service.channel.writeInbound(first);
        pumpChannels();
        ReadProbe reads = service.channel.pipeline().get(ReadProbe.class);
        int before = reads.reads.get();
        service.channel.writeInbound(last);
        pumpChannels();
        assertNotEquals(token, service.getGeneration());
        assertTrue(service.getLastFailure().getMessage().contains("queue overflow"));
        assertFalse(service.channel.isOpen());
        assertEquals(before, reads.reads.get());
        assertEquals(0, first.refCnt()); assertEquals(0, last.refCnt());
        assertEquals(0, ingressCount(service, "queuedFrames"));
        assertEquals(0, ingressCount(service, "queuedBytes"));
      }
      awaitWorker(service);
      assertCursor(service.getCursor(), 101, 0);
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void staleHandoffCompletionCannotReadOrInvalidateReplacementChannel() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    try {
      service.book("BTC").subscribe(view -> {
        entered.countDown();
        assertTrue(release.await(3, TimeUnit.SECONDS));
      });
      service.start(); service.negotiate();
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      assertTrue(entered.await(3, TimeUnit.SECONDS));
      EmbeddedChannel old = service.channel;
      Object oldHandler = handler(service);
      ReadProbe oldReads = old.pipeline().get(ReadProbe.class);
      long token = service.getGeneration();
      old.close(); old.runPendingTasks();
      assertNotEquals(token, service.getGeneration());
      // Install only a replacement transport while the prior worker completion is still blocked.
      assertTrue(service.openConnection().blockingAwait(3, TimeUnit.SECONDS));
      pumpChannels();
      EmbeddedChannel replacement = service.channel;
      ReadProbe replacementReads = replacement.pipeline().get(ReadProbe.class);
      int oldReadCount = oldReads.reads.get(), replacementReadCount = replacementReads.reads.get();
      long replacementToken = service.getGeneration();
      Throwable replacementFailure = service.getLastFailure();
      assertTrue(replacement.isOpen());
      assertEquals(1, ingressCount(service, "queuedFrames"));
      release.countDown();
      awaitWorker(service);
      // A fresh successor must not be aged by stale execution/credit bookkeeping either.
      checkHealth(service);
      assertFalse(field(oldHandler, "handoff").getBoolean(oldHandler));
      assertEquals(oldReadCount, oldReads.reads.get());
      assertEquals(replacementReadCount, replacementReads.reads.get());
      assertEquals(replacementToken, service.getGeneration());
      assertSame(replacementFailure, service.getLastFailure());
      assertSame(replacement, service.channel);
      assertTrue(replacement.isOpen());
      assertEquals(0, ingressCount(service, "queuedFrames"));
      assertEquals(0, ingressCount(service, "queuedBytes"));
      service.book("BTC").test().assertNoValues();
    } finally { release.countDown(); service.stop(); }
  }

  @Test(timeout = 10000) public void successorWatchdogIgnoresOldClocksButEnforcesItsOwnSilenceAndNegotiationDeadlines() throws Exception {
    for (boolean expireSilence : Arrays.asList(true, false)) {
      Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
      try {
        assertEquals(P2pStreamingService.Readiness.DISCONNECTED, service.getReadiness());
        assertFalse(service.isTerminal());
        service.start();
        awaitWorker(service);
        EmbeddedChannel oldChannel = service.channel;
        Object oldHandler = handler(service);
        long oldToken = service.getGeneration();
        assertEquals(oldToken, field(service, "negotiationToken").getLong(service));
        oldChannel.close();
        pumpChannels();
        awaitWorker(service);
        long successorToken = service.getGeneration();
        assertNotEquals(oldToken, successorToken);
        long oldArrival = System.nanoTime() - TimeUnit.SECONDS.toNanos(31);
        long oldNegotiation = System.nanoTime() - TimeUnit.SECONDS.toNanos(11);
        field(oldHandler, "lastIngressNanos").setLong(oldHandler, oldArrival);
        field(service, "negotiationSinceNanos").setLong(service, oldNegotiation);
        assertFalse(field(service, "negotiated").getBoolean(service));
        int controlsBeforeSuccessor = service.sent.size();
        long beforeInstall = System.nanoTime();
        // Do not call connect(): its completion would refresh clocks and hide this window.
        assertTrue(service.openConnection().blockingAwait(3, TimeUnit.SECONDS));
        pumpChannels();
        Object successor = handler(service);
        assertNotSame(oldHandler, successor);
        assertNotSame(oldChannel, service.channel);
        assertSame(successor, field(service, "activeHandler").get(service));
        assertTrue(service.channel.isOpen());
        assertEquals(successorToken, service.getGeneration());
        long successorArrival = field(successor, "lastIngressNanos").getLong(successor);
        assertTrue("Accepted handler needs a fresh silence allowance", successorArrival >= beforeInstall);
        assertTrue(successorArrival <= System.nanoTime());
        assertEquals(oldArrival, field(oldHandler, "lastIngressNanos").getLong(oldHandler));
        assertEquals(oldNegotiation, field(service, "negotiationSinceNanos").getLong(service));
        assertEquals(oldToken, field(service, "negotiationToken").getLong(service));
        assertEquals(controlsBeforeSuccessor, service.sent.size());
        Throwable priorFailure = service.getLastFailure();
        checkHealth(service);
        assertEquals(successorToken, service.getGeneration());
        assertSame(priorFailure, service.getLastFailure());
        assertTrue(service.channel.isOpen());
        assertFalse(service.isTerminal());
        if (expireSilence) {
          field(successor, "lastIngressNanos").setLong(successor,
              System.nanoTime() - TimeUnit.SECONDS.toNanos(31));
        } else {
          // The same expired negotiation timestamp becomes actionable only for its owner.
          field(service, "negotiationToken").setLong(service, successorToken);
        }
        checkHealth(service);
        waitFor(() -> !service.channel.isOpen());
        assertNotEquals(successorToken, service.getGeneration());
        assertEquals("P2P ingress/queue/negotiation timeout", service.getLastFailure().getMessage());
        assertFalse("A recoverable watchdog expiry is not terminal", service.isTerminal());
      } finally {
        service.stop();
        assertTrue(service.isTerminal());
      }
    }
  }

  @Test(timeout = 10000) public void blockIngressKeepsUnchangedBooksLiveButIngressSilenceClosesTransport() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
    try {
      service.start(); service.negotiate();
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.LIVE);
      awaitWorker(service);
      Object owner = handler(service);
      java.lang.reflect.Field lastIngress = field(owner, "lastIngressNanos");
      java.lang.reflect.Method check = P2pStreamingService.class.getDeclaredMethod("checkHealth");
      check.setAccessible(true);
      lastIngress.setLong(owner, System.nanoTime() - TimeUnit.SECONDS.toNanos(31));
      service.frame(new SyntheticWire.Bytes().u8(0).u64(0).bytes());
      check.invoke(service);
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      lastIngress.setLong(owner, System.nanoTime() - TimeUnit.SECONDS.toNanos(31));
      check.invoke(service);
      assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      waitFor(() -> !service.channel.isOpen());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void malformedCapabilityAndPermissionErrorsFailWithoutRetryLoop() throws Exception {
    for (String message : Arrays.asList(
        "{\"channel\":\"info\",\"binary_version\":2,\"streams\":{\"book\":\"on\"}}",
        "{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"book\":\"off\"}}",
        "{\"channel\":\"error\",\"data\":\"permission_denied\"}")) {
      Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
      try {
        service.start(); service.text(message);
        waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.FAILED);
        assertTrue(service.isTerminal());
        service.connect().test().assertError(IllegalStateException.class);
        service.awaitLive().test().awaitDone(3, TimeUnit.SECONDS).assertError(java.io.IOException.class);
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void crossedSnapshotFailsBeforeTransportOrPublication() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100,
        SyntheticWire.order(1, true, "BTC", "101", "1"),
        SyntheticWire.order(2, false, "BTC", "100", "1")), 32);
    try {
      TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
      service.connect().test().awaitDone(3, TimeUnit.SECONDS).assertError(IllegalArgumentException.class);
      waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.FAILED);
      books.assertNoValues();
      assertTrue(service.channelsCreated.isEmpty());
      service.awaitLive().test().awaitDone(3, TimeUnit.SECONDS).assertError(IllegalArgumentException.class);
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void crossedCompleteDiffInvalidatesBeforePublicationAndColdReseeds() throws Exception {
    for (boolean live : Arrays.asList(false, true)) {
      AtomicInteger fetches = new AtomicInteger();
      Harness service = new Harness((coins, min) -> {
        fetches.incrementAndGet();
        return SyntheticWire.seed(100, SyntheticWire.order(1, true, "BTC", "99", "1"),
            SyntheticWire.order(2, false, "BTC", "100", "1"));
      }, 32);
      try {
        TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
        TestObserver<P2pStreamingService.Observation> observations = service.observations().test();
        service.start(); service.negotiate();
        if (live) {
          service.frame(SyntheticWire.status(0, 1, 100, 0));
          waitFor(() -> books.values().size() == 1);
        }
        long token = service.getGeneration();
        service.frame(SyntheticWire.diff(1, 101, SyntheticWire.order(3, true, "BTC", "101", "1")));
        waitFor(() -> !service.channel.isOpen());
        assertNotEquals(token, service.getGeneration());
        assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
        books.assertValueCount(live ? 1 : 0);
        observations.assertNoValues();
        assertEquals(BigInteger.valueOf(101), service.getCursor().getHeight());
        assertEquals(0, service.getCursor().getFramesAtHeight());
        service.book("BTC").test().assertNoValues();
        service.start(); service.negotiate();
        assertEquals(2, fetches.get());
        service.frame(SyntheticWire.status(0, 1, 100, 0));
        waitFor(() -> books.values().size() == (live ? 2 : 1));
        assertEquals(0, new BigDecimal("99").compareTo(books.values().get(books.values().size() - 1).bids.firstKey()));
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void intermediateCrossingResolvedInSameFramePublishesOnlyValidFinalBook() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100,
        SyntheticWire.order(1, true, "BTC", "99", "1"),
        SyntheticWire.order(2, false, "BTC", "100", "1")), 32);
    try {
      TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
      service.start(); service.negotiate();
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      waitFor(() -> books.values().size() == 1);
      long token = service.getGeneration();
      service.frame(SyntheticWire.diff(1, 101,
          SyntheticWire.order(3, true, "BTC", "101", "1"),
          SyntheticWire.change(BigInteger.valueOf(2), false, false, "BTC", "0", "0"),
          SyntheticWire.order(4, false, "BTC", "102", "1")));
      waitFor(() -> books.values().size() == 2);
      assertEquals(token, service.getGeneration());
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      assertEquals(1, service.getCursor().getFramesAtHeight());
      assertEquals(0, new BigDecimal("101").compareTo(books.values().get(1).bids.firstKey()));
      assertEquals(0, new BigDecimal("102").compareTo(books.values().get(1).asks.firstKey()));
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void abandonedFragmentExpiresDespitePingPongWithoutContinuation() throws Exception {
    for (boolean binary : Arrays.asList(true, false)) {
      Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
      try {
        service.start(); service.negotiate();
        service.frame(SyntheticWire.status(0, 1, 100, 0));
        waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.LIVE);
        awaitWorker(service);
        long token = service.getGeneration();
        if (binary) service.channel.writeInbound(new BinaryWebSocketFrame(false, 0, Unpooled.wrappedBuffer(new byte[] {6})));
        else service.channel.writeInbound(new TextWebSocketFrame(false, 0, "{"));
        P2pStreamingService.BinaryHandler handler = service.channel.pipeline().get(P2pStreamingService.BinaryHandler.class);
        java.lang.reflect.Field started = handler.getClass().getDeclaredField("fragmentStartedNanos");
        started.setAccessible(true);
        assertNotEquals(0, started.getLong(handler));
        java.lang.reflect.Method check = P2pStreamingService.class.getDeclaredMethod("checkHealth");
        check.setAccessible(true);
        check.invoke(service);
        assertEquals(token, service.getGeneration()); // A new fragment is not itself a failure.
        service.channel.writeInbound(new PingWebSocketFrame(Unpooled.wrappedBuffer(new byte[] {1})));
        pumpChannels();
        Object pong = service.channel.readOutbound();
        assertTrue(pong instanceof PongWebSocketFrame);
        ReferenceCountUtil.release(pong);
        service.channel.writeInbound(new PongWebSocketFrame(Unpooled.wrappedBuffer(new byte[] {2})));
        started.setLong(handler, System.nanoTime() - TimeUnit.SECONDS.toNanos(6));
        check.invoke(service); // No continuation arrives: fragment age, not ingress silence, expires.
        assertNotEquals(token, service.getGeneration());
        assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
        assertTrue(service.getLastFailure().getMessage().contains("fragment age"));
        waitFor(() -> !service.channel.isOpen());
        assertEquals(0, started.getLong(handler));
        service.book("BTC").test().assertNoValues();
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void completedFragmentClearsIndependentAgeDeadline() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
    try {
      service.start(); service.negotiate();
      byte[] bytes = SyntheticWire.status(0, 1, 100, 0);
      service.channel.writeInbound(new BinaryWebSocketFrame(false, 0, Unpooled.wrappedBuffer(Arrays.copyOfRange(bytes, 0, 3))));
      P2pStreamingService.BinaryHandler handler = service.channel.pipeline().get(P2pStreamingService.BinaryHandler.class);
      java.lang.reflect.Field started = handler.getClass().getDeclaredField("fragmentStartedNanos");
      started.setAccessible(true);
      assertNotEquals(0, started.getLong(handler));
      service.channel.writeInbound(new ContinuationWebSocketFrame(true, 0, Unpooled.wrappedBuffer(Arrays.copyOfRange(bytes, 3, bytes.length))));
      waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.LIVE);
      awaitWorker(service);
      assertEquals(0, started.getLong(handler));
      java.lang.reflect.Method check = P2pStreamingService.class.getDeclaredMethod("checkHealth");
      check.setAccessible(true);
      check.invoke(service);
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void laterSnapshotCoveredFramesAreSkippedButEverySequenceIsChecked() throws Exception {
    for (boolean gap : Arrays.asList(false, true)) {
      AtomicInteger fetches = new AtomicInteger();
      Harness service = new Harness((coins, min) -> {
        fetches.incrementAndGet();
        return min.signum() == 0 ? SyntheticWire.seed(100) : SyntheticWire.seed(205,
            SyntheticWire.order(1, true, "BTC", "10", "4"));
      }, 32);
      try {
        TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
        TestObserver<P2pStreamingService.Observation> observations = service.observations().test();
        service.start(); service.negotiate();
        service.text("{\"channel\":\"error\",\"data\":\"history_unavailable\"}");
        waitFor(() -> !service.channel.isOpen());
        service.start(); service.negotiate();
        service.frame(SyntheticWire.diff(1, 200, SyntheticWire.order(1, true, "BTC", "10", "999")));
        waitFor(() -> service.getCursor() != null && service.getCursor().getHeight().equals(BigInteger.valueOf(206)));
        service.frame(SyntheticWire.status(0, 1, 200, 0));
        waitFor(() -> books.values().size() == 1);
        long token = service.getGeneration();
        if (gap) {
          service.frame(SyntheticWire.diff(3, 205)); // Covered by H, but missing seq=2 is still fatal to this connection.
          waitFor(() -> !service.channel.isOpen());
          assertNotEquals(token, service.getGeneration());
          assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
          assertTrue(service.getLastFailure().getMessage().contains("sequence"));
          books.assertValueCount(1);
          observations.assertNoValues();
        } else {
          service.frame(SyntheticWire.diff(2, 201, SyntheticWire.order(1, true, "BTC", "10", "998")));
          service.frame(SyntheticWire.diff(3, 205, SyntheticWire.order(1, true, "BTC", "10", "997")));
          service.frame(SyntheticWire.diff(4, 206, SyntheticWire.order(1, true, "BTC", "10", "8")));
          waitFor(() -> observations.values().size() == 1);
          books.assertValueCount(2);
          assertEquals(token, service.getGeneration());
          assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
          assertEquals(BigInteger.valueOf(206), service.getCursor().getHeight());
          assertEquals(1, service.getCursor().getFramesAtHeight());
          assertEquals(BigInteger.valueOf(4), observations.values().get(0).getSequence());
          assertEquals(0, new BigDecimal("4").compareTo(books.values().get(0).bids.firstEntry().getValue()));
          assertEquals(0, new BigDecimal("8").compareTo(books.values().get(1).bids.firstEntry().getValue()));
          service.frame(SyntheticWire.diff(5, 206, SyntheticWire.order(1, true, "BTC", "10", "9")));
          waitFor(() -> service.getCursor().getFramesAtHeight() == 2);
          service.frame(SyntheticWire.diff(6, 205)); // The old coverage fence cannot mask a real regression.
          waitFor(() -> !service.channel.isOpen());
          assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
          assertTrue(service.getLastFailure().getMessage().contains("Regressing"));
        }
        assertEquals(2, fetches.get());
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void disjointChannelsUseExactFiltersAndBooksStayUsableWhileTradesCatchUp() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = new Harness(Collections.singleton("BTC"), Collections.singleton("ETH"), (coins, min) -> {
      fetches.incrementAndGet();
      assertEquals(Collections.singleton("BTC"), coins);
      return SyntheticWire.seed(100, SyntheticWire.order(1, true, "BTC", "99", "1"),
          SyntheticWire.order(2, false, "BTC", "100", "1"));
    }, 32);
    try {
      TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
      TestObserver<P2pStreamingService.Observation> observations = service.observations().test();
      service.start();
      TestObserver<Void> ready = service.awaitLive().test();
      service.negotiate("{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"book\":\"on\",\"trades\":\"on\"}}", 2);
      assertEquals(Collections.singleton("BTC"), service.getBookCoins());
      assertEquals(Collections.singleton("ETH"), service.getTradeCoins());
      assertSubscription(service.sent.get(2), "book", "BTC", 101);
      service.frame(SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "99", "2")));
      service.frame(SyntheticWire.status(0, 1, 101, 0));
      waitFor(() -> service.sent.size() == 4 && books.values().size() == 1);
      assertSubscription(service.sent.get(3), "trades", "ETH", 101);
      assertEquals(P2pStreamingService.Readiness.CATCHING_UP, service.getReadiness());
      ready.assertNotComplete().assertNoErrors();
      service.book("BTC").test().assertValueCount(1); // Book availability is not aggregate readiness.
      P2pOrderBook.View before = books.values().get(0);
      Ticker ticker = before.toTicker(CurrencyPair.BTC_USDT);
      service.frame(SyntheticWire.status(3, 0, 105, 0));
      service.frame(tradeBatch(1, 105)); // A processed trade frame is a worker barrier.
      waitFor(() -> service.getTradeCursor().getHeight().equals(BigInteger.valueOf(105))
          && service.getTradeCursor().getFramesAtHeight() == 1);
      assertEquals(BigInteger.valueOf(101), service.getCursor().getHeight());
      assertEquals(1, service.getCursor().getFramesAtHeight());
      books.assertValueCount(1);
      observations.assertNoValues();
      service.frame(SyntheticWire.status(3, 1, 105, 0));
      ready.awaitDone(3, TimeUnit.SECONDS).assertComplete().assertNoErrors();
      P2pOrderBook.View after = service.book("BTC").test().assertValueCount(1).values().get(0);
      assertEquals(before.createdAtMicros, after.createdAtMicros);
      assertEquals(before.receivedAtNanos, after.receivedAtNanos);
      assertEquals(ticker.getTimestamp(), after.toTicker(CurrencyPair.BTC_USDT).getTimestamp());
      assertEquals(ticker.getBid(), after.toTicker(CurrencyPair.BTC_USDT).getBid());
      assertEquals(ticker.getAsk(), after.toTicker(CurrencyPair.BTC_USDT).getAsk());
      books.assertValueCount(1);
      observations.assertNoValues();
      assertEquals(1, fetches.get());
      assertEquals(4, service.sent.size());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void tradeOnlyUsesFirstNegotiatedBlockCompleteHeightWithoutHttpOrBookCapability() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = new Harness(Collections.emptySet(), Collections.singleton("BTC"), (coins, min) -> {
      fetches.incrementAndGet();
      throw new AssertionError("Trade-only mode must not fetch an HTTP snapshot");
    }, 32);
    try {
      TestObserver<P2pStreamingService.TradePublication> trades = service.trades("BTC").test();
      service.start();
      service.frame(blockHeight(12)); // Pre-negotiation metadata cannot choose the boundary.
      service.negotiate("{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"trades\":\"on\"}}", 1);
      assertNull(service.getTradeCursor());
      assertTrue(service.getBookCoins().isEmpty());
      assertEquals(2, service.sent.size()); // info + esp, no unbounded live trade subscription.
      service.frame(blockHeight(200));
      waitFor(() -> service.sent.size() == 3);
      assertSubscription(service.sent.get(2), "trades", "BTC", 200);
      assertEquals(BigInteger.valueOf(200), service.getTradeCursor().getHeight());
      assertEquals(0, service.getTradeCursor().getFramesAtHeight());
      TestObserver<Void> ready = service.awaitLive().test();
      service.frame(blockHeight(900)); // Later metadata must not rebase the complete-height cursor.
      service.frame(tradeBatch(1, 200, tradeEvent("BTC", true)));
      waitFor(() -> trades.values().size() == 1);
      assertEquals("p2p:trades:200:1:0", trades.values().get(0).id);
      ready.assertNotComplete().assertNoErrors(); // Unseen replay events need not wait for LIVE.
      service.frame(SyntheticWire.status(3, 1, 200, 0));
      ready.awaitDone(3, TimeUnit.SECONDS).assertComplete().assertNoErrors();
      assertEquals(BigInteger.valueOf(200), service.getTradeCursor().getHeight());
      assertEquals(1, service.getTradeCursor().getFramesAtHeight());
      assertEquals(0, fetches.get());
      assertEquals(3, service.sent.size());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void equalPriceFillsHaveStableFrameIdsAndLateTradeSubscribersSeeOnlyFutureEvents() throws Exception {
    Harness service = new Harness(Collections.emptySet(), Collections.singleton("BTC"), (coins, min) -> {
      throw new AssertionError("Trade-only mode must not fetch an HTTP snapshot");
    }, 32);
    try {
      TestObserver<P2pStreamingService.TradePublication> first = service.trades("BTC").test();
      TestObserver<P2pStreamingService.TradePublication> second = service.trades("BTC").test();
      service.startTrades(200);
      service.frame(SyntheticWire.status(3, 1, 200, 0));
      waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.LIVE);
      service.frame(tradeBatch(1, 200, tradeEvent("BTC", true), tradeEvent("BTC", true), tradeEvent("BTC", false)));
      waitFor(() -> first.values().size() == 3 && second.values().size() == 3);
      for (int i = 0; i < 3; i++) {
        P2pStreamingService.TradePublication publication = first.values().get(i);
        assertEquals("p2p:trades:200:1:" + i, publication.id);
        assertEquals(publication.id, second.values().get(i).id);
        assertEquals("BTC", publication.event.coin);
        assertEquals(new BigInteger("123456789"), publication.event.eventTimeMicros);
        assertEquals(0, new BigDecimal("10.25").compareTo(publication.event.price));
        assertEquals(0, new BigDecimal("2.5").compareTo(publication.event.size));
        assertEquals(i < 2, publication.event.buy);
      }
      TestObserver<P2pStreamingService.TradePublication> late = service.trades("BTC").test();
      late.assertNoValues();
      second.dispose();
      service.frame(tradeBatch(2, 200, tradeEvent("BTC", true)));
      waitFor(() -> first.values().size() == 4 && late.values().size() == 1);
      assertEquals("p2p:trades:200:2:0", late.values().get(0).id);
      assertEquals(first.values().get(3).id, late.values().get(0).id);
      second.assertValueCount(3);
      service.frame(tradeBatch(3, 201, tradeEvent("BTC", true)));
      waitFor(() -> first.values().size() == 5 && late.values().size() == 2);
      assertEquals("p2p:trades:201:1:0", first.values().get(4).id); // Not connection sequence 3.
      assertEquals(3, service.sent.size()); // Observers never create extra wire subscriptions.
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void malformedLaterTradeOrForeignCoinRejectsWholeBatchBeforeAnyPublication() throws Exception {
    for (boolean foreignCoin : Arrays.asList(false, true)) {
      Harness service = new Harness(Collections.emptySet(), Collections.singleton("BTC"), (coins, min) -> {
        throw new AssertionError("Trade-only mode must not fetch an HTTP snapshot");
      }, 32);
      try {
        TestObserver<P2pStreamingService.TradePublication> trades = service.trades("BTC").test();
        service.startTrades(200);
        service.frame(SyntheticWire.status(3, 1, 200, 0));
        waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.LIVE);
        SyntheticWire.Encoded batch = SyntheticWire.trades(BigInteger.ONE, BigInteger.ZERO,
            BigInteger.ZERO, BigInteger.valueOf(200), BigInteger.ZERO,
            tradeEvent("BTC", true), tradeEvent(foreignCoin ? "ETH" : "BTC", false));
        if (!foreignCoin) batch.bytes[batch.offset("trade.1.buy")] = 2;
        long generation = service.getGeneration();
        service.frame(batch.bytes);
        waitFor(() -> !service.channel.isOpen());
        trades.assertNoValues();
        assertNotEquals(generation, service.getGeneration());
        assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
        assertNotNull(service.getLastFailure());
        assertEquals(BigInteger.valueOf(200), service.getTradeCursor().getHeight());
        assertEquals(0, service.getTradeCursor().getFramesAtHeight());
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void missingRequestedTradeOrBookCapabilityFailsExplicitlyBeforeSubscription() throws Exception {
    for (String info : Arrays.asList(
        "{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"book\":\"on\"}}",
        "{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"book\":\"on\",\"trades\":\"off\"}}",
        "{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"book\":\"off\",\"trades\":\"on\"}}",
        "{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"trades\":\"on\"}}")) {
      Harness service = new Harness(Collections.singleton("BTC"), Collections.singleton("ETH"),
          (coins, min) -> SyntheticWire.seed(100), 32);
      try {
        service.start();
        service.text(info);
        waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.FAILED);
        service.connect().test().assertError(IllegalStateException.class);
        service.awaitLive().test().awaitDone(3, TimeUnit.SECONDS).assertError(java.io.IOException.class);
        assertNotNull(service.getLastFailure());
        assertEquals(Collections.singletonList("{\"method\":\"info\"}"), service.sent);
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void independentSequencesWarmReplayAndFilteredHeightsPreserveTradeIdsWhileBooksRecover() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = combinedService(fetches);
    try {
      TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
      TestObserver<P2pStreamingService.TradePublication> trades = service.trades("ETH").test();
      startCombined(service);
      service.frame(SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "99", "2")));
      service.frame(SyntheticWire.diff(2, 101, SyntheticWire.order(1, true, "BTC", "99", "3")));
      service.frame(SyntheticWire.status(0, 1, 101, 0));
      awaitWorker(service);
      assertSubscription(service.sent.get(3), "trades", "ETH", 101);
      service.frame(tradeBatch(1, 101, tradeEvent("ETH", true)));
      service.frame(tradeBatch(2, 101, tradeEvent("ETH", false)));
      service.frame(SyntheticWire.status(3, 1, 101, 0));
      awaitWorker(service);
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      assertCursor(service.getCursor(), 101, 2);
      assertCursor(service.getTradeCursor(), 101, 2);
      assertTradeIds(trades, "p2p:trades:101:1:0", "p2p:trades:101:2:0");

      long oldGeneration = service.getGeneration();
      service.channel.close(); service.channel.runPendingTasks();
      assertNotEquals(oldGeneration, service.getGeneration());
      int before = service.sent.size();
      startCombined(service);
      assertSubscription(service.sent.get(before + 2), "book", "BTC", 101);
      service.frame(SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "99", "2")));
      service.frame(SyntheticWire.diff(2, 101, SyntheticWire.order(1, true, "BTC", "99", "3")));
      service.frame(SyntheticWire.status(0, 1, 101, 0));
      awaitWorker(service);
      assertSubscription(service.sent.get(before + 3), "trades", "ETH", 101);
      service.frame(SyntheticWire.status(0, 0, 101, 0)); // Books can fall behind after trade replay starts.
      service.frame(tradeBatch(1, 101, tradeEvent("ETH", true)));
      service.frame(tradeBatch(2, 101, tradeEvent("ETH", false)));
      awaitWorker(service);
      int bookPublications = books.values().size();
      assertTradeIds(trades, "p2p:trades:101:1:0", "p2p:trades:101:2:0");
      service.frame(tradeBatch(3, 101, tradeEvent("ETH", true), tradeEvent("ETH", true)));
      service.frame(tradeBatch(4, 108, tradeEvent("ETH", false))); // Filtered heights need not be consecutive.
      service.frame(tradeBatch(5, 108, tradeEvent("ETH", true)));
      service.frame(SyntheticWire.status(3, 1, 108, 0));
      awaitWorker(service);
      assertTradeIds(trades, "p2p:trades:101:1:0", "p2p:trades:101:2:0",
          "p2p:trades:101:3:0", "p2p:trades:101:3:1", "p2p:trades:108:1:0", "p2p:trades:108:2:0");
      assertCursor(service.getTradeCursor(), 108, 2);
      assertCursor(service.getCursor(), 101, 2);
      assertEquals(P2pStreamingService.Readiness.CATCHING_UP, service.getReadiness());
      books.assertValueCount(bookPublications);
      service.frame(SyntheticWire.diff(3, 109, SyntheticWire.order(1, true, "BTC", "99", "4")));
      service.frame(SyntheticWire.status(0, 1, 109, 0));
      awaitWorker(service);
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      assertCursor(service.getCursor(), 109, 1);
      assertCursor(service.getTradeCursor(), 108, 2);
      assertEquals(1, fetches.get());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void historyOwnersRemainUntilMatchingLiveAndAllSameHeightReplayFrames() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = combinedService(fetches);
    try {
      startCombined(service);
      service.frame(SyntheticWire.status(0, 0, 101, 0));
      service.frame(SyntheticWire.status(1, 1, 999, 0)); // Another stream cannot release BOOK.
      service.frame(SyntheticWire.diff(1, 101));
      service.frame(SyntheticWire.diff(2, 101));
      assertHistoryOwner(service, "BOOK");
      assertEquals(3, service.sent.size());
      service.frame(SyntheticWire.status(0, 1, 101, 0));
      assertHistoryOwner(service, "TRADES");
      assertEquals(4, service.sent.size());
      service.frame(SyntheticWire.status(3, 0, 101, 0));
      service.frame(tradeBatch(1, 101));
      service.frame(tradeBatch(2, 101));
      service.frame(SyntheticWire.status(0, 1, 101, 0)); // BOOK's LIVE cannot release TRADES.
      assertHistoryOwner(service, "TRADES");
      assertEquals(P2pStreamingService.Readiness.CATCHING_UP, service.getReadiness());
      service.frame(SyntheticWire.status(3, 1, 101, 0));
      assertHistoryOwner(service, null);
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());

      service.channel.close(); service.channel.runPendingTasks();
      int before = service.sent.size();
      startCombined(service);
      service.frame(SyntheticWire.diff(1, 101));
      assertHistoryOwner(service, "BOOK");
      assertEquals(before + 3, service.sent.size());
      service.frame(SyntheticWire.diff(2, 101));
      assertHistoryOwner(service, "BOOK"); // Finishing replay alone is not a LIVE acknowledgement.
      assertEquals(before + 3, service.sent.size());
      service.frame(SyntheticWire.status(0, 1, 101, 0));
      assertHistoryOwner(service, "TRADES");
      assertSubscription(service.sent.get(before + 3), "trades", "ETH", 101);
      service.frame(tradeBatch(1, 101));
      assertHistoryOwner(service, "TRADES");
      service.frame(tradeBatch(2, 101));
      assertHistoryOwner(service, "TRADES");
      service.frame(SyntheticWire.status(3, 1, 101, 0));
      assertHistoryOwner(service, null);
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      assertCursor(service.getCursor(), 101, 2);
      assertCursor(service.getTradeCursor(), 101, 2);
      assertEquals(before + 4, service.sent.size());
      assertEquals(1, fetches.get());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void liveBeforeCompleteSameHeightReplayCannotReleaseEitherHistoryOwner() throws Exception {
    for (boolean tradeOwner : Arrays.asList(false, true)) {
      AtomicInteger fetches = new AtomicInteger();
      Harness service = combinedService(fetches);
      try {
        startCombined(service);
        service.frame(SyntheticWire.diff(1, 101));
        service.frame(SyntheticWire.diff(2, 101));
        service.frame(SyntheticWire.status(0, 1, 101, 0));
        awaitWorker(service);
        service.frame(tradeBatch(1, 101));
        service.frame(tradeBatch(2, 101));
        service.frame(SyntheticWire.status(3, 1, 101, 0));
        awaitWorker(service);
        service.channel.close(); service.channel.runPendingTasks();
        int before = service.sent.size();
        startCombined(service);
        service.frame(SyntheticWire.diff(1, 101));
        if (tradeOwner) {
          service.frame(SyntheticWire.diff(2, 101));
          service.frame(SyntheticWire.status(0, 1, 101, 0));
          assertHistoryOwner(service, "TRADES");
          service.frame(tradeBatch(1, 101));
        }
        service.frame(SyntheticWire.status(tradeOwner ? 3 : 0, 1, 101, 0));
        waitFor(() -> !service.channel.isOpen());
        awaitWorker(service);
        assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
        assertEquals(before + (tradeOwner ? 4 : 3), service.sent.size());
        assertCursor(service.getCursor(), 101, 2);
        assertCursor(service.getTradeCursor(), 101, 2);
        assertNotNull(service.getLastFailure());
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void historyBusyUsesPendingOwnerAndWarmRetriesWithoutResettingEitherCursor() throws Exception {
    for (boolean tradeOwner : Arrays.asList(false, true)) {
      AtomicInteger fetches = new AtomicInteger();
      Harness service = combinedService(fetches);
      try {
        TestObserver<P2pStreamingService.TradePublication> trades = service.trades("ETH").test();
        startCombined(service);
        service.frame(SyntheticWire.diff(1, 101));
        if (tradeOwner) {
          service.frame(SyntheticWire.status(0, 1, 101, 0));
          awaitWorker(service);
          service.frame(tradeBatch(1, 101, tradeEvent("ETH", true)));
          service.frame(SyntheticWire.status(3, 0, 101, 0));
        } else {
          service.frame(SyntheticWire.status(0, 0, 101, 0));
        }
        assertHistoryOwner(service, tradeOwner ? "TRADES" : "BOOK");
        service.text("{\"channel\":\"error\",\"data\":\"history_busy\"}");
        waitFor(() -> !service.channel.isOpen());
        assertNotEquals(P2pStreamingService.Readiness.FAILED, service.getReadiness());
        assertCursor(service.getCursor(), 101, 1);
        assertCursor(service.getTradeCursor(), 101, tradeOwner ? 1 : 0);
        int before = service.sent.size();
        startCombined(service);
        assertSubscription(service.sent.get(before + 2), "book", "BTC", 101);
        service.frame(SyntheticWire.diff(1, 101));
        service.frame(SyntheticWire.status(0, 1, 101, 0));
        awaitWorker(service);
        assertSubscription(service.sent.get(before + 3), "trades", "ETH", 101);
        service.frame(tradeBatch(1, 101, tradeEvent("ETH", true)));
        service.frame(tradeBatch(2, 101, tradeEvent("ETH", false)));
        service.frame(SyntheticWire.status(3, 1, 101, 0));
        awaitWorker(service);
        assertTradeIds(trades, "p2p:trades:101:1:0", "p2p:trades:101:2:0");
        assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
        assertCursor(service.getCursor(), 101, 1);
        assertCursor(service.getTradeCursor(), 101, 2);
        assertEquals(1, fetches.get());
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void informationalServerLogsPreservePendingOwnersCursorsAndLiveState() throws Exception {
    for (int phase = 0; phase < 3; phase++) {
      AtomicInteger fetches = new AtomicInteger();
      Harness service = combinedService(fetches);
      try {
        startCombined(service);
        if (phase > 0) {
          service.frame(SyntheticWire.status(0, 1, 100, 0));
          awaitWorker(service);
        }
        if (phase == 2) {
          service.frame(tradeBatch(1, 101, tradeEvent("ETH", true)));
          service.frame(SyntheticWire.status(3, 1, 101, 0));
          awaitWorker(service);
        }
        String owner = phase == 0 ? "BOOK" : phase == 1 ? "TRADES" : null;
        assertHistoryOwner(service, owner);
        long generation = service.getGeneration();
        P2pStreamingService.Readiness readiness = service.getReadiness();
        P2pStreamingService.Cursor bookCursor = service.getCursor();
        P2pStreamingService.Cursor tradeCursor = service.getTradeCursor();
        int controls = service.sent.size();
        for (String log : Arrays.asList("server log", "history_error", "history_busy", "history_unavailable")) {
          service.text("{\"channel\":\"errors\",\"error\":\"" + log + "\",\"oldest_height\":250}");
          awaitWorker(service);
          assertEquals("Informational log changed readiness: " + log, readiness, service.getReadiness());
          assertEquals(generation, service.getGeneration());
          assertSame(bookCursor, service.getCursor());
          assertSame(tradeCursor, service.getTradeCursor());
          assertNull(service.getLastFailure());
          assertNull(service.getLastTradeContinuityWarning());
          assertEquals(controls, service.sent.size());
          assertHistoryOwner(service, owner);
          assertTrue(service.channel.isOpen());
        }
        assertEquals(1, fetches.get());
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void documentedHistoryErrorRecoversBeforeAndAfterSubscriptionOwnership() throws Exception {
    for (int phase = -1; phase < 3; phase++) {
      AtomicInteger fetches = new AtomicInteger();
      Harness service = combinedService(fetches);
      try {
        if (phase < 0) service.start();
        else startCombined(service);
        if (phase > 0) {
          service.frame(SyntheticWire.status(0, 1, 100, 0));
          awaitWorker(service);
        }
        if (phase == 2) {
          service.frame(SyntheticWire.status(3, 1, 101, 0));
          awaitWorker(service);
          assertHistoryOwner(service, null);
        }
        long generation = service.getGeneration();
        P2pStreamingService.Cursor bookCursor = service.getCursor();
        P2pStreamingService.Cursor tradeCursor = service.getTradeCursor();
        service.text("{\"channel\":\"errors\",\"code\":\"history_error\",\"message\":\"reader failed\"}");
        waitFor(() -> !service.channel.isOpen());
        assertFalse("history_error must permit inherited retry, phase=" + phase, service.isTerminal());
        assertEquals(generation + 1, service.getGeneration());
        assertSame(bookCursor, service.getCursor());
        assertSame(tradeCursor, service.getTradeCursor());
        assertTrue(service.getLastFailure().getMessage().contains("history_error"));
        assertTrue(service.getLastFailure().getMessage().contains("reader failed"));
        assertEquals(1, fetches.get());
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void historyErrorThenLateCloseCannotRevokeWarmSuccessor() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = combinedService(fetches);
    try {
      startCombined(service);
      P2pStreamingService.BinaryHandler oldHandler = handler(service);
      ChannelHandlerContext oldContext = service.channel.pipeline().context(oldHandler);
      long originalGeneration = service.getGeneration();
      service.text("{\"channel\":\"errors\",\"code\":\"history_error\",\"message\":\"reader failed\"}");
      waitFor(() -> !service.channel.isOpen());
      assertFalse(service.isTerminal());
      assertEquals(originalGeneration + 1, service.getGeneration());
      startCombined(service);
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      awaitWorker(service);
      service.frame(SyntheticWire.status(3, 1, 101, 0));
      awaitWorker(service);
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      long successorGeneration = service.getGeneration();
      Throwable cause = service.getLastFailure();
      oldHandler.channelRead(oldContext,
          new io.netty.handler.codec.http.websocketx.CloseWebSocketFrame(1011, "history_error"));
      oldHandler.channelInactive(oldContext);
      awaitWorker(service);
      assertEquals(successorGeneration, service.getGeneration());
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      assertSame(cause, service.getLastFailure());
      assertTrue(service.channel.isOpen());
      assertEquals(1, fetches.get());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void topLevelCodeOverridesHumanErrorAndMessageText() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = combinedService(fetches);
    try {
      startCombined(service);
      P2pStreamingService.Cursor before = service.getCursor();
      service.text("{\"channel\":\"errors\",\"code\":\"history_error\","
          + "\"error\":\"history_unavailable\",\"message\":\"version_mismatch in diagnostic text\"}");
      waitFor(() -> !service.channel.isOpen());
      assertFalse(service.isTerminal());
      assertSame(before, service.getCursor());
      assertTrue(service.getLastFailure().getMessage().contains("history_error"));
      startCombined(service);
      assertEquals("Code must choose warm resume, not the error text's live-first recovery", 1, fetches.get());
      assertSubscription(service.sent.get(service.sent.size() - 1), "book", "BTC", 101);
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void terminalCodedErrorsPreserveCodeMessageAndOriginalException() throws Exception {
    for (boolean pending : Arrays.asList(false, true)) {
      for (String code : Arrays.asList("version_mismatch", "invalid_json", "unknown_future_code")) {
        Harness service = combinedService(new AtomicInteger());
        try {
          if (pending) startCombined(service);
          else service.start();
          TestObserver<Void> waiting = service.awaitLive().test();
          service.text("{\"channel\":\"errors\",\"code\":\"" + code
              + "\",\"error\":\"history_busy\",\"message\":\"request rejected\"}");
          waitFor(service::isTerminal);
          Throwable original = service.getLastFailure();
          assertTrue(original instanceof java.io.IOException);
          assertTrue(original.getMessage().contains(code));
          assertTrue(original.getMessage().contains("request rejected"));
          waiting.awaitDone(3, TimeUnit.SECONDS).assertError(error -> error == original);
          assertEquals(P2pStreamingService.Readiness.FAILED, service.getReadiness());
        } finally { service.stop(); }
      }
    }
  }

  @Test(timeout = 10000) public void malformedPresentCodeIsNeverAnInformationalLog() throws Exception {
    for (String code : Arrays.asList("null", "true", "17", "{}", "[]", "\"\"")) {
      Harness service = combinedService(new AtomicInteger());
      try {
        startCombined(service);
        service.text("{\"channel\":\"errors\",\"code\":" + code + ",\"error\":\"server log\"}");
        waitFor(service::isTerminal);
        assertEquals(P2pStreamingService.Readiness.FAILED, service.getReadiness());
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void tradeRetentionWarnsInclusiveRangeAndResubscribesAtOldestPlusOneHundred() throws Exception {
    for (String error : Arrays.asList(
        "{\"channel\":\"error\",\"data\":\"history_unavailable\",\"oldest_height\":250}",
        "{\"channel\":\"error\",\"data\":{\"code\":\"history_unavailable\",\"oldest_height\":\"250\"}}")) {
      Harness service = tradeOnlyService();
      try {
        TestObserver<P2pStreamingService.TradePublication> trades = service.trades("BTC").test();
        service.startTrades(200);
        service.frame(tradeBatch(1, 200, tradeEvent("BTC", true)));
        assertHistoryOwner(service, "TRADES");
        service.text(error);
        waitFor(() -> !service.channel.isOpen());
        assertCursor(service.getTradeCursor(), 350, 0);
        assertEquals("P2P trades missing heights [200,349] (retention)", service.getLastTradeContinuityWarning());
        assertNotEquals(P2pStreamingService.Readiness.FAILED, service.getReadiness());
        restartTrades(service, 350);
        service.frame(tradeBatch(1, 350, tradeEvent("BTC", false)));
        service.frame(SyntheticWire.status(3, 1, 350, 0));
        awaitWorker(service);
        assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
        assertCursor(service.getTradeCursor(), 350, 1);
        assertTradeIds(trades, "p2p:trades:200:1:0", "p2p:trades:350:1:0");
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void unsafeTradeRetentionBoundariesFailClosedWithoutResettingCursor() throws Exception {
    for (String oldest : Arrays.asList("", ",\"oldest_height\":100", ",\"oldest_height\":99",
        ",\"oldest_height\":-1", ",\"oldest_height\":200.5", ",\"oldest_height\":\"unknown\"",
        ",\"oldest_height\":18446744073709551516", ",\"oldest_height\":18446744073709551616")) {
      Harness service = tradeOnlyService();
      try {
        TestObserver<P2pStreamingService.TradePublication> trades = service.trades("BTC").test();
        service.startTrades(200);
        service.text("{\"channel\":\"error\",\"data\":\"history_unavailable\"" + oldest + "}");
        waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.FAILED);
        assertCursor(service.getTradeCursor(), 200, 0);
        assertTrue(service.getLastFailure().getMessage().contains("Trade continuity failure"));
        assertNull(service.getLastTradeContinuityWarning());
        assertEquals(3, service.sent.size());
        trades.assertNoValues();
        service.connect().test().assertError(IllegalStateException.class);
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void unscopedHistoryErrorsWithoutPendingOwnerFailClosed() throws Exception {
    for (String code : Arrays.asList("history_busy", "history_unavailable")) {
      AtomicInteger fetches = new AtomicInteger();
      Harness service = combinedService(fetches);
      try {
        startCombined(service);
        service.frame(SyntheticWire.status(0, 1, 100, 0));
        awaitWorker(service);
        service.frame(SyntheticWire.status(3, 1, 101, 0));
        assertHistoryOwner(service, null);
        assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
        service.text("{\"channel\":\"error\",\"data\":\"" + code + "\",\"oldest_height\":250}");
        waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.FAILED);
        assertTrue(service.getLastFailure().getMessage().contains("Unattributable"));
        assertCursor(service.getCursor(), 101, 0);
        assertCursor(service.getTradeCursor(), 101, 0);
        assertEquals(1, fetches.get());
        assertEquals(4, service.sent.size());
        service.connect().test().assertError(IllegalStateException.class);
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void liveTradeGapNeedsNoSecondLiveStatus() throws Exception {
    assertTradeGapPreservesPriorLiveness(true);
  }

  @Test(timeout = 10000) public void replayTradeGapStillRequiresNormalLiveCompletion() throws Exception {
    assertTradeGapPreservesPriorLiveness(false);
  }

  private static void assertTradeGapPreservesPriorLiveness(boolean initiallyLive) throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = combinedService(fetches);
    try {
      TestObserver<P2pStreamingService.TradePublication> trades = service.trades("ETH").test();
      TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
      startCombined(service);
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      awaitWorker(service);
      service.frame(tradeBatch(1, 101, tradeEvent("ETH", true)));
      if (initiallyLive) service.frame(SyntheticWire.status(3, 1, 101, 0));
      awaitWorker(service);
      P2pStreamingService.Readiness before = service.getReadiness();
      assertEquals(initiallyLive ? P2pStreamingService.Readiness.LIVE
          : P2pStreamingService.Readiness.CATCHING_UP, before);
      long generation = service.getGeneration();
      TestObserver<Void> live = service.awaitLive().test();
      service.frame(SyntheticWire.status(3, 2, 102, 105));
      awaitWorker(service);
      assertEquals("Gap must preserve prior channel liveness", before, service.getReadiness());
      assertCursor(service.getTradeCursor(), 106, 0);
      assertEquals("P2P trades missing heights [102,105] (provider gap)", service.getLastTradeContinuityWarning());
      service.frame(tradeBatch(2, 106, tradeEvent("ETH", false)));
      awaitWorker(service);
      assertEquals(before, service.getReadiness());
      assertTradeIds(trades, "p2p:trades:101:1:0", "p2p:trades:106:1:0");
      assertCursor(service.getTradeCursor(), 106, 1);
      assertCursor(service.getCursor(), 101, 0);
      assertHistoryOwner(service, initiallyLive ? null : "TRADES");
      if (!initiallyLive) {
        live.assertNotComplete().assertNoErrors();
        service.frame(SyntheticWire.status(3, 1, 106, 0));
      }
      live.awaitDone(3, TimeUnit.SECONDS).assertComplete().assertNoErrors();
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      assertEquals(generation, service.getGeneration());
      assertEquals(1, fetches.get());
      assertEquals(4, service.sent.size());
      books.assertValueCount(1);
      assertTrue(service.channel.isOpen());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void tradeGapReportsInclusiveMissingRangeAndContinuesWithoutBookSnapshotOrRetimestamp() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = combinedService(fetches);
    try {
      TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
      TestObserver<P2pStreamingService.Observation> observations = service.observations().test();
      TestObserver<P2pStreamingService.TradePublication> trades = service.trades("ETH").test();
      startCombined(service);
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      awaitWorker(service);
      service.frame(tradeBatch(1, 101, tradeEvent("ETH", true)));
      service.frame(SyntheticWire.status(3, 1, 101, 0));
      awaitWorker(service);
      P2pOrderBook.View original = books.values().get(0);
      long generation = service.getGeneration();
      service.frame(SyntheticWire.status(3, 0, 102, 0));
      service.frame(SyntheticWire.status(3, 2, 102, 105));
      awaitWorker(service);
      assertEquals("P2P trades missing heights [102,105] (provider gap)", service.getLastTradeContinuityWarning());
      assertCursor(service.getTradeCursor(), 106, 0);
      assertCursor(service.getCursor(), 101, 0);
      assertEquals(P2pStreamingService.Readiness.CATCHING_UP, service.getReadiness());
      service.frame(tradeBatch(2, 106, tradeEvent("ETH", false)));
      service.frame(SyntheticWire.status(3, 1, 106, 0));
      awaitWorker(service);
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      assertEquals(generation, service.getGeneration());
      assertEquals(1, fetches.get());
      assertEquals(4, service.sent.size());
      assertCursor(service.getTradeCursor(), 106, 1);
      assertCursor(service.getCursor(), 101, 0);
      assertTradeIds(trades, "p2p:trades:101:1:0", "p2p:trades:106:1:0");
      books.assertValueCount(1);
      observations.assertNoValues();
      P2pOrderBook.View retained = service.book("BTC").test().assertValueCount(1).values().get(0);
      assertEquals(original.createdAtMicros, retained.createdAtMicros);
      assertEquals(original.receivedAtNanos, retained.receivedAtNanos);
      assertEquals(original.toTicker(CurrencyPair.BTC_USDT).getTimestamp(),
          retained.toTicker(CurrencyPair.BTC_USDT).getTimestamp());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void regressingOrOverflowingTradeGapBoundariesFailClosedWithoutCursorCommit() throws Exception {
    for (BigInteger toHeight : Arrays.asList(BigInteger.valueOf(199), new BigInteger("18446744073709551615"))) {
      Harness service = tradeOnlyService();
      try {
        TestObserver<P2pStreamingService.TradePublication> trades = service.trades("BTC").test();
        service.startTrades(200);
        service.frame(SyntheticWire.status(3, 1, 200, 0));
        awaitWorker(service);
        long fromHeight = toHeight.equals(BigInteger.valueOf(199)) ? 190 : 200;
        service.frame(new SyntheticWire.Bytes().u8(13).u8(3).u8(2)
            .u64(fromHeight).u64(toHeight).u64(1_000_002).bytes());
        waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.FAILED);
        assertCursor(service.getTradeCursor(), 200, 0);
        assertTrue(service.getLastFailure().getMessage().contains("invalid gap boundary"));
        assertNull(service.getLastTradeContinuityWarning());
        assertEquals(3, service.sent.size());
        trades.assertNoValues();
        service.connect().test().assertError(IllegalStateException.class);
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void bookGapColdSnapshotPreservesTradeCursorAndSameHeightDeduplication() throws Exception {
    AtomicInteger fetches = new AtomicInteger();
    Harness service = new Harness(Collections.singleton("BTC"), Collections.singleton("ETH"), (coins, min) -> {
      assertEquals(Collections.singleton("BTC"), coins);
      assertEquals(BigInteger.ZERO, min);
      return SyntheticWire.seed(fetches.incrementAndGet() == 1 ? 100 : 150);
    }, 32);
    try {
      TestObserver<P2pStreamingService.TradePublication> trades = service.trades("ETH").test();
      startCombined(service);
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      awaitWorker(service);
      service.frame(tradeBatch(1, 120, tradeEvent("ETH", true)));
      service.frame(tradeBatch(2, 120, tradeEvent("ETH", false)));
      service.frame(SyntheticWire.status(3, 1, 120, 0));
      awaitWorker(service);
      assertCursor(service.getTradeCursor(), 120, 2);
      service.frame(SyntheticWire.status(0, 2, 102, 110));
      waitFor(() -> !service.channel.isOpen());
      int before = service.sent.size();
      startCombined(service);
      assertEquals(2, fetches.get());
      assertSubscription(service.sent.get(before + 2), "book", "BTC", 151);
      assertCursor(service.getTradeCursor(), 120, 2);
      service.frame(SyntheticWire.status(0, 1, 150, 0));
      awaitWorker(service);
      assertSubscription(service.sent.get(before + 3), "trades", "ETH", 120);
      service.frame(tradeBatch(1, 120, tradeEvent("ETH", true)));
      service.frame(tradeBatch(2, 120, tradeEvent("ETH", false)));
      service.frame(tradeBatch(3, 120, tradeEvent("ETH", true)));
      service.frame(SyntheticWire.status(3, 1, 120, 0));
      awaitWorker(service);
      assertTradeIds(trades, "p2p:trades:120:1:0", "p2p:trades:120:2:0", "p2p:trades:120:3:0");
      assertCursor(service.getTradeCursor(), 120, 3);
      assertCursor(service.getCursor(), 151, 0);
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void tradeSequenceGapWarmRetriesInclusivelyAndStillChecksSkippedReplaySequence() throws Exception {
    Harness service = tradeOnlyService();
    try {
      TestObserver<P2pStreamingService.TradePublication> trades = service.trades("BTC").test();
      service.startTrades(200);
      service.frame(tradeBatch(1, 200, tradeEvent("BTC", true)));
      service.frame(SyntheticWire.status(3, 1, 200, 0));
      awaitWorker(service);
      service.frame(tradeBatch(3, 201, tradeEvent("BTC", false)));
      waitFor(() -> !service.channel.isOpen());
      assertCursor(service.getTradeCursor(), 200, 1);
      assertTradeIds(trades, "p2p:trades:200:1:0");
      restartTrades(service, 200);
      service.frame(tradeBatch(2, 200, tradeEvent("BTC", true))); // Retained frames must also start at seq=1.
      waitFor(() -> !service.channel.isOpen());
      assertCursor(service.getTradeCursor(), 200, 1);
      assertTrue(service.getLastFailure().getMessage().contains("sequence"));
      assertTradeIds(trades, "p2p:trades:200:1:0");
      restartTrades(service, 200);
      service.frame(tradeBatch(1, 200, tradeEvent("BTC", true)));
      service.frame(tradeBatch(2, 201, tradeEvent("BTC", false)));
      service.frame(SyntheticWire.status(3, 1, 201, 0));
      awaitWorker(service);
      assertTradeIds(trades, "p2p:trades:200:1:0", "p2p:trades:201:1:0");
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void interruptedTradeBatchFansOutCurrentEventAndReplaysOnlyUncommittedSuffix() throws Exception {
    for (long height : Arrays.asList(200L, 201L)) {
      Harness service = tradeOnlyService();
      AtomicInteger interruptions = new AtomicInteger();
      try {
        TestObserver<P2pStreamingService.TradePublication> first = service.trades("BTC").doOnNext(event -> {
          if (interruptions.compareAndSet(0, 1)) {
            service.channel.close(); service.channel.runPendingTasks();
          }
        }).test();
        TestObserver<P2pStreamingService.TradePublication> second = service.trades("BTC").test();
        service.startTrades(200);
        service.frame(tradeBatch(1, 200)); // A committed frame precedes the interrupted one.
        service.frame(SyntheticWire.status(3, 1, 200, 0));
        awaitWorker(service);
        long oldGeneration = service.getGeneration();
        byte[] interrupted = tradeBatch(2, height, tradeEvent("BTC", true), tradeEvent("BTC", false), tradeEvent("BTC", true));
        service.frame(interrupted);
        waitFor(() -> !service.channel.isOpen());
        awaitWorker(service);
        String prefix = "p2p:trades:" + height + ":" + (height == 200 ? 2 : 1) + ":";
        assertNotEquals(oldGeneration, service.getGeneration());
        assertTradeIds(first, prefix + "0");
        assertTradeIds(second, prefix + "0");
        assertCursor(service.getTradeCursor(), 200, 1); // Neither frame count nor new height committed.
        restartTrades(service, 200);
        service.frame(tradeBatch(1, 200));
        service.frame(interrupted);
        service.frame(SyntheticWire.status(3, 1, height, 0));
        awaitWorker(service);
        assertTradeIds(first, prefix + "0", prefix + "1", prefix + "2");
        assertTradeIds(second, prefix + "0", prefix + "1", prefix + "2");
        assertCursor(service.getTradeCursor(), height, height == 200 ? 2 : 1);
        assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void changedInterruptedTradeReplayFailsBeforeAnyAdditionalPublication() throws Exception {
    for (boolean changePrefix : Arrays.asList(false, true)) {
      Harness service = tradeOnlyService();
      AtomicInteger interruptions = new AtomicInteger();
      try {
        TestObserver<P2pStreamingService.TradePublication> first = service.trades("BTC").doOnNext(event -> {
          if (interruptions.compareAndSet(0, 1)) {
            service.channel.close(); service.channel.runPendingTasks();
          }
        }).test();
        TestObserver<P2pStreamingService.TradePublication> second = service.trades("BTC").test();
        service.startTrades(200);
        service.frame(tradeBatch(1, 200, tradeEvent("BTC", true), tradeEvent("BTC", false)));
        waitFor(() -> !service.channel.isOpen());
        awaitWorker(service);
        assertTradeIds(first, "p2p:trades:200:1:0");
        assertTradeIds(second, "p2p:trades:200:1:0");
        assertCursor(service.getTradeCursor(), 200, 0);
        restartTrades(service, 200);
        service.frame(tradeBatch(1, 200, tradeEvent("BTC", !changePrefix), tradeEvent("BTC", !changePrefix)));
        waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.FAILED);
        awaitWorker(service);
        assertTrue(service.getLastFailure().getMessage().contains("Trade continuity failure"));
        assertTrue(service.getLastFailure().getCause().getMessage().contains("differs from interrupted frame"));
        assertTradeIds(first, "p2p:trades:200:1:0");
        assertTradeIds(second, "p2p:trades:200:1:0");
        assertCursor(service.getTradeCursor(), 200, 0);
        service.connect().test().assertError(IllegalStateException.class);
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void staleGenerationTradeCallbacksCannotPublishAdvanceCursorOrInvalidateReplacement() throws Exception {
    Harness service = tradeOnlyService();
    try {
      TestObserver<P2pStreamingService.TradePublication> trades = service.trades("BTC").test();
      service.startTrades(200);
      service.frame(tradeBatch(1, 200, tradeEvent("BTC", true)));
      service.frame(SyntheticWire.status(3, 1, 200, 0));
      awaitWorker(service);
      long oldGeneration = service.getGeneration();
      service.channel.close(); service.channel.runPendingTasks();
      restartTrades(service, 200);
      long replacement = service.getGeneration();
      service.binary(oldGeneration, tradeBatch(2, 999, tradeEvent("BTC", false)), 1, System.nanoTime());
      service.binary(oldGeneration, SyntheticWire.status(3, 1, 999, 0), 1, System.nanoTime());
      service.binary(oldGeneration, new byte[] {11}, 1, System.nanoTime()); // Stale malformed traffic is not decoded.
      awaitWorker(service);
      assertEquals(replacement, service.getGeneration());
      assertCursor(service.getTradeCursor(), 200, 1);
      assertTradeIds(trades, "p2p:trades:200:1:0");
      assertEquals(P2pStreamingService.Readiness.CATCHING_UP, service.getReadiness());
      service.frame(tradeBatch(1, 200, tradeEvent("BTC", true)));
      service.frame(tradeBatch(2, 200, tradeEvent("BTC", false)));
      service.frame(SyntheticWire.status(3, 1, 200, 0));
      awaitWorker(service);
      assertEquals(replacement, service.getGeneration());
      assertCursor(service.getTradeCursor(), 200, 2);
      assertTradeIds(trades, "p2p:trades:200:1:0", "p2p:trades:200:2:0");
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void disconnectDuringTradeDispatchDropsQueuedAndRemainingEventsWithoutCursorCommit() throws Exception {
    Harness service = tradeOnlyService();
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    try {
      TestObserver<P2pStreamingService.TradePublication> first = service.trades("BTC").doOnNext(event -> {
        entered.countDown();
        try { release.await(3, TimeUnit.SECONDS); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
      }).test();
      TestObserver<P2pStreamingService.TradePublication> second = service.trades("BTC").test();
      service.startTrades(200);
      service.frame(SyntheticWire.status(3, 1, 200, 0));
      awaitWorker(service);
      long oldGeneration = service.getGeneration();
      service.frame(tradeBatch(1, 201, tradeEvent("BTC", true), tradeEvent("BTC", false)));
      assertTrue(entered.await(3, TimeUnit.SECONDS));
      service.frame(tradeBatch(2, 202, tradeEvent("BTC", true)));
      assertTrue(service.disconnect().blockingAwait(3, TimeUnit.SECONDS));
      release.countDown();
      assertTrue(worker(service).awaitTermination(3, TimeUnit.SECONDS));
      service.binary(oldGeneration, tradeBatch(3, 999, tradeEvent("BTC", true)), 1, System.nanoTime());
      service.binary(service.getGeneration(), tradeBatch(1, 999, tradeEvent("BTC", true)), 1, System.nanoTime());
      assertNotEquals(oldGeneration, service.getGeneration());
      assertEquals(P2pStreamingService.Readiness.DISCONNECTED, service.getReadiness());
      assertCursor(service.getTradeCursor(), 200, 0);
      assertTradeIds(first, "p2p:trades:201:1:0");
      assertTradeIds(second, "p2p:trades:201:1:0");
      service.trades("BTC").test().assertNoValues();
      service.connect().test().assertError(IllegalStateException.class);
      service.awaitLive().test().awaitDone(3, TimeUnit.SECONDS).assertError(java.io.IOException.class);
    } finally { release.countDown(); service.stop(); }
  }

  @Test(timeout = 10000) public void revocationAfterDecodeFencesRecoveryStatusesAndRetention() throws Exception {
    for (int action = 0; action < 4; action++) {
      AtomicInteger fetches = new AtomicInteger();
      Harness service = combinedService(fetches);
      try {
        startCombined(service);
        service.frame(SyntheticWire.status(0, 1, 101, 0));
        awaitWorker(service);
        service.frame(tradeBatch(1, 101, tradeEvent("ETH", true)));
        service.frame(SyntheticWire.status(3, 1, 101, 0));
        awaitWorker(service);
        long token = service.getGeneration();
        final int selected = action;
        byte[] bytes = SyntheticWire.status(selected == 2 ? 0 : 3, selected == 1 ? 1 : 2,
            900, selected == 1 ? 0 : 901);
        P2pWireCodec.Frame decoded = new P2pWireCodec().decode(bytes);
        revokePausedWork(service, () -> {
          if (selected == 3) {
            java.lang.reflect.Method control = P2pStreamingService.class.getDeclaredMethod("control", String.class, long.class);
            control.setAccessible(true);
            control.invoke(service, "{\"channel\":\"errors\",\"code\":\"history_unavailable\",\"stream\":\"trades\",\"oldest_height\":900}", token);
          } else {
            java.lang.reflect.Method process = P2pStreamingService.class.getDeclaredMethod("process",
                P2pWireCodec.Frame.class, int.class, long.class, long.class, long.class);
            process.setAccessible(true);
            process.invoke(service, decoded, bytes.length, 1L, System.nanoTime(), System.nanoTime());
          }
          return null;
        });
        assertCursor(service.getTradeCursor(), 101, 1);
        assertCursor(service.getCursor(), 101, 0);
        assertEquals(1, fetches.get());
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void revokedBlockCannotInitializeTradeCursor() throws Exception {
    Harness service = tradeOnlyService();
    try {
      service.start();
      service.negotiate("{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"trades\":\"on\"}}", 1);
      int sent = service.sent.size();
      P2pWireCodec.Frame decoded = new P2pWireCodec().decode(blockHeight(900));
      revokePausedWork(service, () -> {
        java.lang.reflect.Method process = P2pStreamingService.class.getDeclaredMethod("process",
            P2pWireCodec.Frame.class, int.class, long.class, long.class, long.class);
        process.setAccessible(true);
        process.invoke(service, decoded, 41, 1L, System.nanoTime(), System.nanoTime());
        return null;
      });
      assertNull(service.getTradeCursor());
      assertEquals(sent, service.sent.size());
    } finally { service.stop(); }
  }

  @Test(timeout = 10000) public void pluralErrorEnvelopeWithoutSafeAttributionFailsClosed() throws Exception {
    Harness service = tradeOnlyService();
    try {
      service.startTrades(200);
      service.frame(SyntheticWire.status(3, 1, 200, 0));
      awaitWorker(service);
      service.text("{\"channel\":\"errors\",\"message\":\"unspecified failure\"}");
      awaitWorker(service);
      assertEquals(P2pStreamingService.Readiness.FAILED, service.getReadiness());
      assertCursor(service.getTradeCursor(), 200, 0);
    } finally { service.stop(); }
  }

  private static void revokePausedWork(Harness service, java.util.concurrent.Callable<Void> action) throws Exception {
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    java.util.concurrent.Future<Void> pending = worker(service).submit(() -> {
      entered.countDown();
      assertTrue(release.await(3, TimeUnit.SECONDS));
      return action.call();
    });
    try {
      assertTrue(entered.await(3, TimeUnit.SECONDS));
      service.channel.close();
      service.channel.runPendingTasks();
      long revoked = service.getGeneration();
      release.countDown();
      pending.get(3, TimeUnit.SECONDS);
      awaitWorker(service);
      assertEquals("Revoked work must not invalidate its successor", revoked, service.getGeneration());
    } finally { release.countDown(); }
  }

  @Test(timeout = 15000) public void revocationAtRecoveryCommitPreservesCursorAndInterruptedPrefix() throws Exception {
    for (int action = 0; action < 3; action++) {
      Harness service = tradeOnlyService();
      try {
        service.startTrades(200);
        service.frame(SyntheticWire.status(3, 1, 200, 0));
        awaitWorker(service);
        if (action != 1) {
          TestObserver<P2pStreamingService.TradePublication> interrupt = service.trades("BTC")
              .doOnNext(event -> service.channel.close()).test();
          service.frame(tradeBatch(1, 201, tradeEvent("BTC", true), tradeEvent("BTC", false)));
          awaitWorker(service);
          interrupt.assertValueCount(1);
          interrupt.dispose();
          restartTrades(service, 200);
          awaitWorker(service);
        }
        java.lang.reflect.Field partialField = P2pStreamingService.class.getDeclaredField("partialTrade");
        partialField.setAccessible(true);
        Object partial = partialField.get(service);
        if (action != 1) {
          assertNotNull(partial);
          java.lang.reflect.Field prefix = partial.getClass().getDeclaredField("emittedPrefix");
          prefix.setAccessible(true);
          assertEquals(1, prefix.getInt(partial));
        }
        java.lang.reflect.Field gateField = P2pStreamingService.class.getDeclaredField("stateGate");
        gateField.setAccessible(true);
        Object gate = gateField.get(service);
        long token = service.getGeneration();
        final int selected = action;
        java.util.concurrent.atomic.AtomicReference<Thread> runner = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.Future<Void> pending;
        synchronized (gate) {
          pending = worker(service).submit(() -> {
            runner.set(Thread.currentThread());
            if (selected == 2) {
              java.lang.reflect.Method control = P2pStreamingService.class.getDeclaredMethod("control", String.class, long.class);
              control.setAccessible(true);
              control.invoke(service, "{\"channel\":\"errors\",\"code\":\"history_unavailable\",\"stream\":\"trades\",\"oldest_height\":900}", token);
            } else {
              P2pWireCodec.Frame status = new P2pWireCodec().decode(SyntheticWire.status(3,
                  selected == 1 ? 1 : 2, 900, selected == 1 ? 0 : 901));
              java.lang.reflect.Method process = P2pStreamingService.class.getDeclaredMethod("process",
                  P2pWireCodec.Frame.class, int.class, long.class, long.class, long.class);
              process.setAccessible(true);
              process.invoke(service, status, 27, 1L, System.nanoTime(), System.nanoTime());
            }
            return null;
          });
          long owner = Thread.currentThread().getId();
          waitFor(() -> {
            Thread thread = runner.get();
            if (thread == null) return false;
            java.lang.management.ThreadInfo info = java.lang.management.ManagementFactory
                .getThreadMXBean().getThreadInfo(thread.getId());
            return info != null && info.getThreadState() == Thread.State.BLOCKED
                && info.getLockOwnerId() == owner;
          });
          java.lang.reflect.Method invalidate = P2pStreamingService.class.getDeclaredMethod("invalidate",
              long.class, Throwable.class, boolean.class);
          invalidate.setAccessible(true);
          invalidate.invoke(service, token, new java.io.IOException("test commit revocation"), false);
        }
        pending.get(3, TimeUnit.SECONDS);
        awaitWorker(service);
        assertCursor(service.getTradeCursor(), 200, 0);
        assertSame("An obsolete boundary must not erase the emitted prefix", partial, partialField.get(service));
        assertEquals(token + 1, service.getGeneration());
      } finally { service.stop(); }
    }
  }

  @Test(timeout = 10000) public void revokedSnapshotConstructionCannotInitializeTradeCursor() throws Exception {
    for (boolean stop : new boolean[] {false, true}) {
      CountDownLatch constructed = new CountDownLatch(1), release = new CountDownLatch(1);
      P2pWireCodec.Snapshot snapshot = SyntheticWire.seed(100,
          SyntheticWire.order(1, true, "BTC", "99", "1"), SyntheticWire.order(2, false, "BTC", "100", "1"));
      java.lang.reflect.Field books = P2pWireCodec.Snapshot.class.getDeclaredField("books");
      books.setAccessible(true);
      // Test-only pause after the real replacement books have been constructed, before commit.
      books.set(snapshot, new java.util.LinkedHashMap<String, List<P2pWireCodec.Change>>(snapshot.books) {
        @Override public void forEach(java.util.function.BiConsumer<? super String,
            ? super List<P2pWireCodec.Change>> action) {
          super.forEach(action);
          constructed.countDown();
          boolean interrupted = false;
          for (;;) {
            try { assertTrue(release.await(3, TimeUnit.SECONDS)); break; }
            catch (InterruptedException ignored) { interrupted = true; }
          }
          if (interrupted) Thread.currentThread().interrupt();
        }
      });
      Harness service = new Harness(Collections.singleton("BTC"), Collections.singleton("BTC"),
          (coins, min) -> snapshot, 32);
      try {
        TestObserver<Void> preparation = service.connect().test();
        assertTrue(constructed.await(3, TimeUnit.SECONDS));
        if (stop) {
          assertTrue(service.disconnect().blockingAwait(3, TimeUnit.SECONDS));
        } else {
          java.lang.reflect.Method invalidate = P2pStreamingService.class.getDeclaredMethod("invalidate",
              long.class, Throwable.class, boolean.class);
          invalidate.setAccessible(true);
          invalidate.invoke(service, service.getGeneration(), new java.io.IOException("revoked seed"), false);
        }
        release.countDown();
        if (stop) assertTrue(worker(service).awaitTermination(3, TimeUnit.SECONDS));
        else awaitWorker(service);
        preparation.assertError(java.io.IOException.class);
        assertNull(service.getCursor());
        assertNull(service.getTradeCursor());
        assertTrue(service.sent.isEmpty());
      } finally { release.countDown(); service.stop(); }
    }
  }

  private static Harness combinedService(AtomicInteger fetches) {
    return new Harness(Collections.singleton("BTC"), Collections.singleton("ETH"), (coins, min) -> {
      assertEquals(Collections.singleton("BTC"), coins);
      assertEquals(BigInteger.ZERO, min);
      fetches.incrementAndGet();
      return SyntheticWire.seed(100, SyntheticWire.order(1, true, "BTC", "99", "1"),
          SyntheticWire.order(2, false, "BTC", "100", "1"));
    }, 32);
  }

  private static Harness tradeOnlyService() {
    return new Harness(Collections.emptySet(), Collections.singleton("BTC"), (coins, min) -> {
      throw new AssertionError("Trade recovery must not request a book snapshot");
    }, 32);
  }

  private static void startCombined(Harness service) throws Exception {
    service.start();
    service.negotiate("{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"book\":\"on\",\"trades\":\"on\"}}", 2);
  }

  private static void restartTrades(Harness service, long height) throws Exception {
    int before = service.sent.size();
    service.start();
    service.negotiate("{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"trades\":\"on\"}}", 2);
    assertSubscription(service.sent.get(before + 2), "trades", "BTC", height);
  }

  private static java.lang.reflect.Field field(Object owner, String name) throws Exception {
    Class<?> type = owner.getClass();
    while (type != null) {
      try {
        java.lang.reflect.Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
      } catch (NoSuchFieldException ignored) { type = type.getSuperclass(); }
    }
    throw new NoSuchFieldException(name);
  }

  private static long ingressCount(Harness service, String name) throws Exception {
    return ((AtomicLong) field(service, name).get(service)).get();
  }

  private static P2pStreamingService.BinaryHandler handler(Harness service) {
    P2pStreamingService.BinaryHandler handler =
        service.channel.pipeline().get(P2pStreamingService.BinaryHandler.class);
    assertNotNull(handler);
    return handler;
  }

  private static BinaryWebSocketFrame heartbeatFrame() {
    return new BinaryWebSocketFrame(Unpooled.wrappedBuffer(new SyntheticWire.Bytes().u8(0).u64(0).bytes()));
  }

  private static void checkHealth(Harness service) throws Exception {
    java.lang.reflect.Method check = P2pStreamingService.class.getDeclaredMethod("checkHealth");
    check.setAccessible(true);
    check.invoke(service);
  }

  private static void assertReleased(Harness service,
      List<? extends io.netty.handler.codec.http.websocketx.WebSocketFrame> frames) throws Exception {
    for (io.netty.handler.codec.http.websocketx.WebSocketFrame frame : frames) assertEquals(0, frame.refCnt());
    assertEquals(0, ingressCount(service, "queuedFrames"));
    assertEquals(0, ingressCount(service, "queuedBytes"));
  }

  private static final class ReadProbe extends ChannelOutboundHandlerAdapter {
    final AtomicInteger reads = new AtomicInteger();
    @Override public void read(ChannelHandlerContext context) throws Exception {
      reads.incrementAndGet();
      super.read(context);
    }
  }

  private static final class WorkerBlock implements AutoCloseable {
    private final CountDownLatch release = new CountDownLatch(1);
    private final Future<?> task;
    WorkerBlock(Harness service) throws Exception {
      CountDownLatch entered = new CountDownLatch(1);
      task = worker(service).submit(() -> {
        entered.countDown();
        try {
          assertTrue("Timed out waiting to release controlled worker", release.await(3, TimeUnit.SECONDS));
        } catch (InterruptedException interrupted) {
          // shutdownNow is the behavior under test in the stop cleanup case.
          if (!service.isTerminal()) throw new AssertionError(interrupted);
          Thread.currentThread().interrupt();
        }
      });
      assertTrue("Worker did not enter controlled block", entered.await(3, TimeUnit.SECONDS));
    }
    @Override public void close() throws Exception {
      release.countDown();
      task.get(3, TimeUnit.SECONDS);
    }
  }

  private static java.util.concurrent.ExecutorService worker(Harness service) throws Exception {
    java.lang.reflect.Field field = P2pStreamingService.class.getDeclaredField("work");
    field.setAccessible(true);
    return (java.util.concurrent.ExecutorService) field.get(service);
  }

  private static void awaitWorker(Harness service) throws Exception {
    // A worker fence alone can overtake the next event-loop handoff. Drain both owners first.
    waitFor(() -> {
      try {
        Object handler = service.channel == null ? null
            : service.channel.pipeline().get(P2pStreamingService.BinaryHandler.class);
        return ingressCount(service, "queuedFrames") == 0
            && (handler == null || !field(handler, "handoff").getBoolean(handler));
      } catch (Exception e) { throw new AssertionError(e); }
    });
    worker(service).submit(() -> {}).get(3, TimeUnit.SECONDS);
    pumpChannels();
  }

  private static void assertHistoryOwner(Harness service, String expected) throws Exception {
    awaitWorker(service);
    java.lang.reflect.Field field = P2pStreamingService.class.getDeclaredField("pendingSubscription");
    field.setAccessible(true);
    Object owner = field.get(service);
    assertEquals(expected, owner == null ? null : owner.toString());
  }

  private static void assertCursor(P2pStreamingService.Cursor cursor, long height, long frames) {
    assertNotNull(cursor);
    assertEquals(BigInteger.valueOf(height), cursor.getHeight());
    assertEquals(frames, cursor.getFramesAtHeight());
  }

  private static byte[] diffAtBlockTime(long sequence, long height, long blockTimeMicros,
      P2pWireCodec.Change... changes) {
    byte[] bytes = SyntheticWire.diff(sequence, height, changes);
    // Book diff header: tag, sequence, local_time_us, block_time_us, height.
    ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        .putLong(1 + 2 * Long.BYTES, blockTimeMicros);
    return bytes;
  }

  private static void assertBookProvenance(OrderBook book, BigInteger micros, long height, long frames) {
    assertTrue(book.getTimeStamp() instanceof P2pOrderBook.BlockTimestamp);
    P2pOrderBook.BlockTimestamp timestamp = (P2pOrderBook.BlockTimestamp) book.getTimeStamp();
    assertEquals(micros, timestamp.getCreatedAtMicros());
    assertEquals(BigInteger.valueOf(height), timestamp.getBlockHeight());
    assertEquals(frames, timestamp.getFramesAtHeight());
    assertEquals(micros.divide(BigInteger.valueOf(1000)).longValueExact(), timestamp.getTime());
  }

  private static void assertBookTimestamps(OrderBook book, Ticker ticker, Date expected) {
    assertEquals("OrderBook must use exchange block time, not ingress/status time", expected, book.getTimeStamp());
    book.getBids().forEach(order -> assertEquals(expected, order.getTimestamp()));
    book.getAsks().forEach(order -> assertEquals(expected, order.getTimestamp()));
    assertEquals(expected, ticker.getTimestamp());
  }

  private static void assertTradeIds(TestObserver<P2pStreamingService.TradePublication> observer, String... expected) {
    List<String> actual = new ArrayList<>();
    for (P2pStreamingService.TradePublication publication : observer.values()) actual.add(publication.id);
    assertEquals(Arrays.asList(expected), actual);
    observer.assertNoErrors();
  }

  private static void assertSubscription(String message, String stream, String coin, long height) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    com.fasterxml.jackson.databind.node.ObjectNode actual =
        (com.fasterxml.jackson.databind.node.ObjectNode) mapper.readTree(message);
    // The documented book command may omit the default stream; trades must be explicit.
    if ("book".equals(stream) && !actual.has("stream")) actual.put("stream", "book");
    assertEquals(mapper.readTree("{\"method\":\"subscribe\",\"stream\":\"" + stream
        + "\",\"coins\":[\"" + coin + "\"],\"from_height\":" + height + "}"), actual);
  }

  private static SyntheticWire.Encoded tradeEvent(String coin, boolean buy) {
    SyntheticWire.Encoded buyer = SyntheticWire.tradeSide(new byte[20], BigInteger.ONE, null, null, "0");
    SyntheticWire.Encoded seller = SyntheticWire.tradeSide(new byte[20], BigInteger.TEN, null, null, "0");
    return SyntheticWire.trade(new BigInteger("123456789"), buy, null, coin, "10.25", "2.5", "Open", buyer, seller);
  }

  private static byte[] tradeBatch(long sequence, long height, SyntheticWire.Encoded... events) {
    return SyntheticWire.trades(BigInteger.valueOf(sequence), BigInteger.ZERO, BigInteger.ZERO,
        BigInteger.valueOf(height), BigInteger.ZERO, events).bytes;
  }

  private static byte[] blockHeight(long height) {
    return SyntheticWire.block(BigInteger.ZERO, BigInteger.valueOf(height), BigInteger.ZERO,
        BigInteger.ZERO, BigInteger.ZERO).bytes;
  }

  private static void pumpChannels() {
    for (Harness harness : ACTIVE_HARNESSES.get()) {
      for (EmbeddedChannel channel : harness.channelsCreated) {
        channel.runPendingTasks();
        channel.runScheduledPendingTasks();
        channel.checkException();
      }
    }
  }

  static void waitFor(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    do {
      pumpChannels();
      if (condition.getAsBoolean()) return;
      Thread.sleep(1);
    } while (System.nanoTime() < deadline);
    pumpChannels();
    assertTrue("Timed out waiting for deterministic worker/event-loop condition", condition.getAsBoolean());
  }

  static class Harness extends P2pStreamingService {
    volatile EmbeddedChannel channel;
    final List<String> sent = new CopyOnWriteArrayList<>();
    final List<EmbeddedChannel> channelsCreated = new CopyOnWriteArrayList<>();
    Harness(SnapshotSource snapshots, int capacity) {
      super(URI.create("wss://offline.invalid/ws"), Collections.singleton("BTC"), new P2pWireCodec(), snapshots, capacity);
      ACTIVE_HARNESSES.get().add(this);
    }
    Harness(Set<String> bookCoins, Set<String> tradeCoins, SnapshotSource snapshots, int capacity) {
      super(URI.create("wss://offline.invalid/ws"), bookCoins, tradeCoins, new P2pWireCodec(), snapshots, capacity);
      ACTIVE_HARNESSES.get().add(this);
    }
    @Override protected Completable openConnection() {
      return Completable.fromAction(() -> {
        WebSocketClientHandshaker handshaker = mock(WebSocketClientHandshaker.class);
        when(handshaker.isHandshakeComplete()).thenReturn(true);
        WebSocketClientHandler handler = getWebSocketClientHandler(handshaker, this::messageHandler);
        channel = new EmbeddedChannel(new ReadProbe(), handler);
        channelsCreated.add(channel);
      });
    }
    @Override protected void sendControl(String message, long token) {
      if (token == getGeneration()) sent.add(message);
    }
    void start() { assertTrue(connect().blockingAwait(3, TimeUnit.SECONDS)); }
    void negotiate() throws Exception {
      int before = sent.size();
      text(INFO);
      waitFor(() -> sent.size() == before + 2);
      awaitWorker(this);
    }
    void negotiate(String info, int controls) throws Exception {
      int before = sent.size();
      text(info);
      waitFor(() -> sent.size() == before + controls);
      awaitWorker(this);
    }
    void startTrades(long height) throws Exception {
      start();
      negotiate("{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"trades\":\"on\"}}", 1);
      frame(blockHeight(height));
      waitFor(() -> sent.size() == 3);
      assertSubscription(sent.get(2), "trades", "BTC", height);
    }
    void text(String text) {
      pumpChannels();
      channel.writeInbound(new TextWebSocketFrame(text));
      pumpChannels();
    }
    void frame(byte[] bytes) {
      pumpChannels();
      channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(bytes)));
      pumpChannels();
    }
    void stop() {
      try {
        disconnect().blockingAwait(3, TimeUnit.SECONDS);
        for (EmbeddedChannel channel : channelsCreated) {
          channel.runPendingTasks();
          channel.finishAndReleaseAll();
        }
      } finally { ACTIVE_HARNESSES.get().remove(this); }
    }
  }
}
