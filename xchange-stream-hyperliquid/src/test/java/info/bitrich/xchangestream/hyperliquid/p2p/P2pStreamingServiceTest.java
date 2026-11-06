package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.netty.buffer.Unpooled;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.Test;

/** Real provider handler + EmbeddedChannel ownership; no network or inherited global Rx hooks. */
public class P2pStreamingServiceTest {
  private static final String INFO = "{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"book\":\"on\"}}";

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
      service.frame(new SyntheticWire.Bytes().u8(0).u64(0).bytes()); // Block ingress remains consumable during HTTP.
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
      service.frame(new SyntheticWire.Bytes().u8(11).u64(0).bytes());
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

  @Test(timeout = 10000) public void overflowInvalidatesBeforeBlockedObserverReturnsAndNoStaleReadyResurrection() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 1);
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    try {
      service.book("BTC").subscribe(view -> {
        entered.countDown();
        try { release.await(3, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
      });
      service.start(); service.negotiate();
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      assertTrue(entered.await(3, TimeUnit.SECONDS));
      long token = service.getGeneration();
      service.frame(new SyntheticWire.Bytes().u8(0).u64(0).bytes());
      service.frame(new SyntheticWire.Bytes().u8(0).u64(0).bytes());
      assertNotEquals(token, service.getGeneration());
      assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      service.book("BTC").test().assertNoValues();
      release.countDown();
      waitFor(() -> !service.channel.isOpen());
      assertNotEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
    } finally { release.countDown(); service.stop(); }
  }

  @Test(timeout = 10000) public void blockIngressKeepsUnchangedBooksLiveButIngressSilenceClosesTransport() throws Exception {
    Harness service = new Harness((coins, min) -> SyntheticWire.seed(100), 32);
    try {
      service.start(); service.negotiate();
      service.frame(SyntheticWire.status(0, 1, 100, 0));
      waitFor(() -> service.getReadiness() == P2pStreamingService.Readiness.LIVE);
      java.lang.reflect.Field lastIngress = P2pStreamingService.class.getDeclaredField("lastIngressNanos");
      lastIngress.setAccessible(true);
      java.lang.reflect.Method check = P2pStreamingService.class.getDeclaredMethod("checkHealth");
      check.setAccessible(true);
      lastIngress.setLong(service, System.nanoTime() - TimeUnit.SECONDS.toNanos(31));
      service.frame(new SyntheticWire.Bytes().u8(0).u64(0).bytes());
      check.invoke(service);
      assertEquals(P2pStreamingService.Readiness.LIVE, service.getReadiness());
      lastIngress.setLong(service, System.nanoTime() - TimeUnit.SECONDS.toNanos(31));
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

  static void waitFor(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(1);
    assertTrue("Timed out waiting for deterministic worker condition", condition.getAsBoolean());
  }

  static class Harness extends P2pStreamingService {
    volatile EmbeddedChannel channel;
    final List<String> sent = new CopyOnWriteArrayList<>();
    final List<EmbeddedChannel> channelsCreated = new ArrayList<>();
    Harness(SnapshotSource snapshots, int capacity) {
      super(URI.create("wss://offline.invalid/ws"), Collections.singleton("BTC"), new P2pWireCodec(), snapshots, capacity);
    }
    @Override protected Completable openConnection() {
      return Completable.fromAction(() -> {
        WebSocketClientHandshaker handshaker = mock(WebSocketClientHandshaker.class);
        when(handshaker.isHandshakeComplete()).thenReturn(true);
        WebSocketClientHandler handler = getWebSocketClientHandler(handshaker, this::messageHandler);
        channel = new EmbeddedChannel(handler);
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
    }
    void text(String text) { channel.writeInbound(new TextWebSocketFrame(text)); }
    void frame(byte[] bytes) { channel.writeInbound(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(bytes))); }
    void stop() {
      disconnect().blockingAwait(3, TimeUnit.SECONDS);
      for (EmbeddedChannel channel : channelsCreated) channel.finishAndReleaseAll();
    }
  }
}
