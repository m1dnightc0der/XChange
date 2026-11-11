package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.hyperliquid.p2p.P2pStreamingService.Readiness;
import info.bitrich.xchangestream.hyperliquid.p2p.P2pStreamingService.TradePublication;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.reactivex.rxjava3.observers.TestObserver;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.Test;

/** Offline transport integration: no service overrides, external endpoints, or live feeds. */
public class P2pNettyReconnectTest {
  private static final long WAIT_SECONDS = 12;
  private static final Set<String> COINS = Collections.unmodifiableSet(
      new LinkedHashSet<>(Arrays.asList("BTC", "ETH")));

  @Test(timeout = 45000)
  public void inheritedReconnectRenegotiatesAndRetainsSnapshotAndSameHeightCursor()
      throws Exception {
    AtomicInteger snapshotRequests = new AtomicInteger();
    AtomicReference<URI> snapshotRequest = new AtomicReference<>();
    P2pWireCodec codec = new P2pWireCodec();
    // This transport is entirely in-memory; the configured HTTP URI is never contacted.
    P2pSnapshotClient snapshots = new P2pSnapshotClient(
        URI.create("http://127.0.0.1:1/snapshot"), Collections.emptySet(), codec,
        (uri, maxBytes) -> {
          snapshotRequests.incrementAndGet();
          snapshotRequest.set(uri);
          return new P2pSnapshotClient.Response(200,
              Collections.singletonMap("X-Snapshot-Height", "100"), snapshot());
        }, System::currentTimeMillis,
        delay -> { throw new AssertionError("Unexpected snapshot backoff: " + delay); });

    try (LoopbackPeer peer = new LoopbackPeer()) {
      P2pStreamingService service = new P2pStreamingService(peer.uri(), COINS, snapshots);
      TestObserver<P2pOrderBook.View> btc = service.book("BTC").test();
      TestObserver<P2pOrderBook.View> eth = service.book("ETH").test();
      TestObserver<Readiness> readiness = service.readiness().test();
      TestObserver<P2pStreamingService.Observation> observations = service.observations().test();
      TestObserver<P2pOrderBook.View> recovering = null;
      TestObserver<Void> live = null;
      try {
        assertTrue("Initial transport did not connect",
            service.connect().blockingAwait(WAIT_SECONDS, TimeUnit.SECONDS));
        Session first = peer.nextSession();
        first.expect("info");
        assertEquals(Readiness.CATCHING_UP, service.getReadiness());
        btc.assertNoValues();
        first.completeNegotiation(101);
        first.binary(diff(1, 101, "2"));
        first.binary(diff(2, 101, "3"));
        first.binary(status(1, 101));
        await("initial LIVE books", () -> service.getReadiness() == Readiness.LIVE
            && !btc.values().isEmpty() && !eth.values().isEmpty());
        assertCursor(service, 101, 2);
        assertLevel(last(btc).bids, "10", "3");
        assertLevel(last(eth).bids, "20", "1");
        assertEquals(1, snapshotRequests.get());
        assertEquals("min_height=0&coins=BTC,ETH", snapshotRequest.get().getQuery());
        long firstGeneration = service.getGeneration();
        int publicationCount = btc.values().size();

        // Only the peer closes the socket. Never call service.connect() again: the inherited
        // Netty reconnect owner must schedule recovery and invoke the provider's connect hook.
        first.closeSocket();
        await("generation revocation", () -> service.getGeneration() > firstGeneration
            && service.getReadiness() != Readiness.LIVE);
        recovering = service.book("BTC").test();
        live = service.awaitLive().test();
        Session second = peer.nextSession();
        assertFalse("Reconnect reused the old socket", first.channel == second.channel);
        second.expect("info");
        assertEquals("No commands may precede the new info reply", 1, second.commands.size());
        assertEquals(Readiness.CATCHING_UP, service.getReadiness());
        assertCursor(service, 101, 2);
        assertEquals(COINS, service.getSubscribedCoins());
        assertEquals("Ordinary reconnect must not fetch another snapshot", 1,
            snapshotRequests.get());
        recovering.assertNoValues();
        live.assertNotComplete();
        assertEquals("Retained stale book was republished", publicationCount, btc.values().size());

        second.completeNegotiation(101);
        recovering.assertNoValues();
        live.assertNotComplete();
        // Sequence restarts at 1, but the first two frames at inclusive cursor height 101
        // were already applied. A third same-height frame is new, not snapshot-covered.
        second.binary(diff(1, 101, "2"));
        second.binary(diff(2, 101, "3"));
        second.binary(diff(3, 101, "4"));
        second.binary(status(1, 101));
        live.awaitDone(WAIT_SECONDS, TimeUnit.SECONDS).assertComplete().assertNoErrors();
        TestObserver<P2pOrderBook.View> recoveredBooks = recovering;
        await("recovered publication", () -> !recoveredBooks.values().isEmpty());
        assertCursor(service, 101, 3);
        assertLevel(last(recovering).bids, "10", "4");
        assertLevel(last(eth).bids, "20", "1");
        assertLevel(last(eth).asks, "22", "1");
        assertTrue(service.getGeneration() > firstGeneration);
        assertEquals(Readiness.LIVE, service.getReadiness());

        // Replay skipping must stop at exactly n: the next height remains applicable.
        second.binary(diff(4, 102, "5"));
        await("next-height publication", () -> !recoveredBooks.values().isEmpty()
            && new BigDecimal("5").compareTo(last(recoveredBooks).bids.firstEntry().getValue()) == 0);
        assertCursor(service, 102, 1);
        assertEquals(COINS, service.getSubscribedCoins());
        assertEquals(1, snapshotRequests.get());
        first.assertOnlyNegotiation();
        second.assertOnlyNegotiation();
        peer.assertHealthy();
        btc.assertNoErrors();
        eth.assertNoErrors();
        recovering.assertNoErrors();
        observations.assertNoErrors();
        await("new-generation observation", () -> !observations.values().isEmpty()
            && observations.values().get(observations.values().size() - 1).getGeneration()
                == service.getGeneration());
        assertTrue("No recovery readiness transition observed",
            readiness.values().contains(Readiness.CATCHING_UP));
        assertTrue("LIVE was not reestablished", Collections.frequency(readiness.values(),
            Readiness.LIVE) >= 2);
      } finally {
        if (recovering != null) recovering.dispose();
        if (live != null) live.dispose();
        btc.dispose();
        eth.dispose();
        readiness.dispose();
        observations.dispose();
        assertTrue("Client event-loop shutdown did not complete",
            service.disconnect().blockingAwait(WAIT_SECONDS, TimeUnit.SECONDS));
      }
    }
  }

  @Test(timeout = 45000)
  public void combinedBooksAndTradesShareSocketAndResumeIndependentSameHeightCursors()
      throws Exception {
    Set<String> tradeCoins = Collections.singleton("SOL");
    AtomicInteger snapshotRequests = new AtomicInteger();
    AtomicReference<URI> snapshotRequest = new AtomicReference<>();
    Map<String, List<P2pWireCodec.Change>> seed = new LinkedHashMap<>();
    seed.put("BTC", Arrays.asList(SyntheticWire.order(1, true, "BTC", "10", "1"),
        SyntheticWire.order(2, false, "BTC", "12", "1")));
    seed.put("ETH", Arrays.asList(SyntheticWire.order(3, true, "ETH", "20", "1"),
        SyntheticWire.order(4, false, "ETH", "22", "1")));
    P2pSnapshotClient snapshots = new P2pSnapshotClient(
        URI.create("http://127.0.0.1:1/snapshot"), Collections.emptySet(), new P2pWireCodec(),
        (uri, maxBytes) -> {
          snapshotRequests.incrementAndGet();
          snapshotRequest.set(uri);
          return new P2pSnapshotClient.Response(200,
              Collections.singletonMap("X-Snapshot-Height", "100"),
              SyntheticWire.snapshot(100, seed));
        }, System::currentTimeMillis,
        delay -> { throw new AssertionError("Unexpected snapshot backoff: " + delay); });

    SyntheticWire.Encoded firstBuy = fill("SOL", true, "1");
    SyntheticWire.Encoded firstSell = fill("SOL", false, "2");
    SyntheticWire.Encoded nextHeightBuy = fill("SOL", true, "3");
    SyntheticWire.Encoded sameHeightSell = fill("SOL", false, "4");
    String[] initialIds = {"p2p:trades:101:1:0", "p2p:trades:101:1:1",
        "p2p:trades:102:1:0", "p2p:trades:102:2:0"};
    try (LoopbackPeer peer = new LoopbackPeer()) {
      P2pStreamingService service = new P2pStreamingService(peer.uri(), COINS, tradeCoins, snapshots);
      TestObserver<P2pOrderBook.View> btc = service.book("BTC").test();
      TestObserver<P2pOrderBook.View> eth = service.book("ETH").test();
      TestObserver<TradePublication> trades = service.trades("SOL").test();
      TestObserver<TradePublication> recoveringTrades = null;
      TestObserver<P2pOrderBook.View> recoveringBooks = null;
      TestObserver<Void> live = null;
      try {
        assertTrue("Combined transport did not connect",
            service.connect().blockingAwait(WAIT_SECONDS, TimeUnit.SECONDS));
        Session first = peer.nextSession();
        negotiateStreams(first, true);
        expectSubscription(first, false, COINS, 101);
        assertEquals("Trade subscription must wait for the book stream boundary", 3,
            first.commands.size());
        first.binary(SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "10", "2")));
        first.binary(SyntheticWire.diff(2, 101, SyntheticWire.order(1, true, "BTC", "10", "3")));
        first.binary(SyntheticWire.status(0, 1, 101, 0));
        expectSubscription(first, true, tradeCoins, 101);
        assertEquals(Readiness.CATCHING_UP, service.getReadiness());
        // Both logical streams start at seq=1 on the very same socket. Advancing trades
        // alone makes their retained replay boundary different from the book boundary.
        first.binary(tradeBatch(1, 101, firstBuy, firstSell));
        first.binary(tradeBatch(2, 102, nextHeightBuy));
        first.binary(tradeBatch(3, 102, sameHeightSell));
        first.binary(SyntheticWire.status(3, 1, 102, 0));
        await("combined LIVE streams", () -> service.getReadiness() == Readiness.LIVE
            && !btc.values().isEmpty() && !eth.values().isEmpty());
        assertCursor(service, 101, 2);
        assertTradeCursor(service, 102, 2);
        assertTradeIds(trades, initialIds);
        assertLevel(last(btc).bids, "10", "3");
        assertLevel(last(eth).asks, "22", "1");
        assertEquals(COINS, service.getBookCoins());
        assertEquals(tradeCoins, service.getTradeCoins());
        assertEquals("min_height=0&coins=BTC,ETH", snapshotRequest.get().getQuery());
        assertEquals(1, snapshotRequests.get());
        assertStreamNegotiation(first, true);
        assertOneSocket(peer);
        long firstGeneration = service.getGeneration();
        int bookPublications = btc.values().size();

        // Exercise only the inherited reconnect owner, not a second explicit connect().
        first.closeSocket();
        await("combined generation revocation", () -> service.getGeneration() > firstGeneration
            && service.getReadiness() != Readiness.LIVE);
        recoveringTrades = service.trades("SOL").test();
        recoveringBooks = service.book("BTC").test();
        live = service.awaitLive().test();
        Session second = peer.nextSession();
        assertFalse("Combined reconnect reused the old socket", first.channel == second.channel);
        second.expect("info");
        assertEquals("Only info is allowed before reconnect negotiation", 1, second.commands.size());
        assertCursor(service, 101, 2);
        assertTradeCursor(service, 102, 2);
        assertEquals("Warm reconnect fetched an extra snapshot", 1, snapshotRequests.get());
        assertEquals("Reconnect republished a stale book", bookPublications, btc.values().size());
        recoveringBooks.assertNoValues();
        recoveringTrades.assertNoValues();
        live.assertNotComplete();

        replyStreamInfo(second, true);
        expectSubscription(second, false, COINS, 101);
        second.binary(SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "10", "2")));
        second.binary(SyntheticWire.diff(2, 101, SyntheticWire.order(1, true, "BTC", "10", "3")));
        second.binary(SyntheticWire.diff(3, 101, SyntheticWire.order(1, true, "BTC", "10", "4")));
        second.binary(SyntheticWire.status(0, 1, 101, 0));
        expectSubscription(second, true, tradeCoins, 102);
        live.assertNotComplete();
        // Transport sequence restarts independently; height/ordinal identities do not.
        // These two complete frames are inclusive replay and must not be republished.
        second.binary(tradeBatch(1, 102, nextHeightBuy));
        second.binary(tradeBatch(2, 102, sameHeightSell));
        second.binary(SyntheticWire.status(3, 1, 102, 0));
        live.awaitDone(WAIT_SECONDS, TimeUnit.SECONDS).assertComplete().assertNoErrors();
        TestObserver<P2pOrderBook.View> recovered = recoveringBooks;
        await("combined recovered book", () -> !recovered.values().isEmpty());
        assertCursor(service, 101, 3);
        assertTradeCursor(service, 102, 2);
        assertLevel(last(recoveringBooks).bids, "10", "4");
        assertLevel(last(eth).bids, "20", "1");
        assertTradeIds(trades, initialIds);
        recoveringTrades.assertNoValues();

        second.binary(tradeBatch(3, 102, fill("SOL", true, "5"), fill("SOL", false, "6")));
        await("unseen same-height fills", () -> service.getTradeCursor().getFramesAtHeight() == 3);
        assertTradeIds(trades, "p2p:trades:101:1:0", "p2p:trades:101:1:1",
            "p2p:trades:102:1:0", "p2p:trades:102:2:0", "p2p:trades:102:3:0",
            "p2p:trades:102:3:1");
        assertTradeIds(recoveringTrades, "p2p:trades:102:3:0", "p2p:trades:102:3:1");
        assertEquals(0, new BigDecimal("5").compareTo(recoveringTrades.values().get(0).event.size));
        assertEquals(0, new BigDecimal("6").compareTo(recoveringTrades.values().get(1).event.size));
        assertTrue(recoveringTrades.values().get(0).event.buy);
        assertFalse(recoveringTrades.values().get(1).event.buy);

        second.binary(SyntheticWire.diff(4, 102, SyntheticWire.order(1, true, "BTC", "10", "5")));
        second.binary(tradeBatch(4, 103, fill("SOL", true, "7")));
        await("next-height combined cursors", () -> BigInteger.valueOf(102).equals(
            service.getCursor().getHeight()) && BigInteger.valueOf(103).equals(
            service.getTradeCursor().getHeight()));
        assertCursor(service, 102, 1);
        assertTradeCursor(service, 103, 1);
        assertLevel(last(btc).bids, "10", "5");
        assertTradeIds(trades, "p2p:trades:101:1:0", "p2p:trades:101:1:1",
            "p2p:trades:102:1:0", "p2p:trades:102:2:0", "p2p:trades:102:3:0",
            "p2p:trades:102:3:1", "p2p:trades:103:1:0");
        assertTradeIds(recoveringTrades, "p2p:trades:102:3:0", "p2p:trades:102:3:1",
            "p2p:trades:103:1:0");
        assertEquals(Readiness.LIVE, service.getReadiness());
        assertEquals("Warm reconnect must retain the only HTTP seed", 1, snapshotRequests.get());
        assertStreamNegotiation(first, true);
        assertStreamNegotiation(second, true);
        assertOneSocket(peer);
        btc.assertNoErrors();
        eth.assertNoErrors();
        recoveringBooks.assertNoErrors();
      } finally {
        if (live != null) live.dispose();
        if (recoveringBooks != null) recoveringBooks.dispose();
        if (recoveringTrades != null) recoveringTrades.dispose();
        btc.dispose();
        eth.dispose();
        trades.dispose();
        assertTrue("Combined client event-loop shutdown did not complete",
            service.disconnect().blockingAwait(WAIT_SECONDS, TimeUnit.SECONDS));
      }
    }
  }

  @Test(timeout = 30000)
  public void tradeOnlyUsesBlockBoundaryWithoutAnyHttpSnapshot() throws Exception {
    AtomicInteger snapshotRequests = new AtomicInteger();
    P2pSnapshotClient snapshots = new P2pSnapshotClient(
        URI.create("http://127.0.0.1:1/snapshot"), Collections.emptySet(), new P2pWireCodec(),
        (uri, maxBytes) -> {
          snapshotRequests.incrementAndGet();
          throw new AssertionError("Trade-only connection attempted an HTTP snapshot");
        }, System::currentTimeMillis,
        delay -> { throw new AssertionError("Trade-only snapshot backoff: " + delay); });
    try (LoopbackPeer peer = new LoopbackPeer()) {
      P2pStreamingService service = new P2pStreamingService(
          peer.uri(), Collections.emptySet(), COINS, snapshots);
      TestObserver<TradePublication> btc = service.trades("BTC").test();
      TestObserver<TradePublication> eth = service.trades("ETH").test();
      try {
        assertTrue("Trade-only transport did not connect",
            service.connect().blockingAwait(WAIT_SECONDS, TimeUnit.SECONDS));
        Session session = peer.nextSession();
        negotiateStreams(session, false);
        assertEquals("Trade-only subscribe requires a decoded block boundary", 2,
            session.commands.size());
        assertNull(service.getCursor());
        assertNull(service.getTradeCursor());
        assertEquals(Readiness.CATCHING_UP, service.getReadiness());
        session.binary(SyntheticWire.block(BigInteger.valueOf(1000), BigInteger.valueOf(200),
            BigInteger.valueOf(1_000_001), BigInteger.ONE, BigInteger.valueOf(1_000_002)).bytes);
        expectSubscription(session, true, COINS, 200);
        session.binary(tradeBatch(1, 200, fill("BTC", true, "1"), fill("ETH", false, "2")));
        session.binary(SyntheticWire.status(3, 1, 200, 0));
        await("trade-only LIVE", () -> service.getReadiness() == Readiness.LIVE);
        assertTradeCursor(service, 200, 1);
        assertTradeIds(btc, "p2p:trades:200:1:0");
        assertTradeIds(eth, "p2p:trades:200:1:1");
        assertEquals("BTC", btc.values().get(0).event.coin);
        assertEquals("ETH", eth.values().get(0).event.coin);
        assertTrue(service.getBookCoins().isEmpty());
        assertEquals(COINS, service.getTradeCoins());
        assertNull(service.getCursor());
        assertEquals("Trades must never request an HTTP book snapshot", 0, snapshotRequests.get());
        assertStreamNegotiation(session, false);
        assertOneSocket(peer);
      } finally {
        btc.dispose();
        eth.dispose();
        assertTrue("Trade-only client event-loop shutdown did not complete",
            service.disconnect().blockingAwait(WAIT_SECONDS, TimeUnit.SECONDS));
      }
    }
  }

  @Test(timeout = 90000)
  public void replayBurstBeyondWorkerCapacityPreservesGenerationAndBothCursors() throws Exception {
    final int replayFrames = 320;
    CountDownLatch decoding = new CountDownLatch(1);
    CountDownLatch releaseDecoder = new CountDownLatch(1);
    CountDownLatch ingressSettled = new CountDownLatch(1);
    AtomicInteger decodeCalls = new AtomicInteger();
    List<P2pWireCodec.Frame> decoded = new CopyOnWriteArrayList<>();
    P2pWireCodec realCodec = new P2pWireCodec();
    P2pWireCodec codec = spy(realCodec);
    doAnswer(invocation -> {
      if (decodeCalls.getAndIncrement() == 0) {
        decoding.countDown();
        assertTrue("Test did not release the replay decoder",
            releaseDecoder.await(4, TimeUnit.SECONDS));
      }
      // Only scheduling is controlled: every payload still uses the real strict decoder.
      P2pWireCodec.Frame frame = realCodec.decode(invocation.getArgument(0));
      decoded.add(frame);
      return frame;
    }).when(codec).decode(any(byte[].class));

    Map<String, List<P2pWireCodec.Change>> seed = new LinkedHashMap<>();
    seed.put("BTC", Arrays.asList(SyntheticWire.order(1, true, "BTC", "10", "1"),
        SyntheticWire.order(2, false, "BTC", "12", "1")));
    seed.put("ETH", Arrays.asList(SyntheticWire.order(3, true, "ETH", "20", "1"),
        SyntheticWire.order(4, false, "ETH", "22", "1")));
    AtomicInteger snapshotRequests = new AtomicInteger();
    P2pSnapshotClient snapshots = new P2pSnapshotClient(
        URI.create("http://127.0.0.1:1/snapshot"), Collections.emptySet(), realCodec,
        (uri, maxBytes) -> {
          snapshotRequests.incrementAndGet();
          return new P2pSnapshotClient.Response(200,
              Collections.singletonMap("X-Snapshot-Height", "100"),
              SyntheticWire.snapshot(100, seed));
        }, System::currentTimeMillis,
        delay -> { throw new AssertionError("Unexpected replay snapshot backoff: " + delay); });

    try (LoopbackPeer peer = new LoopbackPeer()) {
      P2pStreamingService service = new P2pStreamingService(peer.uri(), COINS,
          Collections.singleton("SOL"), codec, snapshots::fetch, 256);
      TestObserver<P2pOrderBook.View> books = service.book("BTC").test();
      TestObserver<TradePublication> trades = service.trades("SOL").test();
      TestObserver<Void> live = service.awaitLive().test();
      try {
        assertTrue("Replay transport did not connect",
            service.connect().blockingAwait(WAIT_SECONDS, TimeUnit.SECONDS));
        Session session = peer.nextSession();
        negotiateStreams(session, true);
        expectSubscription(session, false, COINS, 101);
        long generation = service.getGeneration();

        // Observe transport progress without overriding any service callback or injecting frames.
        // A demand-controlled channel pauses reads while the worker is occupied. The old
        // implementation instead closes on overflow. Either event wakes the assertion below;
        // successful server writes alone are deliberately NOT treated as client consumption.
        java.lang.reflect.Field activeChannel =
            P2pStreamingService.class.getDeclaredField("activeChannel");
        activeChannel.setAccessible(true);
        Channel client = (Channel) activeChannel.get(service);
        assertNotNull(client);
        client.closeFuture().addListener(ignored -> ingressSettled.countDown());
        io.netty.util.concurrent.Future<?> probeInstalled = client.eventLoop().submit(() ->
            client.pipeline().addFirst("replay-progress", new ChannelInboundHandlerAdapter() {
              @Override public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
                super.channelReadComplete(ctx);
                if (decoding.getCount() == 0 && !ctx.channel().config().isAutoRead()) {
                  ingressSettled.countDown();
                }
              }
            }));
        assertTrue("Replay progress probe was not installed",
            probeInstalled.await(2, TimeUnit.SECONDS));
        assertTrue("Replay progress probe failed", probeInstalled.isSuccess());

        session.binary(SyntheticWire.diff(1, 101,
            SyntheticWire.order(1, true, "BTC", "10", "1")));
        assertTrue("Worker never entered the real decoder", decoding.await(2, TimeUnit.SECONDS));
        // One task is executing; the remaining 319 diffs plus LIVE exceed the unchanged
        // 256-task queue. Submit one burst, not a write/worker-wait loop.
        io.netty.util.concurrent.Future<?> burstWritten = session.channel.eventLoop().submit(() -> {
          for (int sequence = 2; sequence <= replayFrames; sequence++) {
            session.channel.write(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(
                SyntheticWire.diff(sequence, 101,
                    SyntheticWire.order(1, true, "BTC", "10", Integer.toString(sequence))))));
          }
          session.channel.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(
              SyntheticWire.status(0, 1, 101, 0))));
        });
        assertTrue("Peer did not submit the replay burst", burstWritten.await(2, TimeUnit.SECONDS));
        assertTrue("Peer replay submission failed", burstWritten.isSuccess());
        // The readComplete for the first frame can precede decoder entry. A correctly
        // paused channel need not produce another readComplete while that decoder waits.
        // Observe the pause on its event loop instead of requiring a further network read.
        client.eventLoop().execute(() -> {
          if (!client.config().isAutoRead()) ingressSettled.countDown();
        });
        assertTrue("Neither demand pause nor overflow close was observed",
            ingressSettled.await(2, TimeUnit.SECONDS));
        assertEquals("Worker escaped its latch before the replay assertion", 1L,
            releaseDecoder.getCount());
        assertEquals("Replay burst revoked the provider generation: " + service.getLastFailure(),
            generation, service.getGeneration());
        assertNull("Replay burst must not overflow ingress", service.getLastFailure());
        assertEquals(1, decodeCalls.get());
        releaseDecoder.countDown();

        // Completing book replay permits the independent trade subscription on this socket.
        expectSubscription(session, true, Collections.singleton("SOL"), 101);
        for (int sequence = 1; sequence <= replayFrames; sequence++) {
          session.binary(tradeBatch(sequence, 101, fill("SOL", true, Integer.toString(sequence))));
        }
        session.binary(SyntheticWire.status(3, 1, 101, 0));
        live.awaitDone(WAIT_SECONDS, TimeUnit.SECONDS).assertComplete().assertNoErrors();
        books.assertNoErrors();
        assertFalse("Complete replay did not publish a book", books.values().isEmpty());
        assertLevel(last(books).bids, "10", Integer.toString(replayFrames));
        assertCursor(service, 101, replayFrames);
        assertTradeCursor(service, 101, replayFrames);
        trades.assertNoErrors().assertValueCount(replayFrames);
        assertEquals("Dropped or duplicated decoded replay frames", replayFrames * 2 + 2,
            decoded.size());
        for (int index = 0; index < replayFrames; index++) {
          assertTrue("Book replay frame reordered", decoded.get(index) instanceof P2pWireCodec.Diff);
          assertEquals(BigInteger.valueOf(index + 1),
              ((P2pWireCodec.Diff) decoded.get(index)).sequence);
          assertTrue("Trade replay frame reordered",
              decoded.get(replayFrames + 1 + index) instanceof P2pWireCodec.TradeBatch);
          assertEquals(BigInteger.valueOf(index + 1),
              ((P2pWireCodec.TradeBatch) decoded.get(replayFrames + 1 + index)).sequence);
          assertEquals("p2p:trades:101:" + (index + 1) + ":0", trades.values().get(index).id);
          assertEquals(0, BigDecimal.valueOf(index + 1)
              .compareTo(trades.values().get(index).event.size));
        }
        assertEquals(generation, service.getGeneration());
        assertNull(service.getLastFailure());
        assertEquals(Readiness.LIVE, service.getReadiness());
        assertEquals("Replay must retain its only snapshot", 1, snapshotRequests.get());
        assertStreamNegotiation(session, true);
        assertOneSocket(peer);
      } finally {
        // Always unblock before stopping the worker, including the intended RED assertion path.
        releaseDecoder.countDown();
        live.dispose();
        books.dispose();
        trades.dispose();
        assertTrue("Replay client event-loop shutdown did not complete",
            service.disconnect().blockingAwait(WAIT_SECONDS, TimeUnit.SECONDS));
      }
    }
  }

  @Test(timeout = 45000)
  public void startupBookReplaySocketCloseUsesInheritedReconnect() throws Exception {
    assertStartupBookReplayRecovery(false);
  }

  @Test(timeout = 45000)
  public void documentedHistoryErrorDuringStartupUsesInheritedReconnect() throws Exception {
    assertStartupBookReplayRecovery(true);
  }

  private static void assertStartupBookReplayRecovery(boolean historyError) throws Exception {
    AtomicInteger snapshotRequests = new AtomicInteger();
    P2pSnapshotClient snapshots = startupSnapshots(snapshotRequests);
    try (LoopbackPeer peer = new LoopbackPeer()) {
      P2pStreamingService service = new P2pStreamingService(peer.uri(), COINS,
          Collections.singleton("SOL"), snapshots);
      TestObserver<Void> live = service.awaitLive().test();
      TestObserver<TradePublication> trades = service.trades("SOL").test();
      try {
        assertTrue(service.connect().blockingAwait(WAIT_SECONDS, TimeUnit.SECONDS));
        Session first = peer.nextSession();
        negotiateStreams(first, true);
        expectSubscription(first, false, COINS, 101);
        assertEquals("Reproduce the last logged startup command", 3, first.commands.size());
        assertEquals(Readiness.CATCHING_UP, service.getReadiness());
        long generation = service.getGeneration();
        if (historyError) {
          // Wait for the error before closing so a racing close cannot discard the
          // documented error as stale and accidentally hide terminal classification.
          first.channel.writeAndFlush(new TextWebSocketFrame(
              "{\"channel\":\"errors\",\"code\":\"history_error\",\"message\":\"reader failed\"}"))
              .sync();
          await("history_error handled", () -> service.getGeneration() > generation
              || service.isTerminal());
          assertFalse("Recoverable history_error became terminal: " + service.getLastFailure(),
              service.isTerminal());
          if (first.channel.isOpen()) {
            io.netty.channel.ChannelFuture lateClose = first.channel.writeAndFlush(
                new io.netty.handler.codec.http.websocketx.CloseWebSocketFrame(1011, "history_error"));
            assertTrue(lateClose.await(WAIT_SECONDS, TimeUnit.SECONDS));
            // The client may already have closed this failed generation. The exact
            // late-1011 successor fence is also exercised by the embedded regression.
            assertTrue(lateClose.isSuccess() || lateClose.cause() instanceof java.nio.channels.ClosedChannelException);
          }
        } else {
          first.closeSocket();
        }
        Session second = peer.nextSession();
        assertFalse("Reconnect must use a new transport", first.channel == second.channel);
        negotiateStreams(second, true);
        expectSubscription(second, false, COINS, 101);
        assertEquals("Recoverable startup failure must retain its valid snapshot", 1,
            snapshotRequests.get());
        second.binary(SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "10", "2")));
        second.binary(SyntheticWire.status(0, 1, 101, 0));
        expectSubscription(second, true, Collections.singleton("SOL"), 101);
        second.binary(tradeBatch(1, 101, fill("SOL", true, "1")));
        second.binary(SyntheticWire.status(3, 1, 101, 0));
        await("both recovered channels LIVE", () -> service.getReadiness() == Readiness.LIVE);
        live.awaitDone(WAIT_SECONDS, TimeUnit.SECONDS).assertComplete().assertNoErrors();
        assertTradeIds(trades, "p2p:trades:101:1:0");
        assertOneSocket(peer);
      } finally {
        live.dispose();
        trades.dispose();
        service.disconnect().blockingAwait(WAIT_SECONDS, TimeUnit.SECONDS);
      }
    }
  }

  @Test(timeout = 45000)
  public void documentedInformationalErrorLogDoesNotAbortStartup() throws Exception {
    AtomicInteger snapshotRequests = new AtomicInteger();
    try (LoopbackPeer peer = new LoopbackPeer()) {
      P2pStreamingService service = new P2pStreamingService(peer.uri(), COINS,
          Collections.singleton("SOL"), startupSnapshots(snapshotRequests));
      TestObserver<Void> live = service.awaitLive().test();
      try {
        assertTrue(service.connect().blockingAwait(WAIT_SECONDS, TimeUnit.SECONDS));
        Session session = peer.nextSession();
        negotiateStreams(session, true);
        expectSubscription(session, false, COINS, 101);
        session.channel.write(new TextWebSocketFrame(
            "{\"channel\":\"errors\",\"error\":\"history_error while rotating server log\"}"));
        session.channel.write(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(
            SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "10", "2")))));
        session.channel.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(
            SyntheticWire.status(0, 1, 101, 0)))).sync();
        await("server log and following book status handled", () -> service.isTerminal()
            || session.commands.size() > 3);
        assertFalse("Informational server log became terminal: " + service.getLastFailure(),
            service.isTerminal());
        expectSubscription(session, true, Collections.singleton("SOL"), 101);
        session.binary(SyntheticWire.status(3, 1, 101, 0));
        live.awaitDone(WAIT_SECONDS, TimeUnit.SECONDS).assertComplete().assertNoErrors();
        assertEquals(1, snapshotRequests.get());
        assertOneSocket(peer);
      } finally {
        live.dispose();
        service.disconnect().blockingAwait(WAIT_SECONDS, TimeUnit.SECONDS);
      }
    }
  }

  private static P2pSnapshotClient startupSnapshots(AtomicInteger requests) {
    return new P2pSnapshotClient(URI.create("http://127.0.0.1:1/snapshot"),
        Collections.emptySet(), new P2pWireCodec(), (uri, maxBytes) -> {
          requests.incrementAndGet();
          return new P2pSnapshotClient.Response(200,
              Collections.singletonMap("X-Snapshot-Height", "100"), snapshot());
        }, System::currentTimeMillis,
        delay -> { throw new AssertionError("Unexpected snapshot backoff: " + delay); });
  }

  private static void negotiateStreams(Session session, boolean books) throws Exception {
    assertEquals("{\"method\":\"info\"}", session.expect("info").toString());
    replyStreamInfo(session, books);
  }

  private static void replyStreamInfo(Session session, boolean books) throws Exception {
    session.channel.writeAndFlush(new TextWebSocketFrame(
        "{\"channel\":\"info\",\"binary_version\":3,\"streams\":{" +
            (books ? "\"book\":\"on\"," : "") + "\"trades\":\"on\"}}"))
        .sync();
    JsonNode esp = session.expect("esp");
    assertEquals(2, esp.size());
    assertEquals(3, esp.path("version").asInt());
  }

  private static void expectSubscription(Session session, boolean trades, Set<String> coins,
      long fromHeight) throws Exception {
    JsonNode subscribe = session.expect("subscribe");
    assertEquals("Unexpected subscribe fields", trades ? 4 : 3, subscribe.size());
    if (trades) assertEquals("trades", subscribe.path("stream").asText());
    else assertFalse("Book subscribe must use the documented default stream", subscribe.has("stream"));
    assertTrue(subscribe.path("from_height").isIntegralNumber());
    assertEquals(BigInteger.valueOf(fromHeight), subscribe.path("from_height").bigIntegerValue());
    assertTrue(subscribe.path("coins").isArray());
    Set<String> actual = new LinkedHashSet<>();
    subscribe.path("coins").forEach(coin -> actual.add(coin.asText()));
    assertEquals("Wrong logical-stream coin filter", coins, actual);
    assertEquals("Duplicate logical-stream coin filter", coins.size(), subscribe.path("coins").size());
  }

  private static void assertStreamNegotiation(Session session, boolean books) {
    assertEquals("Unexpected controls or duplicate stream subscriptions", books ? 4 : 3,
        session.commands.size());
    assertEquals("info", session.commands.get(0).path("method").asText());
    assertEquals("esp", session.commands.get(1).path("method").asText());
    assertEquals("subscribe", session.commands.get(2).path("method").asText());
    if (books) {
      assertFalse(session.commands.get(2).has("stream"));
      assertEquals("subscribe", session.commands.get(3).path("method").asText());
    }
    assertEquals("trades", session.commands.get(books ? 3 : 2).path("stream").asText());
    assertTrue("Unconsumed control command", session.incoming.isEmpty());
  }

  private static void assertOneSocket(LoopbackPeer peer) {
    peer.assertHealthy();
    assertTrue("Logical streams opened an extra WebSocket session", peer.sessions.isEmpty());
    assertEquals("Expected only the listening channel and one shared client socket", 2,
        peer.channels.size());
  }

  private static void assertTradeCursor(P2pStreamingService service, long height, long frames) {
    assertNotNull(service.getTradeCursor());
    assertEquals(BigInteger.valueOf(height), service.getTradeCursor().getHeight());
    assertEquals(frames, service.getTradeCursor().getFramesAtHeight());
  }

  private static void assertTradeIds(TestObserver<TradePublication> trades, String... expected) {
    trades.assertNoErrors().assertValueCount(expected.length);
    for (int i = 0; i < expected.length; i++) assertEquals(expected[i], trades.values().get(i).id);
  }

  private static SyntheticWire.Encoded fill(String coin, boolean buy, String size) {
    SyntheticWire.Encoded buyer = SyntheticWire.tradeSide(new byte[20], BigInteger.valueOf(11),
        null, null, "0");
    byte[] sellerAddress = new byte[20];
    sellerAddress[19] = 1;
    SyntheticWire.Encoded seller = SyntheticWire.tradeSide(sellerAddress, BigInteger.valueOf(12),
        null, null, "0");
    return SyntheticWire.trade(BigInteger.valueOf(1_000_123), buy, null, coin,
        "BTC".equals(coin) ? "10" : "20", size, buy ? "Open Long" : "Open Short", buyer, seller);
  }

  private static byte[] tradeBatch(long sequence, long height, SyntheticWire.Encoded... fills) {
    return SyntheticWire.trades(BigInteger.valueOf(sequence), BigInteger.valueOf(1_000_200),
        BigInteger.valueOf(1_000_000), BigInteger.valueOf(height), BigInteger.valueOf(1_000_300),
        fills).bytes;
  }

  private static void assertCursor(P2pStreamingService service, long height, long frames) {
    assertNotNull(service.getCursor());
    assertEquals(BigInteger.valueOf(height), service.getCursor().getHeight());
    assertEquals(frames, service.getCursor().getFramesAtHeight());
  }

  private static P2pOrderBook.View last(TestObserver<P2pOrderBook.View> observer) {
    List<P2pOrderBook.View> values = observer.values();
    return values.get(values.size() - 1);
  }

  private static void assertLevel(java.util.Map<BigDecimal, BigDecimal> levels,
      String price, String size) {
    BigDecimal actual = levels.get(new BigDecimal(price));
    assertNotNull("Missing level " + price, actual);
    assertEquals(0, new BigDecimal(size).compareTo(actual));
  }

  private static void await(String description, BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      TimeUnit.MILLISECONDS.sleep(5);
    }
    assertTrue("Timed out waiting for " + description, condition.getAsBoolean());
  }

  private static final class LoopbackPeer implements AutoCloseable {
    final NioEventLoopGroup group = new NioEventLoopGroup(1,
        new DefaultThreadFactory("p2p-reconnect-test-peer", true));
    final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    final BlockingQueue<Session> sessions = new LinkedBlockingQueue<>();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Channel listener;

    LoopbackPeer() throws Exception {
      try {
        listener = new ServerBootstrap().group(group).channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>() {
              @Override protected void initChannel(SocketChannel channel) {
                channels.add(channel);
                channel.pipeline().addLast(new HttpServerCodec(), new HttpObjectAggregator(8192),
                    new WebSocketServerProtocolHandler("/ws", null, false, 65536),
                    new SimpleChannelInboundHandler<WebSocketFrame>() {
                      private final Session session = new Session(channel);
                      @Override public void userEventTriggered(ChannelHandlerContext ctx, Object event)
                          throws Exception {
                        if (event instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
                          sessions.add(session);
                        }
                        super.userEventTriggered(ctx, event);
                      }
                      @Override protected void channelRead0(ChannelHandlerContext ctx,
                          WebSocketFrame frame) throws Exception {
                        if (!(frame instanceof TextWebSocketFrame)) {
                          throw new AssertionError("Unexpected client binary/control frame");
                        }
                        JsonNode command = new ObjectMapper().readTree(
                            ((TextWebSocketFrame) frame).text());
                        session.commands.add(command);
                        session.incoming.add(command);
                      }
                      @Override public void exceptionCaught(ChannelHandlerContext ctx,
                          Throwable error) {
                        failure.compareAndSet(null, error);
                        ctx.close();
                      }
                    });
              }
            }).bind(new InetSocketAddress("127.0.0.1", 0)).sync().channel();
        channels.add(listener);
      } catch (Exception | Error error) {
        group.shutdownGracefully(0, 2, TimeUnit.SECONDS).syncUninterruptibly();
        throw error;
      }
    }

    URI uri() {
      return URI.create("ws://127.0.0.1:"
          + ((InetSocketAddress) listener.localAddress()).getPort() + "/ws");
    }

    Session nextSession() throws Exception {
      Session session = sessions.poll(WAIT_SECONDS, TimeUnit.SECONDS);
      assertHealthy();
      assertNotNull("No WebSocket handshake (automatic reconnect did not run)", session);
      return session;
    }

    void assertHealthy() {
      if (failure.get() != null) throw new AssertionError("Loopback peer failed", failure.get());
    }

    @Override public void close() throws Exception {
      try {
        assertTrue("Peer channels did not close",
            channels.close().await(WAIT_SECONDS, TimeUnit.SECONDS));
      } finally {
        assertTrue("Peer event loop did not terminate",
            group.shutdownGracefully(0, 2, TimeUnit.SECONDS)
                .await(WAIT_SECONDS, TimeUnit.SECONDS));
      }
    }
  }

  private static final class Session {
    final Channel channel;
    final BlockingQueue<JsonNode> incoming = new LinkedBlockingQueue<>();
    final List<JsonNode> commands = new CopyOnWriteArrayList<>();

    Session(Channel channel) { this.channel = channel; }

    JsonNode expect(String method) throws Exception {
      JsonNode command = incoming.poll(WAIT_SECONDS, TimeUnit.SECONDS);
      assertNotNull("Missing " + method + " command", command);
      assertEquals(method, command.path("method").asText());
      return command;
    }

    void completeNegotiation(long fromHeight) throws Exception {
      channel.writeAndFlush(new TextWebSocketFrame(
          "{\"channel\":\"info\",\"binary_version\":3,\"streams\":{\"book\":\"on\"}}"))
          .sync();
      assertEquals(3, expect("esp").path("version").asInt());
      JsonNode subscribe = expect("subscribe");
      assertEquals(BigInteger.valueOf(fromHeight), subscribe.path("from_height").bigIntegerValue());
      assertTrue(subscribe.path("coins").isArray());
      Set<String> actual = new LinkedHashSet<>();
      subscribe.path("coins").forEach(coin -> actual.add(coin.asText()));
      assertEquals(COINS, actual);
      assertEquals(COINS.size(), subscribe.path("coins").size());
      // No invented esp/subscribe success acknowledgements are sent by this peer.
      assertOnlyNegotiation();
    }

    void assertOnlyNegotiation() {
      assertEquals("Expected exactly info, esp and one complete subscribe", 3, commands.size());
      assertEquals("info", commands.get(0).path("method").asText());
      assertEquals("esp", commands.get(1).path("method").asText());
      assertEquals("subscribe", commands.get(2).path("method").asText());
    }

    void binary(byte[] bytes) throws Exception {
      channel.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(bytes))).sync();
    }

    void closeSocket() throws Exception { channel.close().sync(); }
  }

  // Independent minimal v3 fixtures: little-endian integers and u8-length UTF-8 strings.
  // WS frames have sent_ts_us trailers; length-delimited HTTP snapshots do not.
  private static byte[] snapshot() {
    ByteArrayOutputStream body = new ByteArrayOutputStream();
    for (String coin : COINS) {
      ByteBuf frame = Unpooled.buffer();
      try {
        frame.writeByte(12).writeLongLE(0).writeLongLE(100).writeLongLE(0);
        string(frame, coin);
        frame.writeIntLE(1).writeIntLE(1);
        // Order identifiers are unique across the complete snapshot, not only per coin.
        snapshotOrder(frame, "BTC".equals(coin) ? 1 : 3, true, "BTC".equals(coin) ? "10" : "20");
        snapshotOrder(frame, "BTC".equals(coin) ? 2 : 4, false, "BTC".equals(coin) ? "12" : "22");
        int length = frame.readableBytes();
        for (int shift = 0; shift < 32; shift += 8) body.write(length >>> shift & 255);
        byte[] bytes = copy(frame);
        body.write(bytes, 0, bytes.length);
      } finally { frame.release(); }
    }
    return body.toByteArray();
  }

  private static void snapshotOrder(ByteBuf frame, long id, boolean bid, String price) {
    frame.writeLongLE(id).writeByte(bid ? 1 : 0);
    string(frame, price);
    string(frame, "1");
    string(frame, "fixture-user");
  }

  private static byte[] diff(long sequence, long height, String size) {
    ByteBuf frame = Unpooled.buffer();
    try {
      frame.writeByte(6).writeLongLE(sequence).writeLongLE(0).writeLongLE(0)
          .writeLongLE(height).writeIntLE(1);
      frame.writeLongLE(1).writeByte(1).writeByte(1);
      string(frame, "BTC");
      string(frame, "10");
      string(frame, size);
      string(frame, "fixture-user");
      frame.writeByte(0).writeLongLE(0);
      return copy(frame);
    } finally { frame.release(); }
  }

  private static byte[] status(int state, long height) {
    ByteBuf frame = Unpooled.buffer();
    try {
      frame.writeByte(13).writeByte(0).writeByte(state).writeLongLE(height)
          .writeLongLE(0).writeLongLE(0);
      return copy(frame);
    } finally { frame.release(); }
  }

  private static void string(ByteBuf frame, String value) {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    frame.writeByte(bytes.length).writeBytes(bytes);
  }

  private static byte[] copy(ByteBuf frame) {
    byte[] bytes = new byte[frame.readableBytes()];
    frame.getBytes(frame.readerIndex(), bytes);
    return bytes;
  }
}
