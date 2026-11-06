package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.hyperliquid.p2p.P2pStreamingService.Readiness;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
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
