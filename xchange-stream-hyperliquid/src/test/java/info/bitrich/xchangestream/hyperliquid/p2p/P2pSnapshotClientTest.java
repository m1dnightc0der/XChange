package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assert.*;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class P2pSnapshotClientTest {
  private static final URI ENDPOINT = URI.create("https://snap.example/snapshot");

  @Test public void exactEncodedFiltersAndCommonHeightAreRequired() throws Exception {
    List<URI> requests = new ArrayList<>();
    Map<String, List<P2pWireCodec.Change>> books = new HashMap<>();
    books.put("BTC", Collections.emptyList());
    books.put("k:ABC", Collections.emptyList());
    P2pSnapshotClient client = client((uri, limit) -> {
      requests.add(uri);
      return response(200, "X-Snapshot-Height", "101", SyntheticWire.snapshot(101, books));
    }, new AtomicLong(), new ArrayList<>());
    P2pWireCodec.Snapshot snapshot = client.fetch(new LinkedHashSet<>(Arrays.asList("BTC", "k:ABC")), BigInteger.valueOf(100));
    assertEquals(books.keySet(), snapshot.books.keySet());
    assertEquals("min_height=100&coins=BTC%2Ck%3AABC", requests.get(0).getRawQuery());
  }

  @Test public void rejectsMissingHeaderOlderHeaderMembershipMismatchAndUnsafeUri() {
    for (P2pSnapshotClient.Response invalid : Arrays.asList(
        response(200, "Other", "100", SyntheticWire.seedBytes(100)),
        response(200, "X-Snapshot-Height", "99", SyntheticWire.seedBytes(99)),
        response(200, "X-Snapshot-Height", "101", SyntheticWire.seedBytes(100)),
        response(200, "X-Snapshot-Height", "100", SyntheticWire.snapshot(100,
            Collections.singletonMap("ETH", Collections.emptyList()))))) {
      P2pSnapshotClient client = client((uri, limit) -> invalid, new AtomicLong(), new ArrayList<>());
      assertThrows(IOException.class, () -> client.fetch(Collections.singleton("BTC"), BigInteger.valueOf(100)));
    }
    for (String uri : Arrays.asList("https://user:secret@snap.example/a", "https://snap.example/a#x",
        "file:///tmp/snapshot", "https://snap.example:0/a", "https://snap.example/a?coins=ALL")) {
      assertThrows(IllegalArgumentException.class, () -> new P2pSnapshotClient(URI.create(uri),
          Collections.emptySet(), new P2pWireCodec()));
    }
  }

  @Test public void redirectsRequireApprovedOriginsNoDowngradeAndConsumeQuota() throws Exception {
    List<URI> calls = new ArrayList<>();
    P2pSnapshotClient.Transport transport = (uri, limit) -> {
      calls.add(uri);
      if (uri.getHost().equals("snap.example")) return response(307, "Location", "https://blob.example/data", new byte[0]);
      return response(200, "X-Snapshot-Height", "100", SyntheticWire.seedBytes(100));
    };
    P2pSnapshotClient denied = client(transport, new AtomicLong(), new ArrayList<>());
    assertThrows(IOException.class, () -> denied.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    assertEquals(1, calls.size());
    calls.clear();
    P2pSnapshotClient approved = new P2pSnapshotClient(ENDPOINT,
        Collections.singleton(URI.create("https://blob.example")), new P2pWireCodec(), transport,
        () -> 0, ignored -> {});
    approved.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    approved.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(4, calls.size());
    assertThrows(P2pSnapshotClient.RetryLaterException.class,
        () -> approved.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    P2pSnapshotClient downgrade = new P2pSnapshotClient(ENDPOINT,
        Collections.singleton(URI.create("http://blob.example")), new P2pWireCodec(),
        (uri, limit) -> response(307, "Location", "http://blob.example/data", new byte[0]), () -> 0, ignored -> {});
    assertThrows(IOException.class, () -> downgrade.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
  }

  @Test public void documentedBackoffAndRetryAfterAreBoundedByLocalQuota() throws Exception {
    AtomicLong clock = new AtomicLong();
    List<Long> sleeps = new ArrayList<>();
    List<Integer> codes = new ArrayList<>(Arrays.asList(409, 503, 429, 200));
    P2pSnapshotClient client = client((uri, limit) -> {
      int code = codes.remove(0);
      return code == 200 ? response(200, "X-Snapshot-Height", "100", SyntheticWire.seedBytes(100))
          : response(code, "Retry-After", "7", new byte[0]);
    }, clock, sleeps);
    client.fetch(Collections.singleton("BTC"), BigInteger.ZERO);
    assertEquals(Arrays.asList(10_000L, 20_000L, 7_000L), sleeps);
    assertThrows(P2pSnapshotClient.RetryLaterException.class,
        () -> client.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    P2pSnapshotClient delayed = client((uri, limit) -> response(429, "Retry-After", "3600", new byte[0]),
        clock, new ArrayList<>());
    P2pSnapshotClient.RetryLaterException error = assertThrows(P2pSnapshotClient.RetryLaterException.class,
        () -> delayed.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    assertEquals(clock.get() + 3_600_000L, error.getRetryAtMillis());
  }

  @Test public void redirectCyclesAndOversizedSnapshotBodiesStop() {
    P2pSnapshotClient cyclic = client((uri, limit) -> response(307, "Location", "/snapshot", new byte[0]),
        new AtomicLong(), new ArrayList<>());
    assertThrows(IOException.class, () -> cyclic.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
    P2pSnapshotClient bounded = new P2pSnapshotClient(ENDPOINT, Collections.emptySet(),
        new P2pWireCodec(64, 128, 2), (uri, limit) -> response(200, "X-Snapshot-Height", "100", new byte[129]),
        () -> 0, ignored -> {});
    assertThrows(IOException.class, () -> bounded.fetch(Collections.singleton("BTC"), BigInteger.ZERO));
  }

  private static P2pSnapshotClient client(P2pSnapshotClient.Transport transport, AtomicLong clock, List<Long> sleeps) {
    return new P2pSnapshotClient(ENDPOINT, Collections.emptySet(), new P2pWireCodec(), transport,
        clock::get, delay -> { sleeps.add(delay); clock.addAndGet(delay); });
  }
  private static P2pSnapshotClient.Response response(int code, String header, String value, byte[] bytes) {
    return new P2pSnapshotClient.Response(code, Collections.singletonMap(header, value), bytes);
  }
}
