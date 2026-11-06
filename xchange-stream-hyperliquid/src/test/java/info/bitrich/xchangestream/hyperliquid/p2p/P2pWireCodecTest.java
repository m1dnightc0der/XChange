package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class P2pWireCodecTest {
  private final P2pWireCodec codec = new P2pWireCodec();

  @Test public void preservesUnsignedIdsDecimalPrecisionAndExtensionFields() {
    P2pWireCodec.Change change = new P2pWireCodec.Change(P2pWireCodec.MAX_U64, true, true,
        "BTC", new BigDecimal("12345678901234567890.123456789"),
        new BigDecimal("0.000000000000000000123"), "用户", P2pWireCodec.MAX_U64);
    P2pWireCodec.Diff frame = (P2pWireCodec.Diff) codec.decode(
        SyntheticWire.extension(SyntheticWire.diff(1, 101, change)));
    assertEquals(BigInteger.ONE, frame.sequence);
    assertEquals(BigInteger.valueOf(101), frame.height);
    assertEquals(P2pWireCodec.MAX_U64, frame.changes.get(0).oid);
    assertEquals(change.price, frame.changes.get(0).price);
    assertEquals(change.size, frame.changes.get(0).size);
    assertEquals(change.insertBefore, frame.changes.get(0).insertBefore);
    assertEquals("用户", frame.changes.get(0).user);
    assertThrows(UnsupportedOperationException.class, () -> frame.changes.clear());
  }

  @Test public void rejectsEveryTruncatedKnownFrameAndInvalidFlagsCountsUtf8() {
    byte[] valid = SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "12", "3"));
    for (int size = 0; size < valid.length; size++) {
      final byte[] truncated = Arrays.copyOf(valid, size);
      assertThrows(IllegalArgumentException.class, () -> codec.decode(truncated));
    }
    byte[] badFlag = valid.clone();
    badFlag[45] = 2;
    assertThrows(IllegalArgumentException.class, () -> codec.decode(badFlag));
    byte[] badCount = valid.clone();
    Arrays.fill(badCount, 33, 37, (byte) 255);
    assertThrows(IllegalArgumentException.class, () -> codec.decode(badCount));
    byte[] badUtf8 = valid.clone();
    badUtf8[48] = (byte) 255;
    assertThrows(IllegalArgumentException.class, () -> codec.decode(badUtf8));
    byte[] badDecimal = SyntheticWire.diff(1, 101, SyntheticWire.order(1, true, "BTC", "12", "3"));
    badDecimal[52] = 'N';
    assertThrows(IllegalArgumentException.class, () -> codec.decode(badDecimal));
  }

  @Test public void statusHasNoSequenceAndOtherChannelsDoNotBecomeBooks() {
    P2pWireCodec.Status status = (P2pWireCodec.Status) codec.decode(SyntheticWire.status(0, 2, 100, 105));
    assertEquals(2, status.state);
    assertEquals(BigInteger.valueOf(105), status.toHeight);
    assertTrue(codec.decode(new SyntheticWire.Bytes().u8(0).u64(0).bytes()) instanceof P2pWireCodec.Ignored);
    assertTrue(codec.decode(new byte[] {2}) instanceof P2pWireCodec.Ignored);
    assertTrue(codec.decode(new SyntheticWire.Bytes().u8(10).u64(0).bytes()) instanceof P2pWireCodec.Ignored);
    assertTrue(codec.decode(new SyntheticWire.Bytes().u8(11).u64(0).bytes()) instanceof P2pWireCodec.Ignored);
    assertThrows(IllegalArgumentException.class, () -> codec.decode(new byte[] {0}));
    assertThrows(IllegalArgumentException.class, () -> codec.decode(new byte[] {12}));
    assertTrue(codec.decode(new byte[] {(byte) 255}) instanceof P2pWireCodec.Ignored);
    assertThrows(IllegalArgumentException.class, () -> codec.decode(SyntheticWire.status(0, 1, 100, 1)));
  }

  @Test public void snapshotValidatesExactMembershipEmptyBooksHeightSideAndDuplicateIds() {
    Map<String, List<P2pWireCodec.Change>> books = new LinkedHashMap<>();
    books.put("BTC", Arrays.asList(SyntheticWire.order(1, true, "BTC", "10", "1"),
        SyntheticWire.order(2, false, "BTC", "11", "2")));
    books.put("ETH", Collections.emptyList());
    byte[] body = SyntheticWire.snapshot(100, books);
    P2pWireCodec.Snapshot snapshot = codec.decodeSnapshot(body, books.keySet(), BigInteger.valueOf(100), BigInteger.valueOf(99));
    assertEquals(2, snapshot.books.size());
    assertTrue(snapshot.books.get("ETH").isEmpty());
    assertThrows(IllegalArgumentException.class, () -> codec.decodeSnapshot(body,
        Collections.singleton("BTC"), BigInteger.valueOf(100), BigInteger.ZERO));
    assertThrows(IllegalArgumentException.class, () -> codec.decodeSnapshot(body,
        books.keySet(), BigInteger.valueOf(101), BigInteger.ZERO));
    assertThrows(IllegalArgumentException.class, () -> codec.decodeSnapshot(body,
        books.keySet(), BigInteger.valueOf(100), BigInteger.valueOf(101)));
    byte[] duplicate = SyntheticWire.seedBytes(100, SyntheticWire.order(1, true, "BTC", "10", "1"),
        SyntheticWire.order(1, false, "BTC", "11", "1"));
    assertThrows(IllegalArgumentException.class, () -> codec.decodeSnapshot(duplicate,
        Collections.singleton("BTC"), BigInteger.valueOf(100), BigInteger.ZERO));
    byte[] wrongSide = SyntheticWire.seedBytes(100, SyntheticWire.order(1, true, "BTC", "10", "1"));
    wrongSide[49] = 0;
    assertThrows(IllegalArgumentException.class, () -> codec.decodeSnapshot(wrongSide,
        Collections.singleton("BTC"), BigInteger.valueOf(100), BigInteger.ZERO));
  }

  @Test public void lengthAndDecimalAllocationLimitsFailClosed() {
    P2pWireCodec bounded = new P2pWireCodec(64, 128, 2);
    assertThrows(IllegalArgumentException.class, () -> bounded.decode(new byte[65]));
    byte[] body = new SyntheticWire.Bytes().u32(0xffffffffL).bytes();
    assertThrows(IllegalArgumentException.class, () -> codec.decodeSnapshot(body,
        Collections.singleton("BTC"), BigInteger.ONE, BigInteger.ZERO));
    byte[] malicious = new SyntheticWire.Bytes().u8(6).u64(1).u64(0).u64(0).u64(1).u32(1)
        .u64(1).u8(1).u8(1).text("BTC").text("1e2147483647").text("1").text("")
        .u8(0).u64(0).bytes();
    assertThrows(IllegalArgumentException.class, () -> codec.decode(malicious));
  }
}
