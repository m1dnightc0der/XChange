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
    assertThrows(IllegalArgumentException.class,
        () -> codec.decode(new SyntheticWire.Bytes().u8(0).u64(0).bytes()));
    assertTrue(codec.decode(new byte[] {2}) instanceof P2pWireCodec.Ignored);
    assertTrue(codec.decode(new SyntheticWire.Bytes().u8(10).u64(0).bytes()) instanceof P2pWireCodec.Ignored);
    assertThrows(IllegalArgumentException.class,
        () -> codec.decode(new SyntheticWire.Bytes().u8(11).u64(0).bytes()));
    assertThrows(IllegalArgumentException.class, () -> codec.decode(new byte[] {0}));
    assertThrows(IllegalArgumentException.class, () -> codec.decode(new byte[] {12}));
    assertTrue(codec.decode(new byte[] {(byte) 255}) instanceof P2pWireCodec.Ignored);
    assertThrows(IllegalArgumentException.class, () -> codec.decode(SyntheticWire.status(0, 1, 100, 1)));
  }

  @Test public void independentGoldenTradeHasExactHeaderSideOrderAndTrailer() {
    SyntheticWire.Encoded buyer = SyntheticWire.tradeSide(filled(20, 0x11),
        BigInteger.valueOf(6), null, null, "-1");
    SyntheticWire.Encoded seller = SyntheticWire.tradeSide(filled(20, 0x22),
        BigInteger.valueOf(7), null, null, "0");
    SyntheticWire.Encoded fixture = SyntheticWire.trades(BigInteger.ONE, BigInteger.valueOf(2),
        BigInteger.valueOf(3), BigInteger.valueOf(4), BigInteger.valueOf(8),
        SyntheticWire.trade(BigInteger.valueOf(5), true, null, "H", "2", "3", "", buyer, seller));
    // Literal independent v3 vector; no production encoder/model constructs these bytes.
    byte[] golden = unhex("0b" + "0100000000000000" + "0200000000000000"
        + "0300000000000000" + "0400000000000000" + "01000000"
        + "0500000000000000" + "0100" + "0148" + "0132" + "0133" + "00"
        + "1111111111111111111111111111111111111111" + "0600000000000000"
        + "0000" + "022d31"
        + "2222222222222222222222222222222222222222" + "0700000000000000"
        + "0000" + "0130" + "0800000000000000");
    assertArrayEquals(golden, fixture.bytes);
    P2pWireCodec.TradeBatch batch = (P2pWireCodec.TradeBatch) codec.decode(golden);
    assertEquals(BigInteger.ONE, batch.sequence);
    assertEquals(BigInteger.valueOf(2), batch.localTimeMicros);
    assertEquals(BigInteger.valueOf(3), batch.blockTimeMicros);
    assertEquals(BigInteger.valueOf(4), batch.height);
    assertEquals(BigInteger.valueOf(8), batch.sentTimeMicros);
    assertEquals(1, batch.trades.size());
    P2pWireCodec.TradeEvent trade = batch.trades.get(0);
    assertEquals(BigInteger.valueOf(5), trade.eventTimeMicros);
    assertTrue(trade.buy);
    assertNull(trade.hash);
    assertEquals("H", trade.coin);
    assertEquals(new BigDecimal("2"), trade.price);
    assertEquals(new BigDecimal("3"), trade.size);
    assertEquals("", trade.direction);
    assertSide(trade.buyer, filled(20, 0x11), BigInteger.valueOf(6), null, null, "-1");
    assertSide(trade.seller, filled(20, 0x22), BigInteger.valueOf(7), null, null, "0");
  }

  @Test public void tradesPreserveAllFieldsUnsignedValuesAndDetachedImmutableData() {
    BigInteger high = BigInteger.ONE.shiftLeft(63);
    SyntheticWire.Encoded first = fullTrade("HYPE", "12345678901234567890.123456789",
        "0.000000000000000000123", "opaque override", "-123.4500", "-0.00001");
    SyntheticWire.Encoded side = SyntheticWire.tradeSide(filled(20, 0), BigInteger.ZERO,
        BigInteger.ZERO, filled(16, 0), "+0.00");
    SyntheticWire.Encoded second = SyntheticWire.trade(high, false, filled(32, 0),
        "用户", "1.00", "2.00", "任意", side, side);
    SyntheticWire.Encoded fixture = SyntheticWire.trades(P2pWireCodec.MAX_U64, high,
        P2pWireCodec.MAX_U64, high, P2pWireCodec.MAX_U64, first, second);
    byte[] bytes = SyntheticWire.extension(fixture.bytes);
    P2pWireCodec.TradeBatch batch = (P2pWireCodec.TradeBatch) codec.decode(bytes);
    assertEquals(P2pWireCodec.MAX_U64, batch.sequence);
    assertEquals(high, batch.localTimeMicros);
    assertEquals(P2pWireCodec.MAX_U64, batch.blockTimeMicros);
    assertEquals(high, batch.height);
    assertEquals(P2pWireCodec.MAX_U64, batch.sentTimeMicros);
    assertEquals(2, batch.trades.size());
    P2pWireCodec.TradeEvent trade = batch.trades.get(0);
    assertEquals(P2pWireCodec.MAX_U64, trade.eventTimeMicros);
    assertTrue(trade.buy);
    assertEquals(hex(filled(32, 0xab)), trade.hash);
    assertEquals("HYPE", trade.coin);
    assertEquals(new BigDecimal("12345678901234567890.123456789"), trade.price);
    assertEquals(new BigDecimal("0.000000000000000000123"), trade.size);
    assertEquals("opaque override", trade.direction);
    assertSide(trade.buyer, filled(20, 0x81), P2pWireCodec.MAX_U64,
        P2pWireCodec.MAX_U64, filled(16, 0xcd), "-123.4500");
    assertSide(trade.seller, filled(20, 0xfe), high, high, filled(16, 0xef), "-0.00001");
    P2pWireCodec.TradeEvent sell = batch.trades.get(1);
    assertEquals(high, sell.eventTimeMicros);
    assertFalse(sell.buy);
    assertEquals(hex(filled(32, 0)), sell.hash);
    assertEquals("用户", sell.coin);
    assertEquals("任意", sell.direction);
    assertEquals(new BigDecimal("1.00"), sell.price);
    assertEquals(new BigDecimal("2.00"), sell.size);
    assertSide(sell.buyer, filled(20, 0), BigInteger.ZERO, BigInteger.ZERO, filled(16, 0), "+0.00");
    assertSide(sell.seller, filled(20, 0), BigInteger.ZERO, BigInteger.ZERO, filled(16, 0), "+0.00");
    Arrays.fill(bytes, (byte) 0);
    assertEquals(hex(filled(32, 0xab)), trade.hash);
    assertEquals(hex(filled(20, 0x81)), trade.buyer.user);
    assertEquals(hex(filled(16, 0xcd)), trade.buyer.cloid);
    assertThrows(UnsupportedOperationException.class, () -> batch.trades.clear());
  }

  @Test public void optionalFieldsAreIndependentAndEveryTradeTruncationFails() {
    for (int mask = 0; mask < 32; mask++) {
      BigInteger buyerTwap = (mask & 2) == 0 ? null : BigInteger.ZERO;
      byte[] buyerCloid = (mask & 4) == 0 ? null : filled(16, 0);
      BigInteger sellerTwap = (mask & 8) == 0 ? null : P2pWireCodec.MAX_U64;
      byte[] sellerCloid = (mask & 16) == 0 ? null : filled(16, 0xff);
      byte[] hash = (mask & 1) == 0 ? null : filled(32, 0);
      SyntheticWire.Encoded record = SyntheticWire.trade(BigInteger.ZERO, (mask & 1) != 0,
          hash, "HYPE", "1", "2", "unknown",
          SyntheticWire.tradeSide(filled(20, 1), BigInteger.ZERO, buyerTwap, buyerCloid, "-1"),
          SyntheticWire.tradeSide(filled(20, 2), P2pWireCodec.MAX_U64, sellerTwap, sellerCloid, "2"));
      byte[] bytes = batch(record, record).bytes;
      P2pWireCodec.TradeBatch decoded = (P2pWireCodec.TradeBatch) codec.decode(bytes);
      assertEquals(2, decoded.trades.size()); // Same hash/price/size is not a deduplication key.
      for (P2pWireCodec.TradeEvent trade : decoded.trades) {
        assertEquals(BigInteger.ZERO, trade.eventTimeMicros);
        assertEquals((mask & 1) != 0, trade.buy);
        assertEquals(hash == null ? null : hex(hash), trade.hash);
        assertSide(trade.buyer, filled(20, 1), BigInteger.ZERO, buyerTwap, buyerCloid, "-1");
        assertSide(trade.seller, filled(20, 2), P2pWireCodec.MAX_U64, sellerTwap, sellerCloid, "2");
      }
      assertEveryPrefixRejected(bytes);
    }
  }

  @Test public void tradeFlagsUtf8LengthsAndLaterMalformedFieldsFailClosed() {
    SyntheticWire.Encoded fixture = batch(fullTrade("HYPE", "12.50", "3.25", "opaque", "-1", "-2"),
        fullTrade("ETH", "12.50", "3.25", "opaque", "-1", "-2"));
    for (int index = 0; index < 2; index++) {
      String prefix = "trade." + index + ".";
      for (String field : Arrays.asList("buy", "hasHash", "buyer.hasTwapId", "buyer.hasCloid",
          "seller.hasTwapId", "seller.hasCloid")) {
        rejectMutation(fixture, prefix + field, 2);
        rejectMutation(fixture, prefix + field, 255);
      }
      for (String field : Arrays.asList("coin", "price", "size", "direction",
          "buyer.startPosition", "seller.startPosition")) {
        rejectMutation(fixture, prefix + field, 255); // Invalid UTF-8, not replacement decoding.
        rejectMutation(fixture, prefix + field + ".length", 255);
      }
    }
    for (String bad : Arrays.asList("", "NaN", "Infinity", " 1", "1_0", "1e2147483647", "1e1025")) {
      assertBadTrade(fullTrade("HYPE", bad, "1", "x", "-1", "-2"));
      assertBadTrade(fullTrade("HYPE", "1", bad, "x", "-1", "-2"));
      assertBadTrade(fullTrade("HYPE", "1", "1", "x", bad, "-2"));
      assertBadTrade(fullTrade("HYPE", "1", "1", "x", "-1", bad));
    }
    assertBadTrade(fullTrade("", "1", "1", "", "-1", "-2"));
    assertBadTrade(fullTrade("HYPE", "-1", "1", "", "-1", "-2"));
    assertBadTrade(fullTrade("HYPE", "1", "-1", "", "-1", "-2"));
  }

  @Test public void tradeCountsSequenceFrameCapsAndEmptyBatchesAreBounded() {
    SyntheticWire.Encoded trade = fullTrade("HYPE", "1", "2", "", "-1", "-2");
    byte[] empty = batch().bytes;
    assertEquals(0, ((P2pWireCodec.TradeBatch) codec.decode(empty)).trades.size());
    assertEveryPrefixRejected(empty);
    SyntheticWire.Encoded pair = batch(trade, trade);
    P2pWireCodec atLimit = new P2pWireCodec(pair.bytes.length, pair.bytes.length, 2);
    assertEquals(2, ((P2pWireCodec.TradeBatch) atLimit.decode(pair.bytes)).trades.size());
    assertThrows(IllegalArgumentException.class,
        () -> atLimit.decode(Arrays.copyOf(pair.bytes, pair.bytes.length + 1)));
    P2pWireCodec one = new P2pWireCodec(4096, 4096, 1);
    assertThrows(IllegalArgumentException.class, () -> one.decode(pair.bytes));
    byte[] impossible = empty.clone();
    impossible[33] = 1;
    assertThrows(IllegalArgumentException.class, () -> codec.decode(impossible));
    byte[] huge = pair.bytes.clone();
    Arrays.fill(huge, pair.offset("count"), pair.offset("count") + 4, (byte) 255);
    assertThrows(IllegalArgumentException.class, () -> codec.decode(huge));
    byte[] zeroSequence = pair.bytes.clone();
    Arrays.fill(zeroSequence, 1, 9, (byte) 0);
    assertThrows(IllegalArgumentException.class, () -> codec.decode(zeroSequence));
    assertEquals(0, ((P2pWireCodec.TradeBatch) codec.decode(SyntheticWire.extension(empty))).trades.size());
  }

  @Test public void blockUsesVerifiedFixedOffsetsAndFinalSentTrailerWithoutSequence() {
    SyntheticWire.Encoded fixture = SyntheticWire.block(BigInteger.ONE, BigInteger.valueOf(2),
        BigInteger.valueOf(3), BigInteger.valueOf(4), BigInteger.valueOf(5));
    byte[] golden = unhex("00" + "0100000000000000" + "0200000000000000"
        + "0300000000000000" + "0400000000000000" + "0500000000000000");
    assertArrayEquals(golden, fixture.bytes);
    assertEquals(41, golden.length);
    assertEquals(1, fixture.offset("blockTimeMillis"));
    assertEquals(9, fixture.offset("height"));
    assertEquals(17, fixture.offset("wallTimeMicros"));
    assertEquals(25, fixture.offset("applyDurationMicros"));
    assertEquals(33, fixture.offset("sentTimeMicros"));
    for (byte[] bytes : Arrays.asList(golden, SyntheticWire.extension(golden))) {
      P2pWireCodec.Block block = (P2pWireCodec.Block) codec.decode(bytes);
      assertEquals(BigInteger.ONE, block.blockTimeMillis);
      assertEquals(BigInteger.valueOf(2), block.height);
      assertEquals(BigInteger.valueOf(3), block.wallTimeMicros);
      assertEquals(BigInteger.valueOf(4), block.applyDurationMicros);
      assertEquals(BigInteger.valueOf(5), block.sentTimeMicros);
    }
    assertEveryPrefixRejected(golden);
    for (BigInteger value : Arrays.asList(BigInteger.ZERO, BigInteger.ONE.shiftLeft(63), P2pWireCodec.MAX_U64)) {
      P2pWireCodec.Block block = (P2pWireCodec.Block) codec.decode(
          SyntheticWire.block(value, value, value, value, value).bytes);
      assertEquals(value, block.blockTimeMillis);
      assertEquals(value, block.height);
      assertEquals(value, block.wallTimeMicros);
      assertEquals(value, block.applyDurationMicros);
      assertEquals(value, block.sentTimeMicros);
    }
    // Block is metadata, not a Diff/Status or book-completion delimiter; service tests own state effects.
    assertFalse(codec.decode(golden) instanceof P2pWireCodec.Diff);
    assertFalse(codec.decode(golden) instanceof P2pWireCodec.Status);
  }

  private static SyntheticWire.Encoded fullTrade(String coin, String price, String size,
      String direction, String buyerPosition, String sellerPosition) {
    BigInteger high = BigInteger.ONE.shiftLeft(63);
    return SyntheticWire.trade(P2pWireCodec.MAX_U64, true, filled(32, 0xab), coin, price, size, direction,
        SyntheticWire.tradeSide(filled(20, 0x81), P2pWireCodec.MAX_U64, P2pWireCodec.MAX_U64,
            filled(16, 0xcd), buyerPosition),
        SyntheticWire.tradeSide(filled(20, 0xfe), high, high, filled(16, 0xef), sellerPosition));
  }
  private static SyntheticWire.Encoded batch(SyntheticWire.Encoded... trades) {
    return SyntheticWire.trades(BigInteger.ONE, BigInteger.valueOf(1000001),
        BigInteger.valueOf(1000000), BigInteger.valueOf(101), BigInteger.valueOf(1000002), trades);
  }
  private void assertBadTrade(SyntheticWire.Encoded malformed) {
    byte[] bytes = batch(fullTrade("HYPE", "1", "2", "", "-1", "-2"), malformed).bytes;
    assertThrows(IllegalArgumentException.class, () -> codec.decode(bytes));
  }
  private void assertEveryPrefixRejected(byte[] bytes) {
    for (int length = 0; length < bytes.length; length++) {
      byte[] truncated = Arrays.copyOf(bytes, length);
      assertThrows("truncated length " + length, IllegalArgumentException.class, () -> codec.decode(truncated));
    }
  }
  private void rejectMutation(SyntheticWire.Encoded fixture, String field, int value) {
    byte[] bytes = fixture.bytes.clone();
    bytes[fixture.offset(field)] = (byte) value;
    assertThrows(field + "=" + value, IllegalArgumentException.class, () -> codec.decode(bytes));
  }
  private static void assertSide(P2pWireCodec.TradeSide side, byte[] user, BigInteger oid,
      BigInteger twapId, byte[] cloid, String position) {
    // Lossless immutable lowercase hex, no 0x prefix; absent optional bytes remain null.
    assertEquals(hex(user), side.user);
    assertEquals(oid, side.oid);
    assertEquals(twapId, side.twapId);
    assertEquals(cloid == null ? null : hex(cloid), side.cloid);
    assertEquals(new BigDecimal(position), side.startPosition);
  }
  private static byte[] filled(int length, int value) {
    byte[] bytes = new byte[length];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }
  private static String hex(byte[] bytes) {
    StringBuilder text = new StringBuilder();
    for (byte value : bytes) {
      text.append(Character.forDigit((value & 255) >>> 4, 16));
      text.append(Character.forDigit(value & 15, 16));
    }
    return text.toString();
  }
  private static byte[] unhex(String text) {
    byte[] bytes = new byte[text.length() / 2];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) ((Character.digit(text.charAt(i * 2), 16) << 4)
          | Character.digit(text.charAt(i * 2 + 1), 16));
    }
    return bytes;
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
