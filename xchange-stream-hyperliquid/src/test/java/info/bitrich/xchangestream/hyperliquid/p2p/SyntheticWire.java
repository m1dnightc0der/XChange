package info.bitrich.xchangestream.hyperliquid.p2p;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Hand-written synthetic protocol fixtures. No provider samples, HTTP, or live feeds. */
final class SyntheticWire {
  static P2pWireCodec.Change order(long oid, boolean bid, String coin, String price, String size) {
    return change(BigInteger.valueOf(oid), bid, true, coin, price, size);
  }
  static P2pWireCodec.Change change(BigInteger oid, boolean bid, boolean present,
      String coin, String price, String size) {
    return new P2pWireCodec.Change(oid, bid, present, coin, new BigDecimal(price),
        new BigDecimal(size), "user", null);
  }
  static byte[] diff(long sequence, long height, P2pWireCodec.Change... orders) {
    Bytes out = new Bytes().u8(6).u64(sequence).u64(1_000_001).u64(1_000_000)
        .u64(height).u32(orders.length);
    for (P2pWireCodec.Change order : orders) {
      out.u64(order.oid).u8(order.bid ? 1 : 0).u8(order.present ? 1 : 0)
          .text(order.coin).text(order.price.toPlainString()).text(order.size.toPlainString())
          .text(order.user).u8(order.insertBefore == null ? 0 : 1);
      if (order.insertBefore != null) out.u64(order.insertBefore);
    }
    return out.u64(1_000_002).bytes();
  }
  static byte[] status(int stream, int state, long height, long toHeight) {
    return new Bytes().u8(13).u8(stream).u8(state).u64(height).u64(toHeight).u64(1_000_002).bytes();
  }
  static byte[] snapshot(long height, Map<String, List<P2pWireCodec.Change>> books) {
    Bytes body = new Bytes();
    books.forEach((coin, orders) -> {
      List<P2pWireCodec.Change> bids = new ArrayList<>();
      List<P2pWireCodec.Change> asks = new ArrayList<>();
      orders.forEach(order -> (order.bid ? bids : asks).add(order));
      Bytes frame = new Bytes().u8(12).u64(0).u64(height).u64(0).text(coin)
          .u32(bids.size()).u32(asks.size());
      List<P2pWireCodec.Change> sorted = new ArrayList<>(bids);
      sorted.addAll(asks);
      for (P2pWireCodec.Change order : sorted) frame.u64(order.oid).u8(order.bid ? 1 : 0)
          .text(order.price.toPlainString()).text(order.size.toPlainString()).text(order.user);
      body.u32(frame.bytes().length).raw(frame.bytes());
    });
    return body.bytes();
  }
  static P2pWireCodec.Snapshot seed(long height, P2pWireCodec.Change... orders) {
    Map<String, List<P2pWireCodec.Change>> books = new LinkedHashMap<>();
    books.put("BTC", Arrays.asList(orders));
    return new P2pWireCodec.Snapshot(BigInteger.valueOf(height), books);
  }
  static byte[] seedBytes(long height, P2pWireCodec.Change... orders) {
    return snapshot(height, Collections.singletonMap("BTC", Arrays.asList(orders)));
  }
  static byte[] extension(byte[] original) {
    byte[] extended = new byte[original.length + 3];
    System.arraycopy(original, 0, extended, 0, original.length - 8);
    extended[original.length - 8] = 99;
    System.arraycopy(original, original.length - 8, extended, original.length - 5, 8);
    return extended;
  }
  // Independent wire inputs: no production TradeBatch/TradeEvent/TradeSide constructors.
  static Encoded tradeSide(byte[] user, BigInteger oid, BigInteger twapId,
      byte[] cloid, String startPosition) {
    Fields out = new Fields().raw("user", user).u64("oid", oid)
        .u8("hasTwapId", twapId == null ? 0 : 1);
    if (twapId != null) out.u64("twapId", twapId);
    out.u8("hasCloid", cloid == null ? 0 : 1);
    if (cloid != null) out.raw("cloid", cloid);
    return out.text("startPosition", startPosition).finish();
  }
  static Encoded trade(BigInteger eventTimeMicros, boolean buy, byte[] hash,
      String coin, String price, String size, String direction, Encoded buyer, Encoded seller) {
    Fields out = new Fields().u64("eventTimeMicros", eventTimeMicros)
        .u8("buy", buy ? 1 : 0).u8("hasHash", hash == null ? 0 : 1);
    if (hash != null) out.raw("hash", hash);
    return out.text("coin", coin).text("price", price).text("size", size)
        .text("direction", direction).part("buyer", buyer).part("seller", seller).finish();
  }
  static Encoded trades(BigInteger sequence, BigInteger localTimeMicros,
      BigInteger blockTimeMicros, BigInteger height, BigInteger sentTimeMicros, Encoded... trades) {
    Fields out = new Fields().u8("tag", 11).u64("sequence", sequence)
        .u64("localTimeMicros", localTimeMicros).u64("blockTimeMicros", blockTimeMicros)
        .u64("height", height).u32("count", trades.length);
    for (int i = 0; i < trades.length; i++) out.part("trade." + i, trades[i]);
    return out.u64("sentTimeMicros", sentTimeMicros).finish();
  }
  static Encoded block(BigInteger blockTimeMillis, BigInteger height, BigInteger wallTimeMicros,
      BigInteger applyDurationMicros, BigInteger sentTimeMicros) {
    // Vendor v3 fixed body is 33 bytes, followed by optional extensions and the sent trailer.
    return new Fields().u8("tag", 0).u64("blockTimeMillis", blockTimeMillis)
        .u64("height", height).u64("wallTimeMicros", wallTimeMicros)
        .u64("applyDurationMicros", applyDurationMicros).u64("sentTimeMicros", sentTimeMicros)
        .finish();
  }
  static final class Encoded {
    final byte[] bytes;
    final Map<String, Integer> offsets;
    Encoded(byte[] bytes, Map<String, Integer> offsets) {
      this.bytes = bytes;
      this.offsets = Collections.unmodifiableMap(new LinkedHashMap<>(offsets));
    }
    int offset(String field) {
      Integer offset = offsets.get(field);
      if (offset == null) throw new IllegalArgumentException("Unknown fixture field: " + field);
      return offset;
    }
  }
  private static final class Fields {
    final Bytes out = new Bytes();
    final Map<String, Integer> offsets = new LinkedHashMap<>();
    void mark(String field) { offsets.put(field, out.out.size()); }
    Fields u8(String field, int value) { mark(field); out.u8(value); return this; }
    Fields u32(String field, long value) { mark(field); out.u32(value); return this; }
    Fields u64(String field, BigInteger value) { mark(field); out.u64(value); return this; }
    Fields raw(String field, byte[] value) { mark(field); out.raw(value); return this; }
    Fields text(String field, String value) {
      mark(field + ".length");
      offsets.put(field, out.out.size() + 1);
      out.text(value);
      return this;
    }
    Fields part(String field, Encoded value) {
      int base = out.out.size();
      value.offsets.forEach((name, offset) -> offsets.put(field + "." + name, base + offset));
      out.raw(value.bytes);
      return this;
    }
    Encoded finish() { return new Encoded(out.bytes(), offsets); }
  }
  static final class Bytes {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    Bytes u8(int value) { out.write(value); return this; }
    Bytes u32(long value) { for (int i = 0; i < 4; i++) out.write((int) (value >>> (8 * i))); return this; }
    Bytes u64(long value) { return u64(BigInteger.valueOf(value)); }
    Bytes u64(BigInteger value) {
      for (int i = 0; i < 8; i++) out.write(value.shiftRight(8 * i).intValue() & 255);
      return this;
    }
    Bytes text(String value) {
      byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
      if (encoded.length > 255) throw new IllegalArgumentException();
      return u8(encoded.length).raw(encoded);
    }
    Bytes raw(byte[] bytes) { out.write(bytes, 0, bytes.length); return this; }
    byte[] bytes() { return out.toByteArray(); }
  }
}
