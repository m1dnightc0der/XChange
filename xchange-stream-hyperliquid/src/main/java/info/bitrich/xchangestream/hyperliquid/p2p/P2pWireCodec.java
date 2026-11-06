package info.bitrich.xchangestream.hyperliquid.p2p;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Strict, bounded v3 little-endian codec. Unknown tags and length-delimited extensions are ignored. */
public final class P2pWireCodec {
  public static final BigInteger MAX_U64 = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
  public static final int DEFAULT_MAX_FRAME_BYTES = 16 * 1024 * 1024;
  public static final int DEFAULT_MAX_SNAPSHOT_BYTES = 128 * 1024 * 1024;
  private final int maxFrameBytes;
  private final int maxSnapshotBytes;
  private final int maxOrders;

  public P2pWireCodec() {
    this(DEFAULT_MAX_FRAME_BYTES, DEFAULT_MAX_SNAPSHOT_BYTES, 1_000_000);
  }

  public P2pWireCodec(int maxFrameBytes, int maxSnapshotBytes, int maxOrders) {
    if (maxFrameBytes < 45 || maxSnapshotBytes < maxFrameBytes || maxOrders < 1) {
      throw new IllegalArgumentException("Invalid wire limits");
    }
    this.maxFrameBytes = maxFrameBytes;
    this.maxSnapshotBytes = maxSnapshotBytes;
    this.maxOrders = maxOrders;
  }

  public int getMaxFrameBytes() { return maxFrameBytes; }
  public int getMaxSnapshotBytes() { return maxSnapshotBytes; }

  public Frame decode(byte[] bytes) {
    if (bytes == null || bytes.length == 0 || bytes.length > maxFrameBytes) {
      throw malformed("Invalid WebSocket message length");
    }
    int tag = bytes[0] & 255;
    if (tag == 12) throw malformed("Snapshot frames are HTTP-only");
    if ((tag == 0 || tag == 10 || tag == 11) && bytes.length < 9) {
      throw malformed("Missing channel sent timestamp");
    }
    if (tag != 6 && tag != 13) {
      // No book state is inferred from block, trade, metric, or future tags.
      return new Ignored(tag);
    }
    if (bytes.length < 9) throw malformed("Missing sent timestamp");
    Reader r = new Reader(bytes, 1, bytes.length - 8);
    BigInteger sent = new Reader(bytes, bytes.length - 8, bytes.length).u64();
    if (tag == 13) {
      int stream = r.u8();
      int state = r.u8();
      BigInteger height = r.u64();
      BigInteger to = r.u64();
      if (state > 2 || (state != 2 && to.signum() != 0)
          || (state == 2 && to.compareTo(height) < 0)) throw malformed("Invalid status");
      return new Status(stream, state, height, to, sent);
    }
    BigInteger seq = r.u64();
    if (seq.signum() == 0) throw malformed("Book sequence starts at one");
    BigInteger localTime = r.u64();
    BigInteger blockTime = r.u64();
    BigInteger height = r.u64();
    int count = r.count(maxOrders, 15);
    List<Change> changes = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      BigInteger oid = r.u64();
      boolean bid = r.flag();
      boolean present = r.flag();
      String coin = r.string();
      BigDecimal price = r.decimal();
      BigDecimal size = r.decimal();
      String user = r.string();
      BigInteger before = r.flag() ? r.u64() : null;
      if (coin.isEmpty() || price.signum() < 0 || size.signum() < 0) {
        throw malformed("Invalid change values");
      }
      changes.add(new Change(oid, bid, present, coin, price, size, user, before));
    }
    return new Diff(seq, localTime, blockTime, height, sent, changes);
  }

  public Snapshot decodeSnapshot(byte[] bytes, Set<String> coins, BigInteger headerHeight,
      BigInteger minHeight) {
    unsigned(headerHeight);
    unsigned(minHeight);
    if (bytes == null || bytes.length == 0 || bytes.length > maxSnapshotBytes
        || coins == null || coins.isEmpty() || coins.size() > 512
        || headerHeight.compareTo(minHeight) < 0) throw malformed("Invalid snapshot envelope");
    Reader body = new Reader(bytes, 0, bytes.length);
    Map<String, List<Change>> books = new LinkedHashMap<>();
    Set<BigInteger> ids = new HashSet<>();
    long total = 0;
    while (body.remaining() > 0) {
      long length = body.u32();
      if (length == 0 || length > maxFrameBytes || length > body.remaining()) {
        throw malformed("Invalid snapshot frame length");
      }
      Reader r = new Reader(bytes, body.position, body.position + (int) length);
      body.position += (int) length;
      if (r.u8() != 12 || r.u64().signum() != 0 || !r.u64().equals(headerHeight)
          || r.u64().signum() != 0) throw malformed("Invalid snapshot header");
      String coin = r.string();
      if (!coins.contains(coin) || books.containsKey(coin)) throw malformed("Snapshot membership");
      long bids = r.u32();
      long asks = r.u32();
      long count = bids + asks;
      total += count;
      if (count > maxOrders || total > maxOrders || count > r.remaining() / 12) {
        throw malformed("Snapshot order count limit");
      }
      List<Change> orders = new ArrayList<>();
      for (long i = 0; i < count; i++) {
        BigInteger oid = r.u64();
        boolean bid = r.flag();
        BigDecimal price = r.decimal();
        BigDecimal size = r.decimal();
        String user = r.string();
        if (bid != (i < bids) || !ids.add(oid) || price.signum() <= 0 || size.signum() < 0) {
          throw malformed("Invalid snapshot order");
        }
        orders.add(new Change(oid, bid, true, coin, price, size, user, null));
      }
      books.put(coin, Collections.unmodifiableList(orders));
    }
    if (!books.keySet().equals(coins)) throw malformed("Incomplete snapshot membership");
    return new Snapshot(headerHeight, books);
  }

  static BigInteger unsigned(BigInteger value) {
    if (value == null || value.signum() < 0 || value.compareTo(MAX_U64) > 0) {
      throw malformed("Unsigned 64-bit value out of range");
    }
    return value;
  }

  static IllegalArgumentException malformed(String message) {
    return new IllegalArgumentException(message);
  }

  public interface Frame {}

  public static final class Ignored implements Frame {
    public final int tag;
    Ignored(int tag) { this.tag = tag; }
  }

  public static final class Change {
    public final BigInteger oid;
    public final boolean bid;
    public final boolean present;
    public final String coin;
    public final BigDecimal price;
    public final BigDecimal size;
    public final String user;
    public final BigInteger insertBefore;
    Change(BigInteger oid, boolean bid, boolean present, String coin, BigDecimal price,
        BigDecimal size, String user, BigInteger insertBefore) {
      this.oid = oid;
      this.bid = bid;
      this.present = present;
      this.coin = coin;
      this.price = price;
      this.size = size;
      this.user = user;
      this.insertBefore = insertBefore;
    }
  }

  public static final class Diff implements Frame {
    public final BigInteger sequence;
    public final BigInteger localTimeMicros;
    public final BigInteger blockTimeMicros;
    public final BigInteger height;
    public final BigInteger sentTimeMicros;
    public final List<Change> changes;
    Diff(BigInteger sequence, BigInteger localTimeMicros, BigInteger blockTimeMicros,
        BigInteger height, BigInteger sentTimeMicros, List<Change> changes) {
      this.sequence = sequence;
      this.localTimeMicros = localTimeMicros;
      this.blockTimeMicros = blockTimeMicros;
      this.height = height;
      this.sentTimeMicros = sentTimeMicros;
      this.changes = Collections.unmodifiableList(new ArrayList<>(changes));
    }
  }

  public static final class Status implements Frame {
    public final int stream;
    public final int state;
    public final BigInteger height;
    public final BigInteger toHeight;
    public final BigInteger sentTimeMicros;
    Status(int stream, int state, BigInteger height, BigInteger toHeight, BigInteger sentTimeMicros) {
      this.stream = stream;
      this.state = state;
      this.height = height;
      this.toHeight = toHeight;
      this.sentTimeMicros = sentTimeMicros;
    }
  }

  public static final class Snapshot {
    public final BigInteger height;
    public final Map<String, List<Change>> books;
    Snapshot(BigInteger height, Map<String, List<Change>> books) {
      this.height = unsigned(height);
      Map<String, List<Change>> copy = new LinkedHashMap<>();
      books.forEach((coin, orders) -> copy.put(coin,
          Collections.unmodifiableList(new ArrayList<>(orders))));
      this.books = Collections.unmodifiableMap(copy);
    }
  }

  private static final class Reader {
    final byte[] bytes;
    final int end;
    int position;
    Reader(byte[] bytes, int position, int end) {
      this.bytes = bytes;
      this.position = position;
      this.end = end;
    }
    int remaining() { return end - position; }
    void need(int n) {
      if (n < 0 || n > remaining()) throw malformed("Truncated v3 frame");
    }
    int u8() { need(1); return bytes[position++] & 255; }
    boolean flag() {
      int flag = u8();
      if (flag > 1) throw malformed("Invalid boolean flag");
      return flag == 1;
    }
    long u32() {
      need(4);
      long value = 0;
      for (int i = 0; i < 4; i++) value |= (long) u8() << (8 * i);
      return value;
    }
    BigInteger u64() {
      need(8);
      byte[] bigEndian = new byte[8];
      for (int i = 7; i >= 0; i--) bigEndian[i] = bytes[position++];
      return new BigInteger(1, bigEndian);
    }
    int count(int max, int minimumBytes) {
      long n = u32();
      if (n > max || n > remaining() / minimumBytes) throw malformed("Order count limit");
      return (int) n;
    }
    String string() {
      int length = u8();
      need(length);
      try {
        String value = StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(bytes, position, length)).toString();
        position += length;
        return value;
      } catch (CharacterCodingException e) {
        throw malformed("Malformed UTF-8");
      }
    }
    BigDecimal decimal() {
      String text = string();
      // A decimal is textual, not a binary float. Bound exponents to prevent arithmetic DoS.
      if (!text.matches("[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?")) {
        throw malformed("Invalid decimal");
      }
      try {
        BigDecimal value = new BigDecimal(text);
        if (Math.abs((long) value.scale()) > 1024 || value.precision() > 255) {
          throw malformed("Decimal magnitude limit");
        }
        return value;
      } catch (NumberFormatException e) {
        throw malformed("Invalid decimal");
      }
    }
  }
}
