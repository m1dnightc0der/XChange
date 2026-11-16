package info.bitrich.xchangestream.hyperliquid.p2p;

import java.io.IOException;

/** Signals that a validated trade-retention boundary has advanced the resume cursor. */
public final class P2pTradeRetentionRecoveryException extends IOException {
  public P2pTradeRetentionRecoveryException() {
    super("P2P trade retention cursor advanced");
  }
}
