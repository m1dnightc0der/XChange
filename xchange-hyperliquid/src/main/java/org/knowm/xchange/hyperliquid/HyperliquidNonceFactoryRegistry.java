package org.knowm.xchange.hyperliquid;

import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.knowm.xchange.utils.nonce.CurrentTimeIncrementalNonceFactory;
import org.web3j.crypto.Credentials;
import si.mazi.rescu.SynchronizedValueFactory;

final class HyperliquidNonceFactoryRegistry {

  private static final ConcurrentMap<String, SynchronizedValueFactory<Long>> BY_SIGNER =
      new ConcurrentHashMap<>();

  private HyperliquidNonceFactoryRegistry() {}

  static SynchronizedValueFactory<Long> forSecretKey(String secretKey) {
    String signerAddress =
        Credentials.create(secretKey.trim()).getAddress().toLowerCase(Locale.ROOT);
    return BY_SIGNER.computeIfAbsent(
        signerAddress,
        ignored -> new CurrentTimeIncrementalNonceFactory(TimeUnit.MILLISECONDS));
  }

  static void clearForTests() {
    BY_SIGNER.clear();
  }
}
