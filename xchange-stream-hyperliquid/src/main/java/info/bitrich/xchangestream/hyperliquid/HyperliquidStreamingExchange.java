package info.bitrich.xchangestream.hyperliquid;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.core.StreamingTradeService;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.reactivex.rxjava3.core.Completable;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.hyperliquid.HyperliquidExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HyperliquidStreamingExchange extends HyperliquidExchange implements StreamingExchange {
  private final Logger LOG = LoggerFactory.getLogger(this.getClass());
  
  // Production URI
  public static final String WS_MAINNET_URI = "wss://api.hyperliquid.xyz/ws";
  
  // Testnet URI
  public static final String WS_TESTNET_URI = "wss://api.hyperliquid-testnet.xyz/ws";

  private HyperliquidStreamingService streamingService;
  private HyperliquidStreamingMarketDataService streamingMarketDataService;
  private HyperliquidStreamingTradeService streamingTradeService;



  @Override
  public Completable connect(ProductSubscription... args) {
    LOG.debug("HyperliquidStreamingExchange - connect called from: {}", Thread.currentThread().getStackTrace()[2]);

    this.streamingService = new HyperliquidStreamingService(getApiUrl(), this.exchangeSpecification);
    this.streamingMarketDataService = new HyperliquidStreamingMarketDataService(streamingService);

    // Initialize trade service with authentication if private key is available
    if (exchangeSpecification.getSecretKey() != null && !exchangeSpecification.getSecretKey().isEmpty()) {
      // Create nonce factory for signing
      si.mazi.rescu.SynchronizedValueFactory<Long> nonceFactory =
          new org.knowm.xchange.utils.nonce.AtomicLongIncrementalTime2014NonceFactory();

      org.knowm.xchange.hyperliquid.service.HyperliquidAuth hyperliquidAuth =
          org.knowm.xchange.hyperliquid.service.HyperliquidAuth.createHyperliquidAuth(
              exchangeSpecification.getSecretKey(),
              nonceFactory,
              !useSandbox(), // isMainnet
              (String) exchangeSpecification.getExchangeSpecificParametersItem("vault"), (String) exchangeSpecification.getExchangeSpecificParametersItem("wallet")
          );

      // Create metadata loader using the info API with REST endpoint
      // Convert WebSocket URL to REST API URL
      String restApiUrl = getRestApiUrl();
      ExchangeSpecification restSpec = new ExchangeSpecification(this.getClass());
      restSpec.setSslUri(restApiUrl);
      restSpec.setHost(restApiUrl.replace("https://", "").replace("/", ""));

      org.knowm.xchange.hyperliquid.HyperliquidInfo hyperliquidInfo =
          org.knowm.xchange.client.ExchangeRestProxyBuilder.forInterface(
              org.knowm.xchange.hyperliquid.HyperliquidInfo.class,
              restSpec
          ).build();
      org.knowm.xchange.hyperliquid.service.HyperliquidMetadataLoader metadataLoader =
          new org.knowm.xchange.hyperliquid.service.HyperliquidMetadataLoader(hyperliquidInfo);

      this.streamingTradeService = new HyperliquidStreamingTradeService(
          streamingService,
          hyperliquidAuth,
          metadataLoader
      );
    }

    return streamingService.connect();
  }

  private String getApiUrl() {
    ExchangeSpecification exchangeSpec = getExchangeSpecification();
    if (exchangeSpec.getOverrideWebsocketApiUri() != null) {
      return exchangeSpec.getOverrideWebsocketApiUri();
    }

    return useSandbox() ? WS_TESTNET_URI : WS_MAINNET_URI;
  }

  /**
   * Get REST API URL based on sandbox setting
   * Converts WebSocket URL to corresponding REST API URL
   */
  private String getRestApiUrl() {
    // Map WebSocket URLs to REST API URLs
    if (useSandbox()) {
      return "https://api.hyperliquid-testnet.xyz/";
    } else {
      return "https://api.hyperliquid.xyz/";
    }
  }

  @Override
  public Completable disconnect() {
    if (streamingService != null) {
      return streamingService.disconnect();
    }
    return Completable.complete();
  }

  @Override
  public boolean isAlive() {
    return streamingService != null && streamingService.isSocketOpen();
  }

  @Override
  public StreamingMarketDataService getStreamingMarketDataService() {
    return streamingMarketDataService;
  }

  @Override
  public StreamingTradeService getStreamingTradeService() {
    if (streamingTradeService == null) {
      throw new NotYetImplementedForExchangeException(
          "StreamingTradeService requires authentication - set API key in ExchangeSpecification");
    }
    return streamingTradeService;
  }

  @Override
  public void useCompressedMessages(boolean compressedMessages) {
    throw new NotYetImplementedForExchangeException("useCompressedMessage");
  }

  @Override
  public ExchangeSpecification getDefaultExchangeSpecification() {
    ExchangeSpecification spec = super.getDefaultExchangeSpecification();
    // Override WebSocket-specific settings
    spec.setSslUri(WS_MAINNET_URI);
    spec.setExchangeDescription("Hyperliquid Streaming API");
    return spec;
  }

  /**
   * Enables the user to listen on channel inactive events and react appropriately.
   *
   * @param channelInactiveHandler a WebSocketMessageHandler instance.
   */
  public void setChannelInactiveHandler(
      WebSocketClientHandler.WebSocketMessageHandler channelInactiveHandler) {
    if (streamingService != null) {
      streamingService.setChannelInactiveHandler(channelInactiveHandler);
    }
  }

  /**
   * Set the vault address for trading on behalf of a vault.
   *
   * <p>This is a convenience method that sets the vaultAddress parameter in the exchange specification.
   * Must be called before {@link #connect()} to take effect.</p>
   *
   * <p>When a vault address is configured, all trading operations will execute on behalf of the vault
   * instead of your personal account. You must be the vault leader or have permission to trade on the vault.</p>
   *
   * <h3>Example Usage</h3>
   * <pre>
   * HyperliquidStreamingExchange exchange = new HyperliquidStreamingExchange();
   * ExchangeSpecification spec = exchange.getDefaultExchangeSpecification();
   * spec.setSecretKey("0x...");
   * exchange.applySpecification(spec);
   * exchange.setVaultAddress("0x0f0d9105b88e938df7816b23e7ad4d1660568a0e");
   * exchange.connect().blockingAwait();
   * </pre>
   *
   * @param vaultAddress Vault address (0x + 40 hex characters)
   * @throws IllegalArgumentException if the vault address format is invalid
   * @see #getVaultAddress()
   */
  public void setVaultAddress(String vaultAddress) {
    // Validate format
    if (vaultAddress != null && !vaultAddress.isEmpty()) {
      if (!vaultAddress.matches("^0x[a-fA-F0-9]{40}$")) {
        throw new IllegalArgumentException(
            "Invalid vault address format: " + vaultAddress +
            ". Must be 0x followed by 40 hexadecimal characters."
        );
      }
    }
    exchangeSpecification.setExchangeSpecificParametersItem("vault", vaultAddress);
  }

  /**
   * Get the configured vault address.
   *
   * <p>Returns the vault address that was configured for this exchange instance.
   * Returns null if no vault address is configured (personal account trading).</p>
   *
   * @return Vault address, or null if not configured
   * @see #setVaultAddress(String)
   */
  public String getVaultAddress() {
    return (String) exchangeSpecification.getExchangeSpecificParametersItem("vaultAddress");
  }
}