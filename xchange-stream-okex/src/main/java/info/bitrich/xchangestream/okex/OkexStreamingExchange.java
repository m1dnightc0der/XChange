package info.bitrich.xchangestream.okex;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.core.StreamingTradeService;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.reactivex.rxjava3.core.Completable;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.okex.OkexExchange;

public class OkexStreamingExchange extends OkexExchange implements StreamingExchange {
  // Production URIs
  public static final String WS_PUBLIC_URI="wss://ws.okx.com:8443";
  public static final String WS_PUBLIC_PATH = "/ws/v5/public";
  public static final String WS_PRIVATE_URI = "wss://ws.okx.com:8443";
  public static final String WS_PRIVATE_PATH = "/ws/v5/private";
  public static final String AWS_WS_PUBLIC_URI = "wss://wsaws.okx.com:8443";

  public static final String AWS_WS_PUBLIC_PATH = "/ws/v5/public";
  public static final String AWS_WS_PRIVATE_URI = "wss://wsaws.okx.com:8443";

  public static final String AWS_WS_PRIVATE_PATH = "/ws/v5/private";

  // Demo(Sandbox) URIs
  public static final String SANDBOX_WS_PUBLIC_URI =
      "wss://wspap.okx.com:8443";
  public static final String SANDBOX_WS_PUBLIC_PATH =
          "/ws/v5/public?brokerId=9999";
  public static final String SANDBOX_WS_PRIVATE_URI =
      "wss://wspap.okx.com:8443";
  public static final String SANDBOX_WS_PRIVATE_PATH =
          "/ws/v5/private?brokerId=9999";
  private OkexStreamingService streamingService;

  private OkexStreamingMarketDataService streamingMarketDataService;

  private OkexStreamingTradeService streamingTradeService;

  public OkexStreamingExchange() {}

  @Override
  public void applySpecification(ExchangeSpecification exchangeSpecification) {
    super.applySpecification(exchangeSpecification);

    // After parent initialization, ensure we have exchangeMetaData loaded
    // This is critical for proper order/position conversion in streaming services
    // Note: BaseExchange.applySpecification() calls remoteInit() if shouldLoadRemoteMetaData is true,
    // but we add this check to ensure metadata is available before streaming services are created
  }

  /**
   * FIX Issue 8: Initialize streaming services once during exchange initialization (Bybit pattern).
   * This prevents creating new service instances on every reconnection, which caused thread leaks.
   * See: ISSUE_8_OOM_BUG_CONFIRMED.md
   */
  @Override
  protected void initServices() {
    super.initServices();

    // Ensure exchangeMetaData is loaded before creating streaming services
    // This is critical for proper contract size to volume conversion on SWAP instruments
    boolean needsMetadata = exchangeMetaData == null ||
                           exchangeMetaData.getInstruments() == null ||
                           exchangeMetaData.getInstruments().isEmpty();

    if (needsMetadata) {
      // Attempt to load metadata if not already loaded
      try {
        logger.info("Exchange metadata not loaded, calling remoteInit() to fetch from API");
        remoteInit();
      } catch (Exception e) {
        // If remoteInit fails, warn but continue
        // Services will work without metadata, but contract conversions will be disabled
        logger.warn("Failed to load exchange metadata during initServices. " +
                   "Contract size conversions will be disabled. Error: {}", e.getMessage());
      }
    }

    // Create streaming services ONCE during initialization
    this.streamingService = new OkexStreamingService(getApiUrl(), this.exchangeSpecification);
    this.streamingMarketDataService = new OkexStreamingMarketDataService(streamingService);
    this.streamingTradeService = new OkexStreamingTradeService(
        streamingService,
        exchangeMetaData,
        getInstrumentCodeMap()
    );
  }

  /**
   * FIX Issue 8: Simplified to just connect using existing streaming service.
   * Service is created once in initServices(), not recreated on every connect() call.
   */
  @Override
  public Completable connect(ProductSubscription... args) {
    // Just connect - services already created in initServices()
    return streamingService.connect();
  }

  private String getApiUrl() {
    String apiUrl;
    ExchangeSpecification exchangeSpec = getExchangeSpecification();
    if (exchangeSpec.getOverrideWebsocketApiUri() != null) {

      apiUrl =
              (this.exchangeSpecification.getApiKey() == null)
                      ? exchangeSpec.getOverrideWebsocketApiUri()+WS_PUBLIC_PATH
                      : exchangeSpec.getOverrideWebsocketApiUri()+WS_PRIVATE_PATH;
      return apiUrl;
    }

    boolean userAws =
        Boolean.TRUE.equals(exchangeSpecification.getExchangeSpecificParametersItem(PARAM_USE_AWS));
    if (useSandbox()) {
      apiUrl =
          (this.exchangeSpecification.getApiKey() == null)
              ? SANDBOX_WS_PUBLIC_URI+SANDBOX_WS_PUBLIC_PATH
              : SANDBOX_WS_PRIVATE_URI+SANDBOX_WS_PRIVATE_PATH;
    } else {
      apiUrl =
          (this.exchangeSpecification.getApiKey() == null)
              ? userAws ? AWS_WS_PUBLIC_URI+AWS_WS_PUBLIC_PATH : WS_PUBLIC_URI+WS_PUBLIC_PATH
              : userAws ? AWS_WS_PRIVATE_URI+AWS_WS_PRIVATE_PATH: WS_PRIVATE_URI+WS_PRIVATE_PATH;
    }
    return apiUrl;
  }

  @Override
  public Completable disconnect() {
    if(streamingService != null) {
      streamingService.pingPongDisconnectIfConnected();
      return streamingService.disconnect();
    }
    return null;
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
    return streamingTradeService;
  }

  @Override
  public void useCompressedMessages(boolean compressedMessages) {
    throw new NotYetImplementedForExchangeException("useCompressedMessage");
  }

  /**
   * Enables the user to listen on channel inactive events and react appropriately.
   *
   * @param channelInactiveHandler a WebSocketMessageHandler instance.
   */
  public void setChannelInactiveHandler(
      WebSocketClientHandler.WebSocketMessageHandler channelInactiveHandler) {
    streamingService.setChannelInactiveHandler(channelInactiveHandler);
  }
}
