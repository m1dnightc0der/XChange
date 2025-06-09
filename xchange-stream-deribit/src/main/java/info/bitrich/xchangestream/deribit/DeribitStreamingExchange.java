package info.bitrich.xchangestream.deribit;

import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.core.StreamingTradeService;
import info.bitrich.xchangestream.service.netty.WebSocketClientHandler;
import io.reactivex.rxjava3.core.Completable;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.deribit.v2.DeribitExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeribitStreamingExchange extends DeribitExchange implements StreamingExchange {
  private final Logger LOG = LoggerFactory.getLogger(this.getClass());
  // Production URIs
  public static final String WS_PUBLIC_CHANNEL_URI = "wss://www.deribit.com/ws/api/v2/public";
  public static final String WS_PRIVATE_CHANNEL_URI = "wss://www.deribit.com/ws/api/v2/private";

  // Demo(Sandbox) URIs
  public static final String SANDBOX_WS_PUBLIC_CHANNEL_URI =
      "wss://test.deribit.com/ws/api/v2/public";
  public static final String SANDBOX_WS_PRIVATE_CHANNEL_URI =
      "wss://test.deribit.com/ws/api/v2/private";

  private DeribitStreamingService streamingService;

  private DeribitStreamingMarketDataService streamingMarketDataService;

  private DeribitStreamingTradeService streamingTradeService;

  public DeribitStreamingExchange() {}

  @Override
  public Completable connect(ProductSubscription... args) {
    LOG.debug("DeribitStreamingExchange - connect called from: {}",  Thread.currentThread().getStackTrace()[2]);

    this.streamingService = new DeribitStreamingService(getApiUrl(), this.exchangeSpecification);
    this.streamingMarketDataService = new DeribitStreamingMarketDataService(streamingService);
    this.streamingTradeService = new DeribitStreamingTradeService(streamingService, exchangeMetaData);

    return streamingService.connect();
  }

  private String getApiUrl() {
    String apiUrl;
    ExchangeSpecification exchangeSpec = getExchangeSpecification();
    if (exchangeSpec.getOverrideWebsocketApiUri() != null) {
      return exchangeSpec.getOverrideWebsocketApiUri();
    }

    if (useSandbox()) {
      apiUrl =
          (this.exchangeSpecification.getApiKey() == null)
              ? SANDBOX_WS_PUBLIC_CHANNEL_URI
              : SANDBOX_WS_PRIVATE_CHANNEL_URI;
    } else {
      apiUrl =
          (this.exchangeSpecification.getApiKey() == null) ?
               WS_PUBLIC_CHANNEL_URI
              : WS_PRIVATE_CHANNEL_URI;
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
