package info.bitrich.xchangestream.hyperliquid;

import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.hyperliquid.dto.HyperliquidBbo;
import info.bitrich.xchangestream.service.netty.StreamingObjectMapperHelper;
import io.reactivex.rxjava3.core.Observable;
import org.knowm.xchange.dto.marketdata.*;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.hyperliquid.HyperliquidAdapters;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidL2Book;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidTrade;
import org.knowm.xchange.instrument.Instrument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static info.bitrich.xchangestream.hyperliquid.HyperliquidStreamingService.*;

public class HyperliquidStreamingMarketDataService implements StreamingMarketDataService {

    private static final Logger LOG = LoggerFactory.getLogger(HyperliquidStreamingMarketDataService.class);

    private final HyperliquidStreamingService service;
    private final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();
    private final Map<String, OrderBook> orderBookMap = new ConcurrentHashMap<>();

    public HyperliquidStreamingMarketDataService(HyperliquidStreamingService service) {
        this.service = service;
    }

    @Override
    public Observable<Ticker> getTicker(Instrument instrument, Object... args) {
        String coin = HyperliquidAdapters.adaptInstrumentToCoin(instrument);
        String channelName = BBO + "." + coin;

        return service
                .subscribeChannel(channelName)
                .filter(message -> message.has("data"))
                .filter(message -> message.has("channel") && message.get("channel").asText().equals(BBO))
                .flatMap(jsonNode -> {
                    try {
                        HyperliquidBbo bbo = mapper.treeToValue(
                                jsonNode.get("data"), HyperliquidBbo.class);

                        Ticker ticker = new Ticker.Builder()
                                .instrument(instrument)
                                .bid(bbo.getBids().firstKey())
                                .bidSize(bbo.getBids().firstEntry().getValue())
                                .ask(bbo.getAsks().firstKey())
                                .askSize(bbo.getAsks().firstEntry().getValue())
                                .timestamp(new Date(bbo.getTime()))
                                .build();

                        return Observable.just(ticker);
                    } catch (Exception e) {
                        LOG.error("Error processing BBO data", e);
                        return Observable.empty();
                    }
                });
    }

    @Override
    public Observable<OrderBook> getOrderBook(Instrument instrument, Object... args) {
        String coin = HyperliquidAdapters.adaptInstrumentToCoin(instrument);
        String channelName = L2BOOK + "." + coin;

        return service
                .subscribeChannel(channelName)
                .filter(message -> message.has("data"))
                .filter(message -> message.has("channel") && message.get("channel").asText().equals(L2BOOK))
                .flatMap(jsonNode -> {
                    try {
                        HyperliquidL2Book l2Book = mapper.treeToValue(
                                jsonNode.get("data"), HyperliquidL2Book.class);
                        OrderBook orderBook = HyperliquidAdapters.adaptOrderBook(l2Book, instrument);


                        return Observable.just(orderBook);
                    } catch (Exception e) {
                        LOG.error("Error processing L2Book data", e);
                        return Observable.empty();
                    }
                });
    }

    @Override
    public Observable<Trade> getTrades(Instrument instrument, Object... args) {
        String coin = HyperliquidAdapters.adaptInstrumentToCoin(instrument);

        String channelName = TRADES + "." + coin;


        return service
                .subscribeChannel(channelName)
                .filter(message -> message.has("data"))
                .filter(message -> message.has("channel") && message.get("channel").asText().equals("trades"))
                .flatMap(
                        jsonNode -> {
                            try {
                                List<HyperliquidTrade> hyperLiquidTradeList =
                                        mapper.treeToValue(
                                                jsonNode.get("data"),
                                                mapper.getTypeFactory().constructCollectionType(List.class, HyperliquidTrade.class));
                                return Observable.fromIterable(
                                        HyperliquidAdapters.adaptTrades(hyperLiquidTradeList, instrument).getTrades());
                            } catch (Exception e) {
                                LOG.error("Error processing trade data", e);
                                return Observable.empty();
                            }
                        });


    }

    @Override
    public Observable<List<OrderBookUpdate>> getOrderBookUpdates(Instrument instrument, Object... args) {
        throw new NotYetImplementedForExchangeException("getOrderBookUpdates");
    }

    @Override
    public Observable<FundingRate> getFundingRate(Instrument instrument, Object... args) {
        throw new NotYetImplementedForExchangeException("getFundingRate");
    }


}