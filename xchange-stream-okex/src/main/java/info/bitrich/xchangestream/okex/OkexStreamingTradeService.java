package info.bitrich.xchangestream.okex;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.core.StreamingTradeService;
import info.bitrich.xchangestream.okex.dto.OkexOrderMessage;
import info.bitrich.xchangestream.service.netty.StreamingObjectMapperHelper;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.SingleEmitter;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.meta.ExchangeMetaData;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.UserTrade;
import org.knowm.xchange.exceptions.ExchangeUnavailableException;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.okex.OkexAdapters;
import org.knowm.xchange.okex.dto.trade.OkexOrderDetails;
import org.knowm.xchange.okex.dto.trade.OkexOrderRequest;
import org.knowm.xchange.utils.nonce.AtomicLongIncrementalTime2014NonceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import si.mazi.rescu.SynchronizedValueFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static info.bitrich.xchangestream.okex.OkexStreamingService.USERFILLS;
import static info.bitrich.xchangestream.okex.OkexStreamingService.USERTRADES;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.knowm.xchange.okex.OkexExchange.PARAM_CONVERT_QUANTITIES;

public class OkexStreamingTradeService implements StreamingTradeService {

    private final OkexStreamingService service;
    private final ExchangeMetaData exchangeMetaData;
    private final Map<String, Integer> instrumentCodeMap;
    private static final Logger LOG = LoggerFactory.getLogger(OkexStreamingTradeService.class);
    private final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();
    private final SynchronizedValueFactory<Long> nonceFactory =
            new AtomicLongIncrementalTime2014NonceFactory();
    protected final Map<Long, SingleEmitter> messages = new ConcurrentHashMap<>();

    public OkexStreamingTradeService(
            OkexStreamingService service,
            ExchangeMetaData exchangeMetaData,
            Map<String, Integer> instrumentCodeMap) {
        this.service = service;
        this.exchangeMetaData = exchangeMetaData;
        this.instrumentCodeMap = instrumentCodeMap;
    }

    private ExchangeMetaData quantityConversionMetaData() {
        return Boolean.TRUE.equals(
                        service
                                .getExchangeSpecification()
                                .getExchangeSpecificParametersItem(PARAM_CONVERT_QUANTITIES))
                ? exchangeMetaData
                : null;
    }

    @Override
    public String placeLimitOrder(LimitOrder limitOrder) throws Exception {

        ArrayList orderList = new ArrayList<OkexOrderMessage.OrderArg>();
        OkexOrderRequest okxOrder = OkexAdapters.adaptOrder(
                limitOrder,
                quantityConversionMetaData(),
                "1",
                instrumentCodeMap,
                true  // USE instIdCode for WebSocket operations
        );


        OkexOrderMessage.OrderArg args = new OkexOrderMessage.OrderArg(okxOrder.instrumentId, okxOrder.tradeMode, okxOrder.marginCurrency, okxOrder.clientOrderId, okxOrder.tag, okxOrder.side, okxOrder.posSide, okxOrder.orderType, okxOrder.amount, okxOrder.price, null, null, okxOrder.reducePosition, null, false, null, null, null);
        orderList.add(args);

        OkexOrderMessage message = new OkexOrderMessage();
        Long id = nonceFactory.createValue();
        message.setId(id);
        message.setOp("order");
        message.setArgs(orderList);

        if (!service.isPrivateStreamReady()) {
            LOG.warn("OKX private websocket is not connected and authenticated; refusing streaming order placement before send");
            throw new ExchangeUnavailableException("OKX private websocket is not connected and authenticated; refusing streaming order placement before send");
        }

        @NonNull JsonNode response = service.subscribeSingle(id, mapper.writeValueAsString(message)).timeout(1000, MILLISECONDS).blockingSingle();
        JsonNode responseData = response.get("data");
        if (responseData == null || !responseData.isArray() || responseData.size() == 0) {
            throw OkexAdapters.adaptError("unknown", "Order response did not include data: " + response);
        }
        if (responseData.size() != 1) {
            throw OkexAdapters.adaptError(
                    "unknown",
                    "Order response expected 1 row but received " + responseData.size() + ": " + response);
        }
        JsonNode orderResponse = responseData.get(0);
        if (orderResponse == null) {
            throw OkexAdapters.adaptError("unknown", "Order response did not include data: " + response);
        }
        JsonNode sCodeNode = orderResponse.get("sCode");
        if (sCodeNode == null || sCodeNode.asText().isEmpty()) {
            throw OkexAdapters.adaptError("unknown", "Order response did not include sCode: " + response);
        }
        String sCode = sCodeNode.asText();
        if (!"0".equals(sCode)) {
            JsonNode sMsgNode = orderResponse.get("sMsg");
            String sMsg = sMsgNode == null ? "Order placement failed" : sMsgNode.asText();
            throw OkexAdapters.adaptError(sCode, sMsg);
        }
        JsonNode ordIdNode = orderResponse.get("ordId");
        if (ordIdNode == null || ordIdNode.asText().isEmpty()) {
            throw OkexAdapters.adaptError("0", "Successful order response did not include ordId: " + response);
        }
        return ordIdNode.asText();
    }

    @Override
    public Observable<Order> getOrderChanges(Instrument instrument, Object... args) {
        String channelUniqueId = USERTRADES;
        return service
                .subscribeChannel(USERTRADES).doOnError(
                        error -> {
                            throw error;
                        })
                .filter(message -> message.has("data"))
                .flatMap(
                        jsonNode -> {
                            List<OkexOrderDetails> okexOrderDetails =
                                    mapper.treeToValue(
                                            jsonNode.get("data"),
                                            mapper
                                                    .getTypeFactory()
                                                    .constructCollectionType(List.class, OkexOrderDetails.class));
                            return Observable.fromIterable(
                                    OkexAdapters.adaptOrder(
                                            okexOrderDetails, quantityConversionMetaData()));
                        });
    }

    @Override
    public Observable<UserTrade> getUserTrades(Instrument instrument, Object... args) {
        String channelUniqueId = USERFILLS;
        return service
                .subscribeChannel(USERFILLS).doOnError(
                        error -> {
                            throw error;
                        })
                .filter(message -> message.has("data"))
                .flatMap(
                        jsonNode -> {
                            List<OkexOrderDetails> okexOrderDetails =
                                    mapper.treeToValue(
                                            jsonNode.get("data"),
                                            mapper
                                                    .getTypeFactory()
                                                    .constructCollectionType(List.class, OkexOrderDetails.class));
                            return Observable.fromIterable(
                                    OkexAdapters.adaptUserTrades(
                                                    okexOrderDetails,
                                                    quantityConversionMetaData(),
                                                    OkexAdapters.UserTradeSource.FILLS_CHANNEL)
                                            .getUserTrades());
                        });
    }


}
