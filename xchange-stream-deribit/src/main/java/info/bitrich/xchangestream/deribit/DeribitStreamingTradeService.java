package info.bitrich.xchangestream.deribit;

import static info.bitrich.xchangestream.deribit.DeribitStreamingService.USERTRADES;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.knowm.xchange.deribit.v2.service.DeribitTradeService.findOrderFlagValue;
import static org.knowm.xchange.deribit.v2.service.DeribitTradeService.hasOrderFlag;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.bitrich.xchangestream.core.StreamingTradeService;
import info.bitrich.xchangestream.deribit.dto.DeribitOrderMessage;
import info.bitrich.xchangestream.service.netty.StreamingObjectMapperHelper;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;

import org.knowm.xchange.deribit.v2.dto.DeribitError;
import org.knowm.xchange.deribit.v2.dto.DeribitException;
import org.knowm.xchange.deribit.v2.dto.trade.AdvancedOptions;
import org.knowm.xchange.deribit.v2.dto.trade.OrderFlags;
import org.knowm.xchange.deribit.v2.dto.trade.TimeInForce;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.meta.ExchangeMetaData;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.exceptions.ExchangeUnavailableException;
import org.knowm.xchange.deribit.v2.DeribitAdapters;

import org.knowm.xchange.utils.nonce.AtomicLongIncrementalTime2014NonceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import si.mazi.rescu.SynchronizedValueFactory;

public class DeribitStreamingTradeService implements StreamingTradeService {

  private final DeribitStreamingService service;
  private final ExchangeMetaData exchangeMetaData;
  private final int responseTimeout=1000;
  private static final Logger LOG = LoggerFactory.getLogger(DeribitStreamingTradeService.class);
  private final ObjectMapper mapper = StreamingObjectMapperHelper.getObjectMapper();
  private final SynchronizedValueFactory<Long> nonceFactory =
          new AtomicLongIncrementalTime2014NonceFactory();
  public DeribitStreamingTradeService(
      DeribitStreamingService service, ExchangeMetaData exchangeMetaData) {
    this.service = service;
    this.exchangeMetaData = exchangeMetaData;
  }
  @Override public String placeLimitOrder(LimitOrder limitOrder) throws Exception {
    String instrumentName = DeribitAdapters.adaptInstrumentName(limitOrder.getInstrument());
    BigDecimal amount = limitOrder.getOriginalAmount();
    String label = limitOrder.getUserReference();
    TimeInForce timeInForce = findOrderFlagValue(limitOrder, TimeInForce.class);
    BigDecimal maxShow = null;
    String order_id = null;
    Boolean postOnly = hasOrderFlag(limitOrder, OrderFlags.POST_ONLY);
    Boolean rejectPostOnly = hasOrderFlag(limitOrder, OrderFlags.REJECT_POST_ONLY);
    Boolean reduceOnly = hasOrderFlag(limitOrder, OrderFlags.REDUCE_ONLY);
    AdvancedOptions advanced = findOrderFlagValue(limitOrder, AdvancedOptions.class);
    Boolean mmp = hasOrderFlag(limitOrder, OrderFlags.MMP);
    Long id = nonceFactory.createValue();
    DeribitOrderMessage.OrderArg orderArg =
            new DeribitOrderMessage.OrderArg(instrumentName,amount,limitOrder.getLimitPrice(), null, "limit",(timeInForce==null ? null : timeInForce.toString()), maxShow, (postOnly==null ? false : postOnly),(rejectPostOnly==null ? false : rejectPostOnly),(reduceOnly==null ? false : reduceOnly),null,null,(advanced==null ? null : advanced.toString()),null ,null ,null ,(mmp==null ? false : mmp),label);

    DeribitOrderMessage message = new DeribitOrderMessage(((limitOrder.getType() == Order.OrderType.BID || limitOrder.getType() == Order.OrderType.EXIT_ASK) ? "private/buy" :  "private/sell"),id,orderArg);


      if (!service.isPrivateStreamReady()) {
        String readinessDetail = service.privateStreamReadinessDetail();
        LOG.warn("Deribit private websocket is not ready ({}); refusing streaming order placement before send", readinessDetail);
        throw new ExchangeUnavailableException("Deribit private websocket is not ready; refusing streaming order placement before send: " + readinessDetail);
      }

        @NonNull JsonNode response = service.subscribeSingle(id, mapper.writeValueAsString(message)).timeout(responseTimeout, MILLISECONDS).blockingSingle();

      //esponse.get("data").get(0).get("ordId").textValue();
    if (response.has("result")
        && response.get("result").has("order")
        && response.get("result").get("order").hasNonNull("order_id")) {
      order_id = response.get("result").get("order").get("order_id").textValue();
      return order_id;
    } else if (response.has("error") && response.get("error").isObject()) {
      DeribitError deribitError = new DeribitError();
      if (response.get("error").has("code")) {
        deribitError.setCode(response.get("error").get("code").asInt());
      }
      if (response.get("error").hasNonNull("message")) {
        deribitError.setMessage(response.get("error").get("message").textValue());
      }
      if (response.get("error").has("data")) {
        deribitError.setData(response.get("error").get("data"));
      }
      throw DeribitAdapters.adapt(new DeribitException(deribitError));
    }
    throw new ExchangeException("malformed Deribit order placement response: " + response);




  }
  @Override
  public Observable<Order> getOrderChanges(Instrument instrument, Object... args) {
    String channelUniqueId = USERTRADES+".any.any.raw";
    return service
        .subscribeChannel(channelUniqueId).doOnError(
            error -> {
              throw error;
            })
        .filter(message -> message.has("params"))
        .filter(message -> message.get("params").has("data"))
        .flatMap(
            jsonNode -> {
              org.knowm.xchange.deribit.v2.dto.trade.Order deribitOrderDetails =
                  mapper.treeToValue(
                      jsonNode.get("params").get("data"),
                      mapper
                          .getTypeFactory().constructType(org.knowm.xchange.deribit.v2.dto.trade.Order.class));
              return Observable.fromIterable(Arrays.asList( DeribitAdapters.adaptOrder(deribitOrderDetails)));
            });
  }

}
