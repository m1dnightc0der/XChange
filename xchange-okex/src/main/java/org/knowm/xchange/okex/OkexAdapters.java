package org.knowm.xchange.okex;

import org.apache.commons.lang3.StringUtils;
import org.knowm.xchange.currency.Currency;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.derivative.OptionsContract;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.Order.OrderType;
import org.knowm.xchange.dto.account.Balance;
import org.knowm.xchange.dto.account.OpenPosition;
import org.knowm.xchange.dto.account.OpenPositions;
import org.knowm.xchange.dto.account.Wallet;
import org.knowm.xchange.dto.marketdata.*;
import org.knowm.xchange.dto.meta.CurrencyMetaData;
import org.knowm.xchange.dto.meta.ExchangeMetaData;
import org.knowm.xchange.dto.meta.InstrumentMetaData;
import org.knowm.xchange.dto.meta.WalletHealth;
import org.knowm.xchange.dto.trade.*;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.okex.dto.OkexException;
import org.knowm.xchange.okex.dto.OkexInstType;
import org.knowm.xchange.okex.dto.OkexResponse;
import org.knowm.xchange.okex.dto.account.*;
import org.knowm.xchange.okex.dto.marketdata.*;
import org.knowm.xchange.okex.dto.trade.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/** Author: Max Gao (gaamox@tutanota.com) Created: 08-06-2021 */
public class OkexAdapters {

  public static final String SPOT = "SPOT";
  public static final String SWAP = "SWAP";
  public static final String FUTURES = "FUTURES";
  public static final String OPTION = "OPTION";
  private static final String TRADING_WALLET_ID = "trading";
  private static final String FOUNDING_WALLET_ID = "founding";
  private static final String FUTURES_WALLET_ID = "futures";

  public static UserTrades adaptUserTrades(
      List<OkexOrderDetails> okexTradeHistory, ExchangeMetaData exchangeMetaData) {
    List<UserTrade> userTradeList = new ArrayList<>();

    okexTradeHistory.forEach(
        okexOrderDetails -> {
          Instrument instrument = adaptOkexInstrumentId(okexOrderDetails.getInstrumentId());
          userTradeList.add(
              UserTrade.builder()
                  .originalAmount(
                      (exchangeMetaData==null ? new BigDecimal(okexOrderDetails.getAmount())  : convertContractSizeToVolume(
                          okexOrderDetails.getAmount(),
                          instrument,
                          exchangeMetaData.getInstruments().get(instrument).getContractValue())))
                          .instrument(instrument)
                          .price(new BigDecimal(okexOrderDetails.getAverageFilledPrice()))
                          .type(adaptOkexOrderSideToOrderType(okexOrderDetails.getSide()))
                          .id(okexOrderDetails.getOrderId())
                          .orderId(okexOrderDetails.getOrderId())
                          .timestamp(
                                  Date.from(
                                          Instant.ofEpochMilli(Long.parseLong(okexOrderDetails.getUpdateTime()))))
                          .feeAmount(new BigDecimal(okexOrderDetails.getFee()))
                          .feeCurrency(new Currency(okexOrderDetails.getFeeCurrency()))
                          .orderUserReference(okexOrderDetails.getClientOrderId())

                          .build());
        });

    return new UserTrades(userTradeList, Trades.TradeSortType.SortByTimestamp);
  }

  public static List<LimitOrder> adaptOrder(List<OkexOrderDetails> orders, ExchangeMetaData exchangeMetaData) {
    List<LimitOrder> orderList = new ArrayList<>();
    orders.forEach(
        okexOrderOrder -> {orderList.add(adaptOrder( okexOrderOrder,  exchangeMetaData));
        });
return orderList;
  }
    public static LimitOrder adaptOrder(OkexOrderDetails order, ExchangeMetaData exchangeMetaData) {
    Instrument instrument = adaptOkexInstrumentId(order.getInstrumentId());
    return new LimitOrder(
        "buy".equals(order.getSide()) ? Order.OrderType.BID : Order.OrderType.ASK,
        (( exchangeMetaData==null || exchangeMetaData.getInstruments() ==null)?  new BigDecimal(order.getAmount()) : convertContractSizeToVolume(
            order.getAmount(),
            instrument,
            exchangeMetaData.getInstruments().get(instrument).getContractValue())),
        instrument,
        order.getOrderId(),
        new Date(Long.parseLong(order.getCreationTime())),
        new BigDecimal(order.getPrice()),
        order.getAverageFilledPrice().isEmpty()
            ? BigDecimal.ZERO
            : new BigDecimal(order.getAverageFilledPrice()),
        new BigDecimal(order.getAccumulatedFill()),
        new BigDecimal(order.getFee()),
        "live".equals(order.getState())
            ? Order.OrderStatus.OPEN
            : Order.OrderStatus.valueOf(order.getState().toUpperCase(Locale.ENGLISH)),
        order.getClientOrderId());
  }

  public static OpenOrders adaptOpenOrders(
      List<OkexOrderDetails> orders, ExchangeMetaData exchangeMetaData) {
    List<LimitOrder> openOrders =
        orders.stream()
            .map(order -> OkexAdapters.adaptOrder(order, exchangeMetaData))
            .collect(Collectors.toList());
    return new OpenOrders(openOrders);
  }

  public static OkexAmendOrderRequest adaptAmendOrder(
      LimitOrder order, ExchangeMetaData exchangeMetaData) {
    return OkexAmendOrderRequest.builder()
        .instrumentId(adaptInstrument(order.getInstrument()))
        .orderId(order.getId())
        .amendedAmount(exchangeMetaData==null ? order.getOriginalAmount().toString() : convertVolumeToContractSize(order, exchangeMetaData))
        .amendedPrice(order.getLimitPrice().toString())
        .build();
  }

  public static OkexOrderRequest adaptOrder(
      MarketOrder order, ExchangeMetaData exchangeMetaData, String accountLevel) {
    return adaptOrder(order, exchangeMetaData, accountLevel, null, false);
  }

  public static OkexOrderRequest adaptOrder(
      MarketOrder order,
      ExchangeMetaData exchangeMetaData,
      String accountLevel,
      Map<String, Integer> instrumentCodeMap,
      Boolean useInstIdCode) {

    String instrumentId;
    if (Boolean.TRUE.equals(useInstIdCode) && instrumentCodeMap != null) {
      Integer instIdCode = adaptInstrumentCode(order.getInstrument(), instrumentCodeMap);
      instrumentId = instIdCode.toString();
    } else {
      instrumentId = adaptInstrument(order.getInstrument());
    }

    return OkexOrderRequest.builder()
        .instrumentId(instrumentId)
        .tradeMode(adaptTradeMode(order.getInstrument(), accountLevel))
        .side(adaptSide(order.getType()))
        .posSide(order.hasFlag(OkexOrderFlags.LONG_SHORT) ? adaptPosSide(order.getType()) : "net")
        .reducePosition(order.hasFlag(OkexOrderFlags.REDUCE_ONLY))
        .clientOrderId(order.getUserReference())
        .orderType(OkexOrderType.market.name())
        .amount(exchangeMetaData==null ? order.getOriginalAmount().toString(): convertVolumeToContractSize(order, exchangeMetaData))
        .build();
  }
  public static OkexOrderRequest adaptOrder(StopOrder order, ExchangeMetaData exchangeMetaData) {
    return adaptOrder(order, exchangeMetaData, "1", null, false);
  }

  public static OkexOrderRequest adaptOrder(
      StopOrder order,
      ExchangeMetaData exchangeMetaData,
      String accountLevel,
      Map<String, Integer> instrumentCodeMap,
      Boolean useInstIdCode) {

    String instrumentId;
    if (Boolean.TRUE.equals(useInstIdCode) && instrumentCodeMap != null) {
      Integer instIdCode = adaptInstrumentCode(order.getInstrument(), instrumentCodeMap);
      instrumentId = instIdCode.toString();
    } else {
      instrumentId = adaptInstrument(order.getInstrument());
    }

    if (order.getIntention() != null && order.getIntention().equals(StopOrder.Intention.TAKE_PROFIT)) {
      return OkexOrderRequest.builder()
          .instrumentId(instrumentId)
          .tradeMode(adaptTradeMode(order.getInstrument(), accountLevel))
          .side(adaptSide(order.getType()))
          .posSide(order.hasFlag(OkexOrderFlags.LONG_SHORT) ? adaptPosSide(order.getType()) : "net")
          .clientOrderId(order.getUserReference())
          .takeProfitLimitPrice(order.getLimitPrice().toString())
          .takeProfitTriggerPrice(order.getStopPrice().toString())
          .reducePosition(order.hasFlag(OkexOrderFlags.REDUCE_ONLY))
          .orderType(OkexOrderType.conditional.name())
          .amount(exchangeMetaData == null ? order.getOriginalAmount().toString() :
              convertVolumeToContractSize(order, exchangeMetaData))
          .build();
    } else {
      return OkexOrderRequest.builder()
          .instrumentId(instrumentId)
          .tradeMode(adaptTradeMode(order.getInstrument(), accountLevel))
          .side(adaptSide(order.getType()))
          .posSide(order.hasFlag(OkexOrderFlags.LONG_SHORT) ? adaptPosSide(order.getType()) :
              (order.getInstrument() instanceof CurrencyPair ? null : "net"))
          .clientOrderId(order.getUserReference())
          .stopLossLimitPrice(order.getLimitPrice().toString())
          .stopLossTriggerPrice(order.getStopPrice().toString())
          .orderType(OkexOrderType.conditional.name())
          .reducePosition(order.hasFlag(OkexOrderFlags.REDUCE_ONLY))
          .amount(exchangeMetaData == null ? order.getOriginalAmount().toString() :
              convertVolumeToContractSize(order, exchangeMetaData))
          .build();
    }
  }

  /**
   * contract_size to volume: crypto-margined contracts：contract_size,volume(contract_size to
   * volume:volume = sz*ctVal/price) USDT-margined contracts:sz,volume,USDT(contract_size to
   * volume:volume = contract_size*ctVal;contract_size to USDT:volume = contract_size*ctVal*price)
   * OPTION:volume = sz*ctMult volume to contract_size: crypto-margined
   * contracts：contract_size,volume(coin to contract_size:contract_size = volume*price/ctVal)
   * USDT-margined contracts:contract_size,volume,USDT(coin to contract_size:contract_size =
   * volume/ctVal;USDT to contract_size:contract_size = volume/ctVal/price)
   */
  private static String convertVolumeToContractSize(
      Order order, ExchangeMetaData exchangeMetaData) {
    return  order.getOriginalAmount().toString();
  }

  private static BigDecimal convertContractSizeToVolume(
      String okexSize, Instrument instrument, BigDecimal contractValue) {
    return //(instrument instanceof FuturesContract)
       // ? new BigDecimal(okexSize).multiply(contractValue).stripTrailingZeros()
    //    :
    new BigDecimal(okexSize).stripTrailingZeros();
  }

  private static String adaptTradeMode(Instrument instrument, String accountLevel) {
    if (accountLevel.equals("3") || accountLevel.equals("4")) {
      return "cross";
    } else {
      return (instrument instanceof CurrencyPair) ? "cross" : "cross";
    }
  }

  public static OkexOrderRequest adaptOrder(
      LimitOrder order, ExchangeMetaData exchangeMetaData, String accountLevel) {
    return adaptOrder(order, exchangeMetaData, accountLevel, null, false);
  }

  public static OkexOrderRequest adaptOrder(
      LimitOrder order,
      ExchangeMetaData exchangeMetaData,
      String accountLevel,
      Map<String, Integer> instrumentCodeMap,
      Boolean useInstIdCode) {

    String instrumentId;
    if (Boolean.TRUE.equals(useInstIdCode) && instrumentCodeMap != null) {
      Integer instIdCode = adaptInstrumentCode(order.getInstrument(), instrumentCodeMap);
      instrumentId = instIdCode.toString();
    } else {
      instrumentId = adaptInstrument(order.getInstrument());
    }

    return OkexOrderRequest.builder()
        .instrumentId(instrumentId)
        .tradeMode(adaptTradeMode(order.getInstrument(), accountLevel))
        .side(adaptSide(order.getType()))
        .posSide(
            order.hasFlag(OkexOrderFlags.LONG_SHORT) ? adaptPosSide(order.getType()) :
                (order.getInstrument() instanceof CurrencyPair ? null : "net"))
        .clientOrderId(order.getUserReference())
        .reducePosition(order.hasFlag(OkexOrderFlags.REDUCE_ONLY))
        .orderType(adaptOrderType(order.getOrderFlags(), order.getInstrument()))
        .amount(exchangeMetaData==null ? order.getOriginalAmount().toString() :
            convertVolumeToContractSize(order, exchangeMetaData))
        .price(order.getLimitPrice().toString())
        .build();
  }

  public static String adaptSide(OrderType orderType) {
    switch (orderType) {
      case BID:
        return "buy";
      case ASK:
        return "sell";
      case EXIT_ASK:
        return "buy";
      case EXIT_BID:
        return "sell";
      default:
        return null;
    }
  }

  public static String adaptOrderType(Collection<Order.IOrderFlags> orderFlags, Instrument instrument) {


     if(orderFlags.contains(OkexOrderFlags.OPTIMAL_LIMIT_IOC)
        && instrument instanceof FuturesContract)
      return OkexOrderType.optimal_limit_ioc.name();
    else if(orderFlags.contains(OkexOrderFlags.IOC))
      return OkexOrderType.ioc.name();
    if(orderFlags.contains(OkexOrderFlags.POST_ONLY))
      return OkexOrderType.post_only.name();
    else
      return OkexOrderType.limit.name();

  }

  public static String adaptPosSide(OrderType orderType) {
    switch (orderType) {
      case BID:
        return "long";
      case ASK:
        return "short";
      case EXIT_ASK:
        return "short";
      case EXIT_BID:
        return "long";
      default:
        return null;
    }
  }

  public static LimitOrder adaptLimitOrder(
      OkexPublicOrder okexPublicOrder, Instrument instrument, OrderType orderType, Date timestamp) {
    return adaptOrderbookOrder(
        okexPublicOrder.getVolume(), okexPublicOrder.getPrice(), instrument, orderType,timestamp);
  }

  public static OrderBook adaptOrderBook(
      List<OkexOrderbook> okexOrderbooks, Instrument instrument) {
    List<LimitOrder> asks = new ArrayList<>();
    List<LimitOrder> bids = new ArrayList<>();


    Date timeStamp = new Date(Long.parseLong(okexOrderbooks.get(0).getTs()));

    okexOrderbooks
        .get(0)
        .getAsks()
        .forEach(okexAsk -> asks.add(adaptLimitOrder(okexAsk, instrument, OrderType.ASK, timeStamp)));

    okexOrderbooks
        .get(0)
        .getBids()
        .forEach(okexBid -> bids.add(adaptLimitOrder(okexBid, instrument, OrderType.BID, timeStamp)));

    return new OrderBook(timeStamp, asks, bids);
  }

  public static OrderBook adaptOrderBook(
      OkexResponse<List<OkexOrderbook>> okexOrderbook, Instrument instrument) {
    if (!okexOrderbook.isSuccess())
      throw new OkexException(okexOrderbook.getMsg(), Integer.parseInt(okexOrderbook.getCode()));

    return adaptOrderBook(okexOrderbook.getData(), instrument);
  }

  public static LimitOrder adaptOrderbookOrder(
      BigDecimal amount, BigDecimal price, Instrument instrument, Order.OrderType orderType, Date timestamp) {

    return new LimitOrder(orderType, amount, instrument, "", timestamp, price);
  }

  public static Ticker adaptTicker(OkexTicker okexTicker) {
    return new Ticker.Builder()
        .instrument(adaptOkexInstrumentId(okexTicker.getInstrumentId()))
        .open(okexTicker.getOpen24h())
        .last(okexTicker.getLast())
        .bid(okexTicker.getBidPrice())
        .ask(okexTicker.getAskPrice())
        .high(okexTicker.getHigh24h())
        .low(okexTicker.getLow24h())
        // .vwap(null)
        .volume((okexTicker.getInstrumentType().equals("SWAP") || okexTicker.getInstrumentType().equals("FUTURES")) ?
            okexTicker.getVolumeCurrency24h() : okexTicker.getVolume24h())
        .quoteVolume((okexTicker.getInstrumentType().equals("SWAP") || okexTicker.getInstrumentType().equals("FUTURES")) ?
            okexTicker.getVolumeCurrency24h().multiply(okexTicker.getLast()) : okexTicker.getVolumeCurrency24h())
        .timestamp(okexTicker.getTimestamp())
        .bidSize(okexTicker.getBidSize())
        .askSize(okexTicker.getAskSize())
        .percentageChange(null)
        .build();
  }

  public static Instrument adaptOkexInstrumentId(String instrumentId) {
    String[] tokens = instrumentId.split("-");
    if (tokens.length == 2) {
      // SPOT or Margin
      return new CurrencyPair(tokens[0], tokens[1]);
    } else if (tokens.length == 3) {
      // Future Or Swap
      return new FuturesContract(instrumentId.replace("-", "/"));
    } else if (tokens.length == 5) {
      // Option
      return new OptionsContract(instrumentId.replace("-", "/"));
    }
    return null;
  }

  public static String adaptInstrument(Instrument instrument) {
    return instrument.toString().replace('/', '-');
  }

  /**
   * Builds a map of instrument IDs to their corresponding OKEx instIdCode values.
   * The instIdCode is used for WebSocket order operations to reduce latency.
   * Uses String keys (OKX format like "BTC-USDT-SWAP") to avoid HashMap identity issues.
   *
   * @param instruments List of OKEx instruments
   * @return Map of instrumentId (String) to instIdCode (Integer)
   */
  public static Map<String, Integer> buildInstrumentCodeMap(List<OkexInstrument> instruments) {
    Map<String, Integer> instrumentCodeMap = new HashMap<>();
    for (OkexInstrument instrument : instruments) {
      if (instrument.getInstIdCode() != null) {
        instrumentCodeMap.put(instrument.getInstrumentId(), instrument.getInstIdCode());
      }
    }
    return instrumentCodeMap;
  }

  /**
   * Converts an XChange Instrument to its corresponding OKEx instIdCode.
   * The instIdCode is used for WebSocket order operations to reduce latency.
   *
   * @param instrument The instrument to convert
   * @param instrumentCodeMap Map containing instrumentId (String) to instIdCode mappings
   * @return The instIdCode for the instrument
   * @throws IllegalArgumentException if instrument is null or not found in the map
   */
  public static Integer adaptInstrumentCode(Instrument instrument, Map<String, Integer> instrumentCodeMap) {
    if (instrument == null) {
      throw new IllegalArgumentException("Instrument cannot be null");
    }

    String instrumentId = adaptInstrument(instrument);
    Integer instIdCode = instrumentCodeMap.get(instrumentId);
    if (instIdCode == null) {
      throw new IllegalArgumentException("Instrument not found in metadata: " + instrument + " (instrumentId: " + instrumentId + ")");
    }

    return instIdCode;
  }

  public static Trades adaptTrades(List<OkexTrade> okexTrades, Instrument instrument) {
    List<Trade> trades = new ArrayList<>();

    okexTrades.forEach(
        okexTrade ->
            trades.add(
                new Trade.Builder()
                    .id(okexTrade.getTradeId())
                    .instrument(instrument)
                    .originalAmount(okexTrade.getSz())
                    .price(okexTrade.getPx())
                    .timestamp(okexTrade.getTs())
                    .type(adaptOkexOrderSideToOrderType(okexTrade.getSide()))
                    .build()));

    return new Trades(trades);
  }

  public static Order.OrderType adaptOkexOrderSideToOrderType(String okexOrderSide) {

    return okexOrderSide.equals("buy") ? Order.OrderType.BID : Order.OrderType.ASK;
  }

  private static Currency adaptCurrency(OkexCurrency currency) {
    return new Currency(currency.getCurrency());
  }

  private static int numberOfDecimals(BigDecimal value) {
    double d = value.doubleValue();
    return -(int) Math.round(Math.log10(d));
  }

  public static ExchangeMetaData adaptToExchangeMetaData(
      List<OkexInstrument> instruments, List<OkexCurrency> currs, List<OkexTradeFee> tradeFee) {

    Map<Instrument, InstrumentMetaData> instrumentMetaData = new HashMap<>();
    Map<Currency, CurrencyMetaData> currencies = new HashMap<>();

    String makerFee = "0.5";
    if (tradeFee != null && !tradeFee.isEmpty()) {
      makerFee = tradeFee.get(0).getMaker();
    }

    for (OkexInstrument instrument : instruments) {
      if (!"live".equals(instrument.getState())) {
        continue;
      }

      Instrument pair = adaptOkexInstrumentId(instrument.getInstrumentId());
      /*
       TODO The Okex swap contracts with USD or USDC as counter currency
       have issue with the volume conversion (from contractSize to volumeInBaseCurrency and reverse)
       In order to fix the issue we need to change the convertContractSizeToVolume and convertVolumeToContractSize
       functions. Probably we need to add price on the function but it is not possible when we place a MarketOrder
       Because of that i think is best to leave this implementation in the future. (Critical)
      */
      if (pair instanceof FuturesContract
          && ((FuturesContract) pair).isPerpetual()
          && !pair.getCounter().equals(Currency.USDT)) {
        continue;
      }
      instrumentMetaData.put(
          pair,
          new InstrumentMetaData.Builder()
              .tradingFee(new BigDecimal(makerFee).negate())
              .minimumAmount(
                  (instrument.getInstrumentType().equals(OkexInstType.SWAP.name()))
                      ? convertContractSizeToVolume(
                          instrument.getMinSize(),
                          pair,
                          new BigDecimal(instrument.getContractValue()))
                      : new BigDecimal(instrument.getMinSize()))
              .volumeScale(
                  (instrument.getInstrumentType().equals(OkexInstType.SWAP.name()))
                      ? convertContractSizeToVolume(
                              instrument.getMinSize(),
                              pair,
                              new BigDecimal(instrument.getContractValue()))
                          .scale()
                      : Math.max(numberOfDecimals(new BigDecimal(instrument.getMinSize())), 0))
              .contractValue(
                  (instrument.getInstrumentType().equals(OkexInstType.SWAP.name()))
                      ? new BigDecimal(instrument.getContractValue())
                      : null)
              .priceScale(numberOfDecimals(new BigDecimal(instrument.getTickSize())))
              .tradingFeeCurrency(Objects.requireNonNull(pair).getCounter())
              .marketOrderEnabled(true)
              .build());
    }

    if (currs != null) {
      currs.forEach(
          currency ->
              currencies.put(
                  adaptCurrency(currency),
                  new CurrencyMetaData(
                      null,
                      new BigDecimal(currency.getMaxFee()),
                      new BigDecimal(currency.getMinWd()),
                      currency.isCanWd() && currency.isCanDep()
                          ? WalletHealth.ONLINE
                          : WalletHealth.OFFLINE)));
    }

    return new ExchangeMetaData(instrumentMetaData, currencies, null, null, true);
  }

  public static Wallet adaptOkexBalances(List<OkexWalletBalance> okexWalletBalanceList) {
    List<Balance> balances = new ArrayList<>();
    if (!okexWalletBalanceList.isEmpty()) {
      OkexWalletBalance okexWalletBalance = okexWalletBalanceList.get(0);
      balances =
          Arrays.stream(okexWalletBalance.getDetails())
              .map(
                  detail ->
                      new Balance.Builder()
                          .currency(new Currency(detail.getCurrency()))
                          .total(new BigDecimal(detail.getCashBalance()))
                          .available(checkForEmpty(detail.getAvailableBalance()))
                          .timestamp(new Date())
                          .build())
              .collect(Collectors.toList());
    }

    return Wallet.Builder.from(balances)
        .id(TRADING_WALLET_ID)
        .features(new HashSet<>(Collections.singletonList(Wallet.WalletFeature.TRADING)))
            .mmr(((!okexWalletBalanceList.isEmpty() && okexWalletBalanceList.get(0)!=null && okexWalletBalanceList.get(0).getMarginRatio()!=null && !okexWalletBalanceList.get(0).getMarginRatio().isEmpty()) ? new BigDecimal(okexWalletBalanceList.get(0).getMarginRatio()) : null))
        .build();
  }

  public static Wallet adaptOkexAssetBalances(List<OkexAssetBalance> okexAssetBalanceList) {
    List<Balance> balances;
    balances =
        okexAssetBalanceList.stream()
            .map(
                detail ->
                    new Balance.Builder()
                        .currency(new Currency(detail.getCurrency()))
                        .total(new BigDecimal(detail.getBalance()))
                        .available(checkForEmpty(detail.getAvailableBalance()))
                        .timestamp(new Date())
                        .build())
            .collect(Collectors.toList());

    return Wallet.Builder.from(balances)
        .id(FOUNDING_WALLET_ID)
        .features(new HashSet<>(Collections.singletonList(Wallet.WalletFeature.FUNDING)))
        .build();
  }

  private static BigDecimal checkForEmpty(String value) {
    return StringUtils.isEmpty(value) ? null : new BigDecimal(value);
  }

  public static CandleStickData adaptCandleStickData(
      List<OkexCandleStick> okexCandleStickList, CurrencyPair currencyPair) {
    CandleStickData candleStickData = null;
    if (!okexCandleStickList.isEmpty()) {
      List<CandleStick> candleStickList = new ArrayList<>();
      for (OkexCandleStick okexCandleStick : okexCandleStickList) {
        candleStickList.add(
            new CandleStick.Builder()
                .timestamp(new Date(okexCandleStick.getTimestamp()))
                .open(new BigDecimal(okexCandleStick.getOpenPrice()))
                .high(new BigDecimal(okexCandleStick.getHighPrice()))
                .low(new BigDecimal(okexCandleStick.getLowPrice()))
                .close(new BigDecimal(okexCandleStick.getClosePrice()))
                .volume(new BigDecimal(okexCandleStick.getVolume()))
                .quotaVolume(new BigDecimal(okexCandleStick.getVolumeCcy()))
                .build());
      }
      candleStickData = new CandleStickData(currencyPair, candleStickList);
    }
    return candleStickData;
  }

  public static OpenPositions adaptOpenPositions(
      OkexResponse<List<OkexPosition>> positions, ExchangeMetaData exchangeMetaData) {
    List<OpenPosition> openPositions = new ArrayList<>();

    positions
        .getData()
        .forEach(
            okexPosition ->
                openPositions.add(
                    new OpenPosition.Builder()
                        .instrument(adaptOkexInstrumentId(okexPosition.getInstrumentId()))
                        .liquidationPrice(okexPosition.getLiquidationPrice())
                        .price(okexPosition.getAverageOpenPrice())
                        .type(adaptOpenPositionType(okexPosition))
                            .notionalValue(adaptOpenPositionType(okexPosition).equals(OpenPosition.Type.SHORT) ? okexPosition.getNotionalUsd().abs().negate() : okexPosition.getNotionalUsd().abs())
                        .size(adaptOpenPositionType(okexPosition).equals(OpenPosition.Type.SHORT) ?
                            okexPosition
                                .getPosition()
                                .abs().negate()
                                .multiply((exchangeMetaData==null )  ? BigDecimal.ONE :
                                    exchangeMetaData
                                        .getInstruments()
                                        .get(adaptOkexInstrumentId(okexPosition.getInstrumentId()))
                                        .getContractValue()) : okexPosition
                                .getPosition()
                                .abs()
                                .multiply((exchangeMetaData==null )  ? BigDecimal.ONE :
                                        exchangeMetaData
                                                .getInstruments()
                                                .get(adaptOkexInstrumentId(okexPosition.getInstrumentId()))
                                                .getContractValue()))
                        .unRealisedPnl(okexPosition.getUnrealizedPnL())
                        .build()));
    return new OpenPositions(openPositions);
  }

  public static OpenPosition.Type adaptOpenPositionType(OkexPosition okexPosition) {
    switch (okexPosition.getPositionSide()) {
      case "long":
        return OpenPosition.Type.LONG;
      case "short":
        return OpenPosition.Type.SHORT;
      case "net":
        return (okexPosition.getPosition().compareTo(BigDecimal.ZERO) >= 0)
            ? OpenPosition.Type.LONG
            : OpenPosition.Type.SHORT;
      default:
        throw new UnsupportedOperationException();
    }
  }

  public static FundingRate adaptFundingRate(List<OkexFundingRate> okexFundingRate) {
    return new FundingRate.Builder()
        .instrument(adaptOkexInstrumentId(okexFundingRate.get(0).getInstId()))
        .fundingRate8h(okexFundingRate.get(0).getFundingRate())
        .fundingRate1h(
            okexFundingRate
                .get(0)
                .getFundingRate()
                .divide(
                    BigDecimal.valueOf(8),
                    okexFundingRate.get(0).getFundingRate().scale(),
                    RoundingMode.HALF_EVEN))
        .fundingRateDate(okexFundingRate.get(0).getFundingTime())
        .build();
  }

  public static Wallet adaptOkexAccountPositionRisk(
      List<OkexAccountPositionRisk> accountPositionRiskData) {
    BigDecimal totalPositionValueInUsd = BigDecimal.ZERO;

    for (OkexAccountPositionRisk.PositionData positionData :
        accountPositionRiskData.get(0).getPositionData()) {
      totalPositionValueInUsd = totalPositionValueInUsd.add(positionData.getNotionalUsdValue());
    }

    return new Wallet.Builder()
        .balances(
            Collections.singletonList(
                new Balance.Builder()
                    .currency(Currency.USD)
                    .total(accountPositionRiskData.get(0).getAdjustEquity())
                    .build()))
        .id(FUTURES_WALLET_ID)
        .currentLeverage(
            (totalPositionValueInUsd.compareTo(BigDecimal.ZERO) != 0)
                ? totalPositionValueInUsd.divide(
                    accountPositionRiskData.get(0).getAdjustEquity(), 3, RoundingMode.HALF_EVEN)
                : BigDecimal.ZERO)
        .features(new HashSet<>(Collections.singletonList(Wallet.WalletFeature.FUTURES_TRADING)))
        .build();
  }
}
