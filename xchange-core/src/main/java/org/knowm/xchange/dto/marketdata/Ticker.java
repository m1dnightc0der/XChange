package org.knowm.xchange.dto.marketdata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.Objects;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.utils.Assert;
import org.knowm.xchange.utils.DateUtils;

import javax.annotation.Nullable;

/**
 * A class encapsulating the information a "Ticker" can contain. Some fields can be empty if not
 * provided by the exchange.
 *
 * <p>A ticker contains data representing the latest trade.
 */
@JsonDeserialize(builder = Ticker.Builder.class)
public final class Ticker implements Serializable {

  private static final long serialVersionUID = -3247730106987193154L;

  private final Instrument instrument;
  private final BigDecimal open;
  private final BigDecimal last;
  private final BigDecimal bid;
  private final BigDecimal ask;
  private final BigDecimal high;
  private final BigDecimal low;
  private final BigDecimal vwap;
  private final BigDecimal volume;
  private final BigDecimal quoteVolume;
  private  BigDecimal markPrice;
  private BigDecimal settlementPrice;
  private BigDecimal openInterest;
  private BigDecimal askIv;
  private BigDecimal bidIv;
  private BigDecimal markIv;

  private BigDecimal deliveryPrice;
  private BigDecimal estimatedDeliveryPrice;
  private BigDecimal currentFunding;
  private BigDecimal funding8h;
  private BigDecimal indexPrice;
  private String instrumentName;
  private BigDecimal interestRate;

  private String underlyingIndex;
  private BigDecimal underlyingPrice;




  private BigDecimal vega;
  private BigDecimal theta;
  private BigDecimal rho;
  private BigDecimal gamma;
  private BigDecimal delta;

  /** the timestamp of the ticker according to the exchange's server, null if not provided */
  private final Date timestamp;

  private final BigDecimal bidSize;
  private final BigDecimal askSize;
  private final BigDecimal percentageChange;








  /**
   * Constructor
   *
   * @param instrument The tradable identifier (e.g. BTC in BTC/USD)
   * @param last Last price
   * @param bid Bid price
   * @param ask Ask price
   * @param high High price
   * @param low Low price
   * @param vwap Volume Weighted Average Price
   * @param volume 24h volume in base currency
   * @param quoteVolume 24h volume in counter currency
   * @param timestamp - the timestamp of the ticker according to the exchange's server, null if not
   *     provided
   * @param bidSize The instantaneous size at the bid price
   * @param askSize The instantaneous size at the ask price
   * @param percentageChange Price percentage change. Is compared against the last price value. Will
   *     be null if not provided and cannot be calculated. Should be represented as percentage (e.g.
   *     0.5 equal 0.5%, 1 equal 1%, 50 equal 50%, 100 equal 100%)
   */
  private Ticker(
      Instrument instrument,
      BigDecimal open,
      BigDecimal last,
      BigDecimal bid,
      BigDecimal ask,
      BigDecimal high,
      BigDecimal low,
      BigDecimal vwap,
      BigDecimal volume,
      BigDecimal quoteVolume,
      Date timestamp,
      BigDecimal bidSize,
      BigDecimal askSize,
      BigDecimal percentageChange) {
    this.open = open;
    this.instrument = instrument;
    this.last = last;
    this.bid = bid;
    this.ask = ask;
    this.high = high;
    this.low = low;
    this.vwap = vwap;
    this.volume = volume;
    this.quoteVolume = quoteVolume;
    this.timestamp = timestamp;
    this.bidSize = bidSize;
    this.askSize = askSize;
    this.percentageChange = percentageChange;
  }


  /**
   * Constructor
   *
   * @param instrument The tradable identifier (e.g. BTC in BTC/USD)
   * @param last Last price
   * @param bid Bid price
   * @param ask Ask price
   * @param high High price
   * @param low Low price
   * @param vwap Volume Weighted Average Price
   * @param volume 24h volume in base currency
   * @param quoteVolume 24h volume in counter currency
   * @param timestamp - the timestamp of the ticker according to the exchange's server, null if not
   *     provided
   * @param bidSize The instantaneous size at the bid price
   * @param askSize The instantaneous size at the ask price
   * @param percentageChange Price percentage change. Is compared against the last price value. Will
   *     be null if not provided and cannot be calculated. Should be represented as percentage (e.g.
   *     0.5 equal 0.5%, 1 equal 1%, 50 equal 50%, 100 equal 100%)
   * @param markPrice
   * @param settlementPrice
   * @param openInterest
   * @param askIv
   * @param bidIv
   * @param markIv
   * @param deliveryPrice
   * @param  estimatedDeliveryPrice
   * @param currentFunding
   * @param funding8h
   * @param indexPrice
   * @param  instrumentName
   * @param interestRate
   * @param underlyingIndex
   * @param underlyingPrice
   * @param vega
   * @param  theta
   * @param rho
   * @param gamma
   * @param delta
   */

  private Ticker(
          Instrument instrument,
          BigDecimal open,
          BigDecimal last,
          BigDecimal bid,
          BigDecimal ask,
          BigDecimal high,
          BigDecimal low,
          BigDecimal vwap,
          BigDecimal volume,
          BigDecimal quoteVolume,
          Date timestamp,
          BigDecimal bidSize,
          BigDecimal askSize,
          BigDecimal percentageChange,
            BigDecimal markPrice,
   BigDecimal settlementPrice,
   BigDecimal openInterest,
   BigDecimal askIv,
   BigDecimal bidIv,
   BigDecimal markIv,
   BigDecimal deliveryPrice,
          BigDecimal     estimatedDeliveryPrice,
   BigDecimal currentFunding,
   BigDecimal funding8h,
          BigDecimal indexPrice,
   String instrumentName,
   BigDecimal interestRate,
   String underlyingIndex,
   BigDecimal underlyingPrice,
   BigDecimal vega,
   BigDecimal theta,
   BigDecimal rho,
   BigDecimal gamma,
   BigDecimal delta) {
    this.open = open;
    this.instrument = instrument;
    this.last = last;
    this.bid = bid;
    this.ask = ask;
    this.high = high;
    this.low = low;
    this.vwap = vwap;
    this.volume = volume;
    this.quoteVolume = quoteVolume;
    this.timestamp = timestamp;
    this.bidSize = bidSize;
    this.askSize = askSize;
    this.percentageChange = percentageChange;
    this.markPrice=markPrice;
            this.settlementPrice=settlementPrice;
            this.openInterest=openInterest;
            this.askIv=askIv;
            this.bidIv=bidIv;
            this.markIv=markIv;
            this.deliveryPrice=deliveryPrice;
    this.estimatedDeliveryPrice=estimatedDeliveryPrice;
            this.currentFunding=currentFunding;
            this.funding8h=funding8h;
            this.indexPrice=indexPrice;
            this.instrumentName=instrumentName;
            this.interestRate=interestRate;
            this.underlyingIndex=underlyingIndex;
            this.underlyingPrice=underlyingPrice;
            this.vega=vega;
            this.theta=theta;
            this.rho=rho;
            this.gamma=gamma;
            this.delta=delta;



  }


  public Instrument getInstrument() {
    return instrument;
  }

  /**
   * @deprecated CurrencyPair is a subtype of Instrument - this method will throw an exception if
   *     the order was for a derivative
   *     <p>use {@link #getInstrument()} instead
   */
  @Deprecated
  @JsonIgnore
  public CurrencyPair getCurrencyPair() {
    if (instrument == null) {
      return null;
    }
    if (!(instrument instanceof CurrencyPair)) {
      throw new IllegalStateException(
          "The instrument of this order is not a currency pair: " + instrument);
    }
    return (CurrencyPair) instrument;
  }

  public BigDecimal getOpen() {

    return open;
  }

  public BigDecimal getLast() {

    return last;
  }

  public BigDecimal getBid() {

    return bid;
  }


  @Nullable
  public BigDecimal getMarkPrice() {


    return markPrice;
  }
  @Nullable
  public BigDecimal getSettlementPrice() {

    return settlementPrice;
  }
  @Nullable
  public BigDecimal getOpenInterest() {

    return openInterest;
  }
  @Nullable
  public BigDecimal getAskIv() {

    return askIv;
  }
  @Nullable
  public BigDecimal getBidIv() {

    return bidIv;
  }
  @Nullable
  public BigDecimal getMarkIv() {

    return markIv;
  }
  @Nullable
  public BigDecimal getDeliveryPrice() {

    return deliveryPrice;
  }
  @Nullable
  public BigDecimal getEstimatedDeliveryPrice() {

    return estimatedDeliveryPrice;
  }
  @Nullable
  public BigDecimal getCurrentFunding() {

    return currentFunding;
  }
  @Nullable
  public  BigDecimal getFunding8h() {

    return funding8h;
  }
  @Nullable
  public BigDecimal getIndexPrice() {

    return indexPrice;
  }
  @Nullable
  public String getInstrumentName() {

    return instrumentName;
  }
  @Nullable
  public BigDecimal getInterestRate() {

    return interestRate;
  }
  @Nullable
  public String getUnderlyingIndex() {

    return underlyingIndex;
  }
  @Nullable
  public BigDecimal getUnderlyingPrice() {

    return underlyingPrice;
  }
  @Nullable
  public BigDecimal getVega() {

    return vega;
  }
  @Nullable
  public BigDecimal getTheta() {

    return theta;
  }
  @Nullable
  public BigDecimal getRho() {

    return rho;
  }
  @Nullable
  public BigDecimal getGamma() {

    return gamma;
  }
  @Nullable
  public BigDecimal getDelta() {

    return delta;
  }


  public BigDecimal getAsk() {

    return ask;
  }

  public BigDecimal getHigh() {

    return high;
  }

  public BigDecimal getLow() {

    return low;
  }

  public BigDecimal getVwap() {

    return vwap;
  }

  public BigDecimal getVolume() {
    if (volume == null && quoteVolume != null && last != null && !last.equals(BigDecimal.ZERO)) {
      return quoteVolume.divide(last, RoundingMode.HALF_UP);
    }

    return volume;
  }

  public BigDecimal getQuoteVolume() {
    if (quoteVolume == null && volume != null && last != null) {
      return volume.multiply(last);
    }
    return quoteVolume;
  }

  public Date getTimestamp() {

    return timestamp;
  }

  public BigDecimal getBidSize() {
    return bidSize;
  }

  public BigDecimal getAskSize() {
    return askSize;
  }

  public BigDecimal getPercentageChange() {
    return percentageChange;
  }

  @Override
  public String toString() {

    return "Ticker [instrument="
        + instrument
        + ", open="
        + open
        + ", last="
        + last
        + ", bid="
        + bid
        + ", ask="
        + ask
        + ", high="
        + high
        + ", low="
        + low
        + ",avg="
        + vwap
        + ", volume="
        + volume
        + ", quoteVolume="
        + quoteVolume
        + ", timestamp="
        + DateUtils.toMillisNullSafe(timestamp)
        + ", bidSize="
        + bidSize
        + ", askSize="
        + askSize
        + ", percentageChange="
        + percentageChange
        + "]";
  }

  /**
   * Builder to provide the following to {@link Ticker}:
   *
   * <ul>
   *   <li>Provision of fluent chained construction interface
   * </ul>
   */
  @JsonPOJOBuilder(withPrefix = "")
  public static class Builder {

    private Instrument instrument;
    private BigDecimal open;
    private BigDecimal last;
    private BigDecimal bid;
    private BigDecimal ask;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal vwap;
    private BigDecimal volume;
    private BigDecimal quoteVolume;
    private Date timestamp;
    private BigDecimal bidSize;
    private BigDecimal askSize;
    private BigDecimal percentageChange;

    private BigDecimal markPrice;
    private BigDecimal settlementPrice;
    private BigDecimal openInterest;
    private BigDecimal askIv;
    private  BigDecimal bidIv;
    private BigDecimal markIv;
    private BigDecimal deliveryPrice;

    private BigDecimal  estimatedDeliveryPrice;
    private BigDecimal currentFunding;
    private  BigDecimal funding8h;
    private BigDecimal indexPrice;
    private String instrumentName;
    private BigDecimal interestRate;
    private String underlyingIndex;
    private BigDecimal underlyingPrice;
    private BigDecimal vega;
    private BigDecimal theta;
    private BigDecimal rho;
    private BigDecimal gamma;
    private BigDecimal delta;

    // Prevent repeat builds
    private boolean isBuilt = false;

    public Ticker build() {

      validateState();

      Ticker ticker =
          new Ticker(
              instrument,
              open,
              last,
              bid,
              ask,
              high,
              low,
              vwap,
              volume,
              quoteVolume,
              timestamp,
              bidSize,
              askSize,
              percentageChange, markPrice,settlementPrice,openInterest,askIv,bidIv,markIv,deliveryPrice,estimatedDeliveryPrice, currentFunding,funding8h, indexPrice,instrumentName,interestRate,underlyingIndex,underlyingPrice,vega,theta,rho, gamma,delta);

      isBuilt = true;

      return ticker;
    }

    private void validateState() {

      if (isBuilt) {
        throw new IllegalStateException("The entity has been built");
      }
    }

    public Builder instrument(Instrument instrument) {
      Assert.notNull(instrument, "Null instrument");
      this.instrument = instrument;
      return this;
    }

    /**
     * @deprecated Use {@link #instrument(Instrument)}
     */
    @Deprecated
    public Builder currencyPair(CurrencyPair currencyPair) {
      return instrument(currencyPair);
    }

    public Builder open(BigDecimal open) {

      this.open = open;
      return this;
    }

    public Builder last(BigDecimal last) {

      this.last = last;
      return this;
    }

    public Builder bid(BigDecimal bid) {

      this.bid = bid;
      return this;
    }

    public Builder ask(BigDecimal ask) {

      this.ask = ask;
      return this;
    }

    public Builder high(BigDecimal high) {

      this.high = high;
      return this;
    }

    public Builder low(BigDecimal low) {

      this.low = low;
      return this;
    }

    public Builder vwap(BigDecimal vwap) {

      this.vwap = vwap;
      return this;
    }

    public Builder volume(BigDecimal volume) {

      this.volume = volume;
      return this;
    }

    public Builder quoteVolume(BigDecimal quoteVolume) {

      this.quoteVolume = quoteVolume;
      return this;
    }

    public Builder timestamp(Date timestamp) {

      this.timestamp = timestamp;
      return this;
    }

    public Builder bidSize(BigDecimal bidSize) {
      this.bidSize = bidSize;
      return this;
    }

    public Builder askSize(BigDecimal askSize) {
      this.askSize = askSize;
      return this;
    }

    public Builder percentageChange(BigDecimal percentageChange) {
      this.percentageChange = percentageChange;
      return this;
    }

    public Builder markPrice (BigDecimal markPrice){
      this.markPrice = markPrice;
      return this;
    }
    public Builder settlementPrice (BigDecimal settlementPrice){
      this.settlementPrice = settlementPrice;
      return this;
    }
    public Builder openInterest (BigDecimal openInterest){
      this.openInterest = openInterest;
      return this;
    }
    public Builder askIv (BigDecimal askIv){
      this.askIv = askIv;
      return this;
    }
    public  Builder bidIv (BigDecimal bidIv){
      this.bidIv = bidIv;
      return this;
    }
    public Builder markIv (BigDecimal markIv){
      this.markIv = markIv;
      return this;
    }
    public Builder deliveryPrice (BigDecimal deliveryPrice){
      this.deliveryPrice = deliveryPrice;
      return this;
    }

    public Builder estimatedDeliveryPrice (BigDecimal estimatedDeliveryPrice){
      this.estimatedDeliveryPrice = estimatedDeliveryPrice;
      return this;
    }
    public Builder currentFunding (BigDecimal currentFunding){
      this.currentFunding = currentFunding;
      return this;
    }
    public Builder funding8h(BigDecimal funding8h){
      this.funding8h = funding8h;
      return this;
    }
    public Builder indexPrice(BigDecimal indexPrice){
      this.indexPrice = indexPrice;
      return this;
    }
    public Builder instrumentName(String instrumentName){
      this.instrumentName = instrumentName;
      return this;
    }
    public Builder interestRate (BigDecimal interestRate){
      this.interestRate = interestRate;
      return this;}
    public Builder underlyingIndex(String underlyingIndex){
      this.underlyingIndex = underlyingIndex;
      return this;
    }
    public Builder underlyingPrice(BigDecimal underlyingPrice){
      this.underlyingPrice = underlyingPrice;
      return this;
    }
    public Builder vega (BigDecimal vega){
      this.vega = vega;
      return this;
    }
    public Builder theta(BigDecimal theta){
      this.theta = theta;
      return this;
    }
    public Builder rho(BigDecimal rho){
      this.rho = rho;
      return this;
    }
    public Builder gamma(BigDecimal gamma){
      this.gamma = gamma;
      return this;
    }
    public Builder delta(BigDecimal delta){
      this.delta = delta;
      return this;
    }

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Ticker ticker = (Ticker) o;
    return Objects.equals(getInstrument(), ticker.getInstrument())
        && Objects.equals(getOpen(), ticker.getOpen())
        && Objects.equals(getLast(), ticker.getLast())
        && Objects.equals(getBid(), ticker.getBid())
        && Objects.equals(getAsk(), ticker.getAsk())
        && Objects.equals(getHigh(), ticker.getHigh())
        && Objects.equals(getLow(), ticker.getLow())
        && Objects.equals(getVwap(), ticker.getVwap())
        && Objects.equals(getVolume(), ticker.getVolume())
        && Objects.equals(getQuoteVolume(), ticker.getQuoteVolume())
        && Objects.equals(getTimestamp(), ticker.getTimestamp())
        && Objects.equals(getBidSize(), ticker.getBidSize())
        && Objects.equals(getAskSize(), ticker.getAskSize())
        && Objects.equals(getPercentageChange(), ticker.getPercentageChange());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getInstrument(),
        getOpen(),
        getLast(),
        getBid(),
        getAsk(),
        getHigh(),
        getLow(),
        getVwap(),
        getVolume(),
        getQuoteVolume(),
        getTimestamp(),
        getBidSize(),
        getAskSize());
  }
}
