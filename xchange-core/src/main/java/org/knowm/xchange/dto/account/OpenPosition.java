package org.knowm.xchange.dto.account;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;
import org.knowm.xchange.instrument.Instrument;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class OpenPosition implements Serializable {
  /** The instrument */
  private final Instrument instrument;

  /** Is this a long or a short position */
  private final Type type;

  /** The size of the position */
  private final BigDecimal size;

  /** The average entry price for the position */
  @JsonIgnore private final BigDecimal price;

  /** The estimatedLiquidationPrice */
  @JsonIgnore private final BigDecimal liquidationPrice;

  /** The unrealised pnl of the position */
  @JsonIgnore private final BigDecimal unRealisedPnl;

  /** The options delta for the position */
  @JsonIgnore private final BigDecimal delta;

  @JsonIgnore private final BigDecimal gamma;

  @JsonIgnore private final BigDecimal theta;

  @JsonIgnore private final BigDecimal rho;
  @JsonIgnore private final BigDecimal vega;
  @JsonIgnore private final BigDecimal markPrice;

  public OpenPosition(
          @JsonProperty("instrument") Instrument instrument,
          @JsonProperty("type") Type type,
          @JsonProperty("size") BigDecimal size,
          @JsonProperty("price") BigDecimal price,
          @JsonProperty("liquidationPrice") BigDecimal liquidationPrice,
          @JsonProperty("unRealisedPnl") BigDecimal unRealisedPnl,
          @JsonProperty("delta") BigDecimal delta,
          @JsonProperty("markPrice") BigDecimal markPrice,
          @JsonProperty("gamma") BigDecimal gamma,
          @JsonProperty("theta") BigDecimal theta,
          @JsonProperty("rho") BigDecimal rho,
          @JsonProperty("vega") BigDecimal vega
          ) {
    this.instrument = instrument;
    this.type = type;
    this.size = size;
    this.price = price;
    this.liquidationPrice = liquidationPrice;
    this.unRealisedPnl = unRealisedPnl;
    this.delta=delta;
    this.gamma=gamma;
    this.theta=theta;
    this.rho=rho;
    this.vega=vega;
    this.markPrice=markPrice;
  }
  public OpenPosition(
          @JsonProperty("instrument") Instrument instrument,
          @JsonProperty("type") Type type,
          @JsonProperty("size") BigDecimal size,
          @JsonProperty("price") BigDecimal price,
          @JsonProperty("liquidationPrice") BigDecimal liquidationPrice,
          @JsonProperty("unRealisedPnl") BigDecimal unRealisedPnl
  ) {
    this.instrument = instrument;
    this.type = type;
    this.size = size;
    this.price = price;
    this.liquidationPrice = liquidationPrice;
    this.unRealisedPnl = unRealisedPnl;
    this.delta=null;
    this.gamma=null;
    this.theta=null;
    this.rho=null;
    this.vega=null;
    this.markPrice=null;
  }

  public Instrument getInstrument() {
    return instrument;
  }

  public Type getType() {
    return type;
  }

  public BigDecimal getSize() {
    return size;
  }
  public BigDecimal getDelta() {
    return delta;
  }


  public BigDecimal getGamma() {
    return gamma;
  }



  public BigDecimal getTheta() {
    return theta;
  }


  public BigDecimal getRho() {
    return rho;
  }
  public BigDecimal getVega() {
    return vega;
  }
  public BigDecimal getMarkPrice() {
    return markPrice;
  }
  public BigDecimal getPrice() {
    return price;
  }

  public BigDecimal getLiquidationPrice() {
    return liquidationPrice;
  }

  public BigDecimal getUnRealisedPnl() {
    return unRealisedPnl;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final OpenPosition that = (OpenPosition) o;
    return Objects.equals(instrument, that.instrument)
        && type == that.type
        && Objects.equals(size, that.size)
        && Objects.equals(price, that.price)
        && Objects.equals(liquidationPrice, that.liquidationPrice)
        && Objects.equals(unRealisedPnl, that.unRealisedPnl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(instrument, type, size, price, liquidationPrice, unRealisedPnl);
  }

  @Override
  public String toString() {
    return "OpenPosition{"
        + "instrument="
        + instrument
        + ", type="
        + type
        + ", size="
        + size
        + ", price="
        + price
        + ", liquidationPrice="
        + liquidationPrice
        + ", unRealisedPnl="
        + unRealisedPnl
            + ", delta="
            + delta
            + ", theta="
            + theta
            + ", gamma="
            + gamma
            + ", rho="
            + rho

            + ", markPrice="
            + markPrice
        + '}';
  }

  public enum Type {
    LONG,
    SHORT
  }

  public static class Builder {
    private Instrument instrument;
    private Type type;
    private BigDecimal size;
    private BigDecimal price;
    private BigDecimal liquidationPrice;
    private BigDecimal unRealisedPnl;
    private BigDecimal delta;
    private BigDecimal gamma;
    private BigDecimal vega;
    private BigDecimal theta;
    private BigDecimal rho;
    private BigDecimal markPrice;
    public static Builder from(OpenPosition openPosition) {
      return new Builder()
          .instrument(openPosition.getInstrument())
          .type(openPosition.getType())
          .size(openPosition.getSize())
          .liquidationPrice(openPosition.getLiquidationPrice())
          .unRealisedPnl(openPosition.getUnRealisedPnl())
          .price(openPosition.getPrice())
              .delta(openPosition.getDelta())
              .gamma(openPosition.getGamma())
              .theta(openPosition.getTheta())
              .rho(openPosition.getRho())
              .vega(openPosition.getVega())
              .markPrice(openPosition.getMarkPrice());
    }

    public Builder instrument(final Instrument instrument) {
      this.instrument = instrument;
      return this;
    }

    public Builder type(final Type type) {
      this.type = type;
      return this;
    }

    public Builder size(final BigDecimal size) {
      this.size = size;
      return this;
    }

    public Builder price(final BigDecimal price) {
      this.price = price;
      return this;
    }

    public Builder liquidationPrice(final BigDecimal liquidationPrice) {
      this.liquidationPrice = liquidationPrice;
      return this;
    }

    public Builder unRealisedPnl(final BigDecimal unRealisedPnl) {
      this.unRealisedPnl = unRealisedPnl;
      return this;
    }

    public Builder delta(final BigDecimal delta) {
      this.delta = delta;
      return this;
    }
    public Builder vega(final BigDecimal vega) {
      this.vega = vega;
      return this;
    }
    public Builder gamma(final BigDecimal gamma) {
      this.gamma = gamma;
      return this;
    }

    public Builder theta(final BigDecimal theta) {
      this.theta = theta;
      return this;
    }

    public Builder rho(final BigDecimal rho) {
      this.rho = rho;
      return this;
    }
    public Builder markPrice(final BigDecimal markPrice) {
      this.markPrice = markPrice;
      return this;
    }
    public OpenPosition build() {
      return new OpenPosition(instrument, type, size, price, liquidationPrice, unRealisedPnl,delta,markPrice, gamma,theta, rho, vega);
    }
  }
}
