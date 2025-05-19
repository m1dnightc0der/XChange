package org.knowm.xchange.bybit.dto.account;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.math.BigDecimal;

/** https://bybit-exchange.github.io/docs/v5/position */
@Getter
@NoArgsConstructor
@ToString
public class BybitPosition {

  @JsonProperty("positionIdx")
  private Integer positionIdx;

  @JsonProperty("riskId")
  private Integer riskId;

  @JsonProperty("riskLimitValue")
  private String riskLimitValue;

  @JsonProperty("symbol")
  private String symbol;

  @JsonProperty("side")
  private String side;

  @JsonProperty("size")
  private BigDecimal size;

  @JsonProperty("avgPrice")
  private BigDecimal avgPrice;

  @JsonProperty("positionValue")
  private BigDecimal positionValue;

  @JsonProperty("tradeMode")
  private Integer tradeMode;

  @JsonProperty("autoAddMargin")
  private Integer autoAddMargin;

  @JsonProperty("positionStatus")
  private String positionStatus;

  @JsonProperty("leverage")
  private String leverage;

  @JsonProperty("markPrice")
  private BigDecimal markPrice;

  @JsonProperty("liqPrice")
  private BigDecimal liqPrice;

  @JsonProperty("bustPrice")
  private BigDecimal bustPrice;

  @JsonProperty("positionIM")
  private String positionIM;

  @JsonProperty("positionMM")
  private String positionMM;

  @JsonProperty("positionBalance")
  private String positionBalance;


  @JsonProperty("tpslMode")
  private String tpslMode;

  @JsonProperty("takeProfit")
  private String takeProfit;

  @JsonProperty("stopLoss")
  private String stopLoss;

  @JsonProperty("trailingStop")
  private String trailingStop;


  @JsonProperty("sessionAvgPrice")
  private String sessionAvgPrice;

  @JsonProperty("delta")
  private String delta;

  @JsonProperty("gamma")
  private String gamma;

  @JsonProperty("vega")
  private String vega;

  @JsonProperty("theta")
  private String theta;

  @JsonProperty("unrealisedPnl")
  private BigDecimal unrealisedPnl;

  @JsonProperty("curRealisedPnl")
  private BigDecimal curRealisedPnl;

  @JsonProperty("cumRealisedPnl")
  private BigDecimal cumRealisedPnl;

  @JsonProperty("adlRankIndicator")
  private Integer adlRankIndicator;

  @JsonProperty("isReduceOnly")
  private Boolean isReduceOnly;

  @JsonProperty("mmrSysUpdatedTime")
  private String mmrSysUpdatedTime;

  @JsonProperty("leverageSysUpdatedTime")
  private String leverageSysUpdatedTime;

  @JsonProperty("createdTime")
  private String createdTime;

  @JsonProperty("updatedTime")
  private String updatedTime;

  @JsonProperty("seq")
  private Long seq;

}
