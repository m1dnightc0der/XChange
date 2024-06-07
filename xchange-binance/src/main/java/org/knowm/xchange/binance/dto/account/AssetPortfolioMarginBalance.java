package org.knowm.xchange.binance.dto.account;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import org.knowm.xchange.currency.Currency;

public class AssetPortfolioMarginBalance {
  private final Currency currency;
  private final BigDecimal totalWalletBalance;
  private final BigDecimal crossMarginAsset;
  private final BigDecimal crossMarginBorrowed;
  private final BigDecimal crossMarginFree;
  private final BigDecimal crossMarginInterest;
  private final BigDecimal crossMarginLocked;
  private final BigDecimal umWalletBalance;
  private final BigDecimal umUnrealizedPNL;
  private final BigDecimal cmWalletBalance;
  private final BigDecimal cmUnrealizedPNL;

  private final BigDecimal negativeBalance;
  private final Long updateTime;

  public AssetPortfolioMarginBalance(
      @JsonProperty("asset") String asset,
      @JsonProperty("totalWalletBalance") BigDecimal totalWalletBalance,
      @JsonProperty("crossMarginAsset") BigDecimal crossMarginAsset,
      @JsonProperty("crossMarginBorrowed") BigDecimal crossMarginBorrowed,
      @JsonProperty("crossMarginFree") BigDecimal crossMarginFree,
      @JsonProperty("crossMarginInterest") BigDecimal crossMarginInterest,
      @JsonProperty("crossMarginLocked") BigDecimal crossMarginLocked,
      @JsonProperty("umWalletBalance") BigDecimal umWalletBalance,
      @JsonProperty("umUnrealizedPNL") BigDecimal umUnrealizedPNL,
      @JsonProperty("cmWalletBalance") BigDecimal cmWalletBalance,
      @JsonProperty("cmUnrealizedPNL") BigDecimal cmUnrealizedPNL,
      @JsonProperty("negativeBalance") BigDecimal negativeBalance,

      @JsonProperty("updateTime") Long updateTime) {
    this.currency = Currency.getInstance(asset);
    this.totalWalletBalance = totalWalletBalance;
    this.crossMarginAsset = crossMarginAsset;
    this.crossMarginBorrowed = crossMarginBorrowed;
    this.crossMarginFree = crossMarginFree;
    this.crossMarginInterest = crossMarginInterest;
    this.crossMarginLocked = crossMarginLocked;
    this.umWalletBalance = umWalletBalance;
    this.umUnrealizedPNL = umUnrealizedPNL;
    this.cmWalletBalance = cmWalletBalance;
    this.cmUnrealizedPNL = cmUnrealizedPNL;
    this.negativeBalance=negativeBalance;
    this.updateTime = updateTime;
  }

  public Currency getCurrency() {
    return currency;
  }
  public BigDecimal getTotalWalletBalance() {
    return totalWalletBalance;
  }

  public BigDecimal getCrossMarginAsset() {
    return crossMarginAsset;
  }

  public BigDecimal getCrossMarginBorrowed() {
    return crossMarginBorrowed;
  }

  public BigDecimal getCrossMarginFree() {
    return crossMarginFree;
  }

  public BigDecimal getCrossMarginInterest() {
    return crossMarginInterest;
  }

  public BigDecimal getCrossMarginLocked() {
    return crossMarginLocked;
  }

  public BigDecimal getUMWalletBalance() {
    return umWalletBalance;
  }

  public BigDecimal getUMUnrealizedPNL() {
    return umUnrealizedPNL;
  }

  public BigDecimal getCMWalletBalance() {
    return cmWalletBalance;
  }

  public BigDecimal getCMUnrealizedPNL() {
    return cmUnrealizedPNL;
  }

  public BigDecimal getNegativeBalance() {
    return negativeBalance;
  }

  public Long getUpdateTime() {
    return updateTime;
  }

  @Override
  public String toString() {
    return "AssetPortfolioMarginBalance{"
        + "currency = '"
        + currency
        + '\''
        + ",totalWalletBalance = '"
        + totalWalletBalance
        + '\''
        + ",crossMarginAsset = '"
        + crossMarginAsset
        + '\''
        + ",crossMarginBorrowed = '"
        + crossMarginBorrowed
        + '\''
        + ",crossMarginFree = '"
        + crossMarginFree
        + '\''
        + ",crossMarginInterest = '"
        + crossMarginInterest
        + '\''
        + ",crossMarginLocked = '"
        + crossMarginLocked
        + '\''
        + ",umWalletBalance = '"
        + umWalletBalance
        + '\''
        + ",umUnrealizedPNL = '"
        + umUnrealizedPNL
        + '\''
        + ",cmWalletBalance = '"
        + cmWalletBalance
        + '\''
        + ",cmUnrealizedPNL = '"
        + cmUnrealizedPNL
        + '\''
        + ",updateTime = '"
        + updateTime
        + "}";
  }
}
