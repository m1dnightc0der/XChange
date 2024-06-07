package org.knowm.xchange.binance.dto.account;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class PortfolioMarginBorrowResponse {
  public final String id;

  public PortfolioMarginBorrowResponse(@JsonProperty("tranId") String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }
}
