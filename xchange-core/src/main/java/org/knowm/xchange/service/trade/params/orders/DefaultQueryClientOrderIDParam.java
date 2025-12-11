package org.knowm.xchange.service.trade.params.orders;

public class DefaultQueryClientOrderIDParam implements OrderQueryParams {

  private String clientOrderId;

  public DefaultQueryClientOrderIDParam() {}

  public DefaultQueryClientOrderIDParam(String clientOrderId) {

    this.clientOrderId = clientOrderId;
  }

  @Override
  public String getOrderId() {

    return clientOrderId;
  }

  @Override
  public void setOrderId(String clientOrderId) {

    this.clientOrderId = clientOrderId;
  }
}
