package org.knowm.xchange.hyperliquid;

import org.knowm.xchange.BaseExchange;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.hyperliquid.service.HyperliquidMarketDataService;
import org.knowm.xchange.hyperliquid.service.HyperliquidTradeService;

public class HyperliquidExchange extends BaseExchange implements Exchange {

  public static final String USE_SANDBOX = "Use_Sandbox";

  @Override
  public void applySpecification(ExchangeSpecification exchangeSpecification) {
    super.applySpecification(exchangeSpecification);
    concludeHostParams(exchangeSpecification);
  }

  @Override
  protected void initServices() {
    concludeHostParams(exchangeSpecification);
    this.marketDataService = new HyperliquidMarketDataService(this);
    this.tradeService = new HyperliquidTradeService(this);
  }

  private static void concludeHostParams(ExchangeSpecification exchangeSpecification) {
    if (exchangeSpecification.getExchangeSpecificParameters() != null) {
      final boolean useSandbox =
          Boolean.TRUE.equals(
              exchangeSpecification.getExchangeSpecificParametersItem(USE_SANDBOX));
      if (useSandbox) {
        exchangeSpecification.setSslUri("https://api.hyperliquid-testnet.xyz/");
        exchangeSpecification.setHost("api.hyperliquid-testnet.xyz");
      }
    }
  }

  @Override
  public ExchangeSpecification getDefaultExchangeSpecification() {
    ExchangeSpecification exchangeSpecification = new ExchangeSpecification(this.getClass());
    exchangeSpecification.setSslUri("https://api.hyperliquid.xyz");
    exchangeSpecification.setHost("api.hyperliquid.xyz");
    exchangeSpecification.setExchangeName("Hyperliquid");
    exchangeSpecification.setExchangeDescription("Hyperliquid is a derivatives exchange");
    return exchangeSpecification;
  }

  public ExchangeSpecification getSandboxExchangeSpecification() {
    ExchangeSpecification exchangeSpecification = new ExchangeSpecification(this.getClass());
    exchangeSpecification.setSslUri("https://api.hyperliquid-testnet.xyz/");
    exchangeSpecification.setHost("api.hyperliquid-testnet.xyz");
    exchangeSpecification.setExchangeSpecificParametersItem(Exchange.USE_SANDBOX, true);
    return exchangeSpecification;
  }

  protected boolean useSandbox() {
    return Boolean.TRUE.equals(
        exchangeSpecification.getExchangeSpecificParametersItem(USE_SANDBOX));
  }
}