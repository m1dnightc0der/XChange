package org.knowm.xchange.hyperliquid.service;

import org.knowm.xchange.client.ExchangeRestProxyBuilder;
import org.knowm.xchange.hyperliquid.Hyperliquid;
import org.knowm.xchange.hyperliquid.HyperliquidAuthenticated;
import org.knowm.xchange.hyperliquid.HyperliquidInfo;
import org.knowm.xchange.hyperliquid.HyperliquidExchange;
import org.knowm.xchange.service.BaseExchangeService;
import org.knowm.xchange.service.BaseService;

/**
 * Base service class for Hyperliquid API interactions
 */
public class HyperliquidBaseService extends BaseExchangeService<HyperliquidExchange>
    implements BaseService {

  protected final Hyperliquid hyperliquid;
  protected final HyperliquidInfo hyperliquidInfo;
  protected final HyperliquidAuthenticated hyperliquidAuthenticated;
  protected final HyperliquidAuth hyperliquidAuth;

  /**
   * Constructor
   *
   * @param exchange
   */
  public HyperliquidBaseService(HyperliquidExchange exchange) {
    super(exchange);
    
    // Public API
    hyperliquid =
        ExchangeRestProxyBuilder.forInterface(Hyperliquid.class, exchange.getExchangeSpecification())
            .build();
    
    // Info API (public info queries)
    hyperliquidInfo = ExchangeRestProxyBuilder.forInterface(
        HyperliquidInfo.class, 
        exchange.getExchangeSpecification())
        .build();
    
    // Authenticated Exchange API 
    hyperliquidAuthenticated = ExchangeRestProxyBuilder.forInterface(
        HyperliquidAuthenticated.class, 
        exchange.getExchangeSpecification())
        .build();
        
    // Authentication handler
    hyperliquidAuth = HyperliquidAuth.createHyperliquidAuth(
        exchange.getExchangeSpecification().getSecretKey(),
        exchange.getNonceFactory(),
        exchange.getExchangeSpecification().getExchangeSpecificParametersItem("Use_Sandbox") != null && ((Boolean) exchange.getExchangeSpecification().getExchangeSpecificParametersItem("Use_Sandbox") )?
            false : true,
        (String) exchange.getExchangeSpecification().getExchangeSpecificParametersItem("vaultAddress"), (String) exchange.getExchangeSpecification().getExchangeSpecificParametersItem("wallet")
    );
  }
}