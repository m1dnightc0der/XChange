package org.knowm.xchange.examples.hyperliquid;

import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import info.bitrich.xchangestream.hyperliquid.HyperliquidStreamingExchange;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.hyperliquid.HyperliquidExchange;

public class HyperliquidDemoUtils {

    /**
     * Create a Hyperliquid REST exchange for mainnet
     * @return configured Exchange instance
     */
    public static Exchange createExchange() {
        return createExchange(false);
    }
    
    /**
     * Create a Hyperliquid REST exchange
     * @param useTestnet true for testnet, false for mainnet
     * @return configured Exchange instance
     */
    public static Exchange createExchange(boolean useTestnet) {
        // Use the factory to get Hyperliquid exchange
        Exchange exchange = ExchangeFactory.INSTANCE.createExchange(HyperliquidExchange.class);
        ExchangeSpecification spec = exchange.getExchangeSpecification();
        
        // Configure for testnet or mainnet
        if (useTestnet) {
            spec.setExchangeSpecificParametersItem("Use_Sandbox", true);
        }
        
        // Apply specification
        exchange.applySpecification(spec);
        
        return exchange;
    }

    /**
     * Create a Hyperliquid streaming exchange for mainnet
     * @return configured StreamingExchange instance
     */
    public static StreamingExchange createStreamingExchange() {
        return createStreamingExchange(false);
    }
    
    /**
     * Create a Hyperliquid streaming exchange
     * @param useTestnet true for testnet, false for mainnet
     * @return configured StreamingExchange instance
     */
    public static StreamingExchange createStreamingExchange(boolean useTestnet) {
        // Use the factory to get Hyperliquid streaming exchange
        StreamingExchange exchange = StreamingExchangeFactory.INSTANCE.createExchange(HyperliquidStreamingExchange.class);
        ExchangeSpecification spec = exchange.getExchangeSpecification();
        
        // Configure for testnet or mainnet
        if (useTestnet) {
            spec.setExchangeSpecificParametersItem("Use_Sandbox", true);
        }
        
        // Apply specification
        exchange.applySpecification(spec);
        
        return exchange;
    }
}