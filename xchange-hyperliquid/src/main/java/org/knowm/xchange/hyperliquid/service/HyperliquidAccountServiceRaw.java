package org.knowm.xchange.hyperliquid.service;

import org.knowm.xchange.hyperliquid.HyperliquidExchange;
import org.knowm.xchange.hyperliquid.dto.account.HyperliquidClearinghouseState;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Raw implementation of account service for Hyperliquid.
 * Provides direct access to Hyperliquid API responses.
 */
public class HyperliquidAccountServiceRaw extends HyperliquidBaseService {

    public HyperliquidAccountServiceRaw(HyperliquidExchange exchange) {
        super(exchange);
    }

    /**
     * Get the clearinghouse state (perpetuals account summary) for a user.
     * This is a public endpoint that requires the user's wallet address.
     *
     * @param userAddress The user's wallet address in 42-character hexadecimal format
     * @return The clearinghouse state containing positions, margin summaries, etc.
     * @throws IOException if an I/O error occurs
     */
    public HyperliquidClearinghouseState getClearinghouseState(String userAddress) throws IOException {
        Map<String, Object> request = new HashMap<>();
        request.put("type", "clearinghouseState");
        request.put("user", userAddress);

        return hyperliquidInfo.getClearinghouseState("application/json", request);
    }

    /**
     * Get the clearinghouse state using the configured wallet address from exchange specification.
     *
     * @return The clearinghouse state containing positions, margin summaries, etc.
     * @throws IOException if an I/O error occurs
     * @throws IllegalStateException if no wallet address is configured
     */
    public HyperliquidClearinghouseState getClearinghouseState() throws IOException {
        String walletAddress = (String) (exchange.getExchangeSpecification().getExchangeSpecificParameters().get("vault")!=null ? exchange.getExchangeSpecification().getExchangeSpecificParameters().get("vault") : exchange.getExchangeSpecification().getExchangeSpecificParameters().get("wallet"));

        if (walletAddress == null || walletAddress.isEmpty()) {
            throw new IllegalStateException("Wallet address not configured. " +
                    "Set 'wallet' in exchange specific parameters.");
        }

        return getClearinghouseState(walletAddress);
    }
}
