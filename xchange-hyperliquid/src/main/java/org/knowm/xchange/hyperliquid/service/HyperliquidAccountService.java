package org.knowm.xchange.hyperliquid.service;

import org.knowm.xchange.dto.account.AccountInfo;
import org.knowm.xchange.dto.account.Wallet;
import org.knowm.xchange.hyperliquid.HyperliquidAdapters;
import org.knowm.xchange.hyperliquid.HyperliquidExchange;
import org.knowm.xchange.hyperliquid.dto.account.HyperliquidClearinghouseState;
import org.knowm.xchange.service.account.AccountService;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;

/**
 * Account service implementation for Hyperliquid exchange.
 * Provides account information including wallet balance and margin status.
 *
 * Note: Open positions are available via TradeService.getOpenPositions()
 */
public class HyperliquidAccountService extends HyperliquidAccountServiceRaw implements AccountService {

    public HyperliquidAccountService(HyperliquidExchange exchange) {
        super(exchange);
    }

    /**
     * Get account information including wallet balance and margin status.
     * Uses the configured wallet address from exchange specification.
     *
     * Note: Use TradeService.getOpenPositions() to retrieve open positions.
     *
     * @return AccountInfo containing wallet information
     * @throws IOException if an I/O error occurs
     */
    @Override
    public AccountInfo getAccountInfo() throws IOException {
        HyperliquidClearinghouseState state = getClearinghouseState();

        Wallet wallet = HyperliquidAdapters.adaptWallet(state);

        // Get timestamp from state
        Date timestamp = state.getTime() != null ? new Date(state.getTime()) : new Date();

        return new AccountInfo(
                timestamp,
                wallet != null ? wallet : new Wallet.Builder().id("perpetuals").balances(Collections.emptyList()).build()
        );
    }

    /**
     * Get account information for a specific wallet address.
     *
     * Note: Use TradeService.getOpenPositions() to retrieve open positions.
     *
     * @param walletAddress The wallet address to query
     * @return AccountInfo containing wallet information
     * @throws IOException if an I/O error occurs
     */
    public AccountInfo getAccountInfo(String walletAddress) throws IOException {
        HyperliquidClearinghouseState state = getClearinghouseState(walletAddress);

        Wallet wallet = HyperliquidAdapters.adaptWallet(state);

        // Get timestamp from state
        Date timestamp = state.getTime() != null ? new Date(state.getTime()) : new Date();

        return new AccountInfo(
                timestamp,
                wallet != null ? wallet : new Wallet.Builder().id("perpetuals").balances(Collections.emptyList()).build()
        );
    }
}
