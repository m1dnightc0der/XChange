package org.knowm.xchange.examples.hyperliquid.trade;

import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.hyperliquid.HyperliquidExchange;
import org.knowm.xchange.hyperliquid.dto.trade.HyperliquidTradeParams;
import org.knowm.xchange.service.trade.TradeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

/**
 * Demonstrates vault trading functionality on Hyperliquid exchange.
 *
 * <p>This example shows how to configure and trade on behalf of a vault using
 * the XChange library. Vault trading allows you to execute orders using a vault's
 * capital rather than your personal account.</p>
 *
 * <h2>Prerequisites</h2>
 * <ul>
 *   <li>You must be the vault leader or have permission to trade on the vault</li>
 *   <li>The vault address must be valid and accessible from your wallet</li>
 *   <li>Your private key must have authorization to sign transactions for the vault</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 * <p>To enable vault trading, set the vault address in the exchange specification:</p>
 * <pre>
 * spec.setExchangeSpecificParametersItem("vaultAddress", "0x...");
 * </pre>
 *
 * <h2>Supported Operations</h2>
 * <p>All trading operations automatically support vault addresses:</p>
 * <ul>
 *   <li>Place orders (REST: {@code placeOrderRaw()}, WebSocket: {@code placeLimitOrder()})</li>
 *   <li>Modify orders (REST: {@code modifyOrderRaw()})</li>
 *   <li>Cancel orders by ID (REST: {@code cancel()})</li>
 *   <li>Cancel orders by client order ID (REST: {@code cancelByCloid()})</li>
 * </ul>
 *
 * <h2>How It Works</h2>
 * <p>When a vault address is configured:</p>
 * <ol>
 *   <li>The vault address is included in the cryptographic signature (action hash)</li>
 *   <li>The vault address is sent in the API request body</li>
 *   <li>All operations execute on the vault instead of your personal account</li>
 * </ol>
 *
 * <h2>Testing</h2>
 * <p>Use the testnet environment for testing vault operations:</p>
 * <pre>
 * ExchangeSpecification spec = exchange.getSandboxExchangeSpecification();
 * </pre>
 *
 * @see org.knowm.xchange.hyperliquid.service.HyperliquidAuth#signL1Action(java.util.Map, long, Long)
 * @see org.knowm.xchange.hyperliquid.HyperliquidAdapters#createSignedRequestBody
 */
public class HyperliquidVaultTradingDemo {

  private static final Logger LOG = LoggerFactory.getLogger(HyperliquidVaultTradingDemo.class);

  public static void main(String[] args) {
    try {
      // Example 1: Trading on Personal Account (No Vault)
      LOG.info("=== Example 1: Personal Account Trading ===");
      demonstratePersonalAccountTrading();

      // Example 2: Trading on Behalf of a Vault
      LOG.info("\n=== Example 2: Vault Trading ===");
      demonstrateVaultTrading();

      // Example 3: WebSocket Vault Trading
      LOG.info("\n=== Example 3: WebSocket Vault Trading ===");
      demonstrateWebSocketVaultTrading();

    } catch (Exception e) {
      LOG.error("Error in vault trading demo", e);
    }
  }

  /**
   * Demonstrates trading on a personal account (no vault).
   * Orders execute using your wallet's capital.
   */
  private static void demonstratePersonalAccountTrading() throws Exception {
    // Create exchange specification
    ExchangeSpecification spec = new HyperliquidExchange().getDefaultExchangeSpecification();

    // Configure authentication
    spec.setSecretKey(getPrivateKey());
    spec.setExchangeSpecificParametersItem("wallet", getWalletAddress());

    // NOTE: No vaultAddress parameter - trades on personal account


    // Create and configure exchange
    HyperliquidExchange exchange = new HyperliquidExchange();
    exchange.applySpecification(spec);

    TradeService tradeService = exchange.getTradeService();

    // Place a limit order on personal account
    LimitOrder order = new LimitOrder.Builder(Order.OrderType.BID, CurrencyPair.ETH_USD)
        .limitPrice(new BigDecimal("2000.00"))
        .originalAmount(new BigDecimal("0.01"))
        .build();

    LOG.info("Placing order on personal account...");
    String orderId = tradeService.placeLimitOrder(order);
    LOG.info("Order placed successfully on personal account. Order ID: {}", orderId);

    // Cancel the order using proper CancelOrderParams
    LOG.info("Cancelling order on personal account...");
    HyperliquidTradeParams.HyperliquidCancelOrderParams cancelParams =
        new HyperliquidTradeParams.HyperliquidCancelOrderParams(CurrencyPair.ETH_USD, orderId);
    tradeService.cancelOrder(cancelParams);
    LOG.info("Order cancelled successfully on personal account");
  }

  /**
   * Demonstrates trading on behalf of a vault using REST API.
   * All operations execute using the vault's capital.
   */
  private static void demonstrateVaultTrading() throws Exception {
    // Create exchange specification
    ExchangeSpecification spec = new HyperliquidExchange().getDefaultExchangeSpecification();

    // Configure authentication
    spec.setSecretKey(getPrivateKey());
    spec.setExchangeSpecificParametersItem("wallet", getWalletAddress());

    // IMPORTANT: Set vault address to trade on vault
    spec.setExchangeSpecificParametersItem("vaultAddress", getVaultAddress());

    // Create and configure exchange
    HyperliquidExchange exchange = new HyperliquidExchange();
    exchange.applySpecification(spec);

    TradeService tradeService = exchange.getTradeService();

    // Place a limit order on vault
    LimitOrder order = new LimitOrder.Builder(Order.OrderType.BID, CurrencyPair.ETH_USD)
        .limitPrice(new BigDecimal("2100.00"))
        .originalAmount(new BigDecimal("0.01"))
        .build();

    LOG.info("Placing order on vault {}...", getVaultAddress());
    String orderId = tradeService.placeLimitOrder(order);
    LOG.info("Order placed successfully on vault. Order ID: {}", orderId);

    // Modify the order on vault
    LimitOrder modifiedOrder = new LimitOrder.Builder(Order.OrderType.BID, CurrencyPair.ETH_USD)
        .limitPrice(new BigDecimal("2150.00"))
        .originalAmount(new BigDecimal("0.02"))
        .id(orderId)
        .build();

   // LOG.info("Modifying order on vault...");
   // tradeService.changeOrder(modifiedOrder);
   // LOG.info("Order modified successfully on vault");

    // Cancel the order on vault using proper CancelOrderParams
    LOG.info("Cancelling order on vault...");
    HyperliquidTradeParams.HyperliquidCancelOrderParams cancelParams =
        new HyperliquidTradeParams.HyperliquidCancelOrderParams(CurrencyPair.ETH_USD, orderId);
    tradeService.cancelOrder(cancelParams);
    LOG.info("Order cancelled successfully on vault");
  }

  /**
   * Demonstrates trading on behalf of a vault using WebSocket API.
   * WebSocket provides lower latency for order placement.
   */
  private static void demonstrateWebSocketVaultTrading() throws Exception {
    // Create streaming exchange specification
    info.bitrich.xchangestream.hyperliquid.HyperliquidStreamingExchange streamingExchange =
        new info.bitrich.xchangestream.hyperliquid.HyperliquidStreamingExchange();

    ExchangeSpecification spec = streamingExchange.getDefaultExchangeSpecification();

    // Configure authentication
    spec.setSecretKey(getPrivateKey());
    spec.setExchangeSpecificParametersItem("wallet", getWalletAddress());

    // IMPORTANT: Set vault address to trade on vault via WebSocket
    spec.setExchangeSpecificParametersItem("vaultAddress", getVaultAddress());


    // Apply specification and connect
    streamingExchange.applySpecification(spec);
    streamingExchange.connect().blockingAwait();

    LOG.info("WebSocket connected to Hyperliquid");

    info.bitrich.xchangestream.core.StreamingTradeService streamingTradeService =
        streamingExchange.getStreamingTradeService();

    // Place a limit order on vault via WebSocket
    LimitOrder order = new LimitOrder.Builder(Order.OrderType.BID, CurrencyPair.ETH_USD)
        .limitPrice(new BigDecimal("2100.00"))
        .originalAmount(new BigDecimal("0.01"))
        .build();

    LOG.info("Placing order on vault {} via WebSocket...", getVaultAddress());
    String orderId = streamingTradeService.placeLimitOrder(order);
    LOG.info("Order placed successfully on vault via WebSocket. Order ID: {}", orderId);

    // Subscribe to order updates
    LOG.info("Subscribing to order updates for vault...");
    streamingTradeService.getOrderChanges(CurrencyPair.ETH_USD)
        .subscribe(
            orderUpdate -> LOG.info("Order update received: {}", orderUpdate),
            error -> LOG.error("Error in order updates", error)
        );

    // Wait a bit to receive updates
    Thread.sleep(5000);

    // Disconnect
    streamingExchange.disconnect().blockingAwait();
    LOG.info("WebSocket disconnected");
  }

  /**
   * Get private key from environment variable or configuration.
   *
   * <p><b>SECURITY WARNING:</b> Never hardcode private keys in source code.
   * Always use environment variables or secure configuration management.</p>
   *
   * @return Private key hex string with 0x prefix
   */
  private static String getPrivateKey() {
    String privateKey = System.getenv("HYPERLIQUID_PRIVATE_KEY");
    if (privateKey == null || privateKey.isEmpty()) {
      throw new IllegalStateException(
          "HYPERLIQUID_PRIVATE_KEY environment variable not set. " +
          "Set it with: export HYPERLIQUID_PRIVATE_KEY=0x..."
      );
    }
    return privateKey;
  }

  /**
   * Get wallet address from environment variable or configuration.
   *
   * <p>This is the Ethereum address derived from your private key.
   * If not provided, it will be automatically derived from the private key.</p>
   *
   * @return Wallet address hex string with 0x prefix
   */
  private static String getWalletAddress() {
    String wallet = System.getenv("HYPERLIQUID_WALLET_ADDRESS");
    if (wallet == null || wallet.isEmpty()) {
      LOG.warn("HYPERLIQUID_WALLET_ADDRESS not set - will be derived from private key");
      return null; // Will be auto-derived
    }
    return wallet;
  }

  /**
   * Get vault address from environment variable or configuration.
   *
   * <p>This must be a vault you lead or have permission to trade on.
   * The vault address format is a 42-character hex string (0x + 40 hex digits).</p>
   *
   * @return Vault address hex string with 0x prefix
   */
  private static String getVaultAddress() {
    String vaultAddress = System.getenv("HYPERLIQUID_VAULT_ADDRESS");
    if (vaultAddress == null || vaultAddress.isEmpty()) {
      throw new IllegalStateException(
          "HYPERLIQUID_VAULT_ADDRESS environment variable not set. " +
          "Set it with: export HYPERLIQUID_VAULT_ADDRESS=0x..."
      );
    }

    // Validate format
    if (!vaultAddress.matches("^0x[a-fA-F0-9]{40}$")) {
      throw new IllegalArgumentException(
          "Invalid vault address format: " + vaultAddress + ". " +
          "Must be 0x followed by 40 hexadecimal characters."
      );
    }

    return vaultAddress;
  }
}
