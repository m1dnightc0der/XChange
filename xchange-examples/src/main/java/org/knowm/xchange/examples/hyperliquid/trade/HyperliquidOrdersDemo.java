package org.knowm.xchange.examples.hyperliquid.trade;

import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import info.bitrich.xchangestream.core.StreamingTradeService;
import info.bitrich.xchangestream.hyperliquid.HyperliquidStreamingExchange;
import org.knowm.xchange.Exchange;
import org.knowm.xchange.ExchangeFactory;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.dto.trade.OpenOrders;
import org.knowm.xchange.hyperliquid.HyperliquidExchange;
import org.knowm.xchange.hyperliquid.dto.trade.HyperliquidTradeParams;
import org.knowm.xchange.service.trade.TradeService;
import org.knowm.xchange.service.trade.params.orders.ClientOrderIdQueryParamInstrument;
import org.knowm.xchange.service.trade.params.orders.DefaultQueryClientOrderIDParamInstrument;
import org.knowm.xchange.service.trade.params.orders.DefaultQueryOrderParamInstrument;
import org.knowm.xchange.service.trade.params.orders.OrderQueryParamInstrument;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Demonstrates how to use Hyperliquid trading functionality using the HyperliquidTradeService
 * This example covers both getOpenOrders() and placeLimitOrder() methods
 * Based on the Python SDK basic_order.py example
 */
public class HyperliquidOrdersDemo {

  public static void main(String[] args) throws IOException {

    try {
      // Create exchange using demo utils - reads credentials from environment variables
      Exchange hyperliquidExchange = createAuthenticatedExchange();
      StreamingExchange hyperliquidStreamingExchange = createAuthenticatedStreamingExchange();
      // First demonstrate getting open orders
      //demonstrateGetOpenOrders(hyperliquidExchange);
      
      // Then demonstrate placing a limit order (similar to Python basic_order.py)
      demonstrateStreamingPlaceLimitOrder(hyperliquidStreamingExchange);

      //demonstratePlaceLimitOrder(hyperliquidExchange);
      demonstrateGetOpenOrders(hyperliquidExchange);
      demonstrateCancelOpenOrders(hyperliquidExchange);
    } catch (Exception e) {
      System.err.println("Error in main: " + e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Create an authenticated Hyperliquid exchange instance
   * Reads credentials from environment variables for security
   */
  private static Exchange createAuthenticatedExchange() {
    ExchangeSpecification exSpec = new ExchangeSpecification(HyperliquidExchange.class);
    
    // Read credentials from environment variables for security
    // Set these before running: export HYPERLIQUID_PRIVATE_KEY="your_private_key"

    
    // The private key is used as the secret key for authentication
    //exSpec.setSecretKey("");

     // exSpec.setApiKey("");


    exSpec.setSecretKey("");
    exSpec.setApiKey("");
    exSpec.setExchangeSpecificParametersItem("wallet", "");
    // Optional: Set to true for testnet, false for mainnet
     exSpec.setExchangeSpecificParametersItem("Use_Sandbox", false);
    // Optional: Vault address if trading on behalf of a vault

       // exSpec.setExchangeSpecificParametersItem("vaultAddress", "");


    return ExchangeFactory.INSTANCE.createExchange(exSpec);
  }

  private static StreamingExchange createAuthenticatedStreamingExchange() {



    ExchangeSpecification spec =
            StreamingExchangeFactory.INSTANCE
                    .createExchange(HyperliquidStreamingExchange.class)
                    .getDefaultExchangeSpecification();

    spec.setSecretKey("");
    spec.setApiKey("");
    spec.setExchangeSpecificParametersItem("wallet", "");
    // Optional: Set to true for testnet, false for mainnet
    spec.setExchangeSpecificParametersItem("Use_Sandbox", false);

    HyperliquidStreamingExchange exchange =
            (HyperliquidStreamingExchange) StreamingExchangeFactory.INSTANCE.createExchange(spec);

    exchange.connect().blockingAwait();


    return exchange;
  }

  /**
   * Demonstrates retrieving open orders using the HyperliquidTradeService
   */
   private static void demonstrateGetOpenOrders(Exchange hyperliquidExchange) throws IOException {
    
    System.out.println("=== Hyperliquid Open Orders Demo ===");
    System.out.println("Getting trade service...");
    
    TradeService tradeService = hyperliquidExchange.getTradeService();
    
    try {
      System.out.println("Fetching open orders...");
      
      // Call the getOpenOrders method we implemented in HyperliquidTradeService
      OpenOrders openOrders = tradeService.getOpenOrders();
      
      System.out.println("Open Orders Response: " + openOrders);
      
      // Display order details if any orders exist
      List<? extends Order> orderList = openOrders.getOpenOrders();
      if (orderList.isEmpty()) {
        System.out.println("No open orders found.");
      } else {
        System.out.println("Found " + orderList.size() + " open order(s):");
        
        for (int i = 0; i < orderList.size(); i++) {
          Order order = orderList.get(i);
          System.out.println("Order " + (i + 1) + ":");
          System.out.println("  ID: " + order.getId());
          System.out.println("  Type: " + order.getType());
          System.out.println("  Instrument: " + order.getInstrument());
          System.out.println("  Amount: " + order.getOriginalAmount());
          if (order instanceof org.knowm.xchange.dto.trade.LimitOrder) {
            org.knowm.xchange.dto.trade.LimitOrder limitOrder = (org.knowm.xchange.dto.trade.LimitOrder) order;
            System.out.println("  Limit Price: " + limitOrder.getLimitPrice());
          }
          System.out.println("  Status: " + order.getStatus());
          System.out.println("  Timestamp: " + order.getTimestamp());
          System.out.println();
        }
      }
      
    } catch (Exception e) {
      System.err.println("Error fetching open orders: " + e.getMessage());
      e.printStackTrace();
      
      // Check if it's due to missing authentication
      if (e.getMessage() != null && e.getMessage().contains("auth")) {
        System.err.println("\nNote: Make sure to set the HYPERLIQUID_PRIVATE_KEY environment variable");
        System.err.println("Example: export HYPERLIQUID_PRIVATE_KEY=\"0x1234...your_private_key\"");
      }
    }
    
    System.out.println("Finished demonstrateGetOpenOrders method");
  }



  private static void demonstrateCancelOpenOrders(Exchange hyperliquidExchange) throws IOException {

    System.out.println("=== Hyperliquid Cancel Orders Demo ===");
    System.out.println("Getting trade service...");

    TradeService tradeService = hyperliquidExchange.getTradeService();

    try {
      System.out.println("Fetching open orders...");

      // Call the getOpenOrders method we implemented in HyperliquidTradeService
      OpenOrders openOrders = tradeService.getOpenOrders();

      System.out.println("Open Orders Response: " + openOrders);

      // Display order details if any orders exist
      List<? extends Order> orderList = openOrders.getOpenOrders();
      if (orderList.isEmpty()) {
        System.out.println("No open orders found.");
      } else {
        System.out.println("Found " + orderList.size() + " open order(s):");

        for (int i = 0; i < orderList.size(); i++) {
          Order order = orderList.get(i);
          System.out.println("Order " + (i + 1) + ":");
          System.out.println("  ID: " + order.getId());
          System.out.println("  Type: " + order.getType());
          System.out.println("  Instrument: " + order.getInstrument());
          System.out.println("  Amount: " + order.getOriginalAmount());
          if (order instanceof org.knowm.xchange.dto.trade.LimitOrder) {
            org.knowm.xchange.dto.trade.LimitOrder limitOrder = (org.knowm.xchange.dto.trade.LimitOrder) order;
            System.out.println("  Limit Price: " + limitOrder.getLimitPrice());
          }
          System.out.println("  Status: " + order.getStatus());
          System.out.println("  Timestamp: " + order.getTimestamp());
          System.out.println();
          List<OrderQueryParamInstrument> params = new ArrayList<OrderQueryParamInstrument>();
          params.add(new DefaultQueryOrderParamInstrument(order.getInstrument(), order.getId()));

          Collection<Order> orders = tradeService.getOrder(params.toArray(new OrderQueryParamInstrument[params.size()]));

          List<ClientOrderIdQueryParamInstrument> clientOidparams = new ArrayList<ClientOrderIdQueryParamInstrument>();
          clientOidparams.add(new DefaultQueryClientOrderIDParamInstrument(order.getInstrument(), order.getUserReference()));

           orders = tradeService.getOrder(clientOidparams.toArray(new ClientOrderIdQueryParamInstrument[clientOidparams.size()]));




          HyperliquidTradeParams.HyperliquidCancelOrderParams req =
                  new HyperliquidTradeParams.HyperliquidCancelOrderParams(order.getInstrument(), order.getId());
          boolean cancelOrder = tradeService.cancelOrder(req);

          orders = tradeService.getOrder(params.toArray(new OrderQueryParamInstrument[params.size()]));

        }

        List<OrderQueryParamInstrument> params = new ArrayList<OrderQueryParamInstrument>();

        params.add(new DefaultQueryOrderParamInstrument( new CurrencyPair("ETH", "USD"), "192129729628"));

        Collection<Order> orders = tradeService.getOrder(params.toArray(new OrderQueryParamInstrument[params.size()]));
        System.err.println("Error cancelling open orders: " +orders );

        }
      

    } catch (Exception e) {
      System.err.println("Error cancelling open orders: " + e.getMessage());
      e.printStackTrace();

      // Check if it's due to missing authentication
      if (e.getMessage() != null && e.getMessage().contains("auth")) {
        System.err.println("\nNote: Make sure to set the HYPERLIQUID_PRIVATE_KEY environment variable");
        System.err.println("Example: export HYPERLIQUID_PRIVATE_KEY=\"0x1234...your_private_key\"");
      }
    }

    List<OrderQueryParamInstrument> params = new ArrayList<OrderQueryParamInstrument>();

    params.add(new DefaultQueryOrderParamInstrument( new CurrencyPair("ETH", "USD"), "192129729628"));

    Collection<Order> orders = tradeService.getOrder(params.toArray(new OrderQueryParamInstrument[params.size()]));
    System.err.println("Error cancelling open orders: " +orders );

    System.out.println("Finished demonstrateCancelOpenOrders method");
  }

  /**
   * Demonstrates placing a limit order using the HyperliquidTradeService
   * Based on the Python basic_order.py example: order("ETH", True, 0.2, 1100, {"limit": {"tif": "Gtc"}})
   */


  private static void demonstrateStreamingPlaceLimitOrder(StreamingExchange hyperliquidStreamingExchange) throws IOException {

    System.out.println("\n=== Hyperliquid Place Limit Order Demo ===");

    StreamingTradeService tradeService = hyperliquidStreamingExchange.getStreamingTradeService();



    try {
      // Create a limit order similar to the Python example
      // Python: order("ETH", True, 0.2, 1100, {"limit": {"tif": "Gtc"}})
      // This places a buy order for 0.2 ETH at $1100 (very low price to ensure it rests)

      FuturesContract ethUsd = new FuturesContract(new CurrencyPair("ETH", "USD"), "PERPETUAL");

      LimitOrder limitOrder = new LimitOrder.Builder(Order.OrderType.ASK, ethUsd)
              .originalAmount(new BigDecimal("0.003"))        // Size: 0.2 ETH
              .limitPrice(new BigDecimal("5800"))
              .userReference("77dd55")// Price: $1100 (very low to ensure it rests)
              .build();

      System.out.println("Placing limit order:");
      System.out.println("  Type: BUY");
      System.out.println("  Currency Pair: " + ethUsd);
      System.out.println("  Amount: " + limitOrder.getOriginalAmount() + " ETH");
      System.out.println("  Limit Price: $" + limitOrder.getLimitPrice());
      System.out.println("  Order Type: GTC (Good Till Cancelled)");

      // Place the order
      String orderId = tradeService.placeLimitOrder(limitOrder);

      System.out.println("Order placed successfully!");
      System.out.println("Order ID: " + orderId);

      // In a real implementation, you would:
      // 1. Parse the response to extract the actual order ID
      // 2. Query order status using the order ID
      // 3. Optionally cancel the order if it's just for testing

      System.out.println("\nNote: This is a demonstration order with a very low price ($1100 for ETH)");
      System.out.println("The order should rest in the order book and not fill immediately.");
      System.out.println("In a production scenario, you would typically cancel test orders afterwards.");

    } catch (Exception e) {
      System.err.println("Error placing limit order: " + e.getMessage());
      e.printStackTrace();

      // Check for common issues
      if (e.getMessage() != null) {
        if (e.getMessage().contains("auth")) {
          System.err.println("\nNote: Authentication error - check your private key and vault address");
        } else if (e.getMessage().contains("balance") || e.getMessage().contains("margin")) {
          System.err.println("\nNote: Insufficient balance or margin to place order");
        } else if (e.getMessage().contains("price")) {
          System.err.println("\nNote: Price validation error - check minimum price requirements");
        }
      }
    }
  }


  private static void demonstratePlaceLimitOrder(Exchange hyperliquidExchange) throws IOException {
    
    System.out.println("\n=== Hyperliquid Place Limit Order Demo ===");
    
    TradeService tradeService = hyperliquidExchange.getTradeService();



    try {
      // Create a limit order similar to the Python example
      // Python: order("ETH", True, 0.2, 1100, {"limit": {"tif": "Gtc"}})
      // This places a buy order for 0.2 ETH at $1100 (very low price to ensure it rests)
      
      CurrencyPair ethUsd = new CurrencyPair("ETH", "USD");
      
      LimitOrder limitOrder = new LimitOrder.Builder(Order.OrderType.ASK, ethUsd)
          .originalAmount(new BigDecimal("0.003"))        // Size: 0.2 ETH
          .limitPrice(new BigDecimal("5800"))           // Price: $1100 (very low to ensure it rests)
          .build();
      
      System.out.println("Placing limit order:");
      System.out.println("  Type: BUY");
      System.out.println("  Currency Pair: " + ethUsd);
      System.out.println("  Amount: " + limitOrder.getOriginalAmount() + " ETH");
      System.out.println("  Limit Price: $" + limitOrder.getLimitPrice());
      System.out.println("  Order Type: GTC (Good Till Cancelled)");
      
      // Place the order
      String orderId = tradeService.placeLimitOrder(limitOrder);
      
      System.out.println("Order placed successfully!");
      System.out.println("Order ID: " + orderId);
      
      // In a real implementation, you would:
      // 1. Parse the response to extract the actual order ID
      // 2. Query order status using the order ID
      // 3. Optionally cancel the order if it's just for testing
      
      System.out.println("\nNote: This is a demonstration order with a very low price ($1100 for ETH)");
      System.out.println("The order should rest in the order book and not fill immediately.");
      System.out.println("In a production scenario, you would typically cancel test orders afterwards.");
      
    } catch (Exception e) {
      System.err.println("Error placing limit order: " + e.getMessage());
      e.printStackTrace();
      
      // Check for common issues
      if (e.getMessage() != null) {
        if (e.getMessage().contains("auth")) {
          System.err.println("\nNote: Authentication error - check your private key and vault address");
        } else if (e.getMessage().contains("balance") || e.getMessage().contains("margin")) {
          System.err.println("\nNote: Insufficient balance or margin to place order");
        } else if (e.getMessage().contains("price")) {
          System.err.println("\nNote: Price validation error - check minimum price requirements");
        }
      }
    }
  }
}
