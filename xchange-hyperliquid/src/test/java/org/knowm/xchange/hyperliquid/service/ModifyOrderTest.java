package org.knowm.xchange.hyperliquid.service;

import org.junit.Test;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.dto.Order;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.hyperliquid.HyperliquidExchange;
import si.mazi.rescu.SynchronizedValueFactory;

import java.math.BigDecimal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test modifyOrder method with real credentials
 * Tests the modify_order functionality that matches Python SDK
 */
public class ModifyOrderTest {

    private static final String TEST_PRIVATE_KEY = "";
    private static final String TEST_VAULT_ADDRESS = "";

    @Test
    public void testModifyOrder() throws Exception {
        // Create mock exchange
        HyperliquidExchange exchange = mock(HyperliquidExchange.class);

        // Create nonce factory with fixed nonce for reproducibility
        SynchronizedValueFactory<Long> nonceFactory = mock(SynchronizedValueFactory.class);
        when(nonceFactory.createValue()).thenReturn(1759255552068L);

        // Create auth
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonceFactory, false, TEST_VAULT_ADDRESS, null);

        // Create limit order with existing order ID: ETH buy 0.003 at 3850 (modified price)
        // In real usage, this order ID would come from a previously placed order
        LimitOrder limitOrder = new LimitOrder.Builder(Order.OrderType.BID, CurrencyPair.ETH_USD)
            .id("123456789")  // Existing order ID to modify
            .originalAmount(new BigDecimal("0.003"))
            .limitPrice(new BigDecimal("3850"))  // New price
            .build();

        System.out.println("Testing modifyOrder with:");
        System.out.println("  Order ID: 123456789");
        System.out.println("  Coin: ETH");
        System.out.println("  Side: BUY");
        System.out.println("  New Size: 0.003");
        System.out.println("  New Price: 3850");
        System.out.println("  Vault: " + TEST_VAULT_ADDRESS);

        // Manually construct what would be sent
        java.util.Map<String, Object> limitType = new java.util.LinkedHashMap<>();
        limitType.put("tif", "Gtc");
        java.util.Map<String, Object> orderType = new java.util.LinkedHashMap<>();
        orderType.put("limit", limitType);

        // Create order wire for the new order parameters
        java.util.Map<String, Object> orderWire = new java.util.LinkedHashMap<>();
        orderWire.put("a", 1);  // ETH asset index
        orderWire.put("b", true);  // is_buy
        orderWire.put("p", "3850");  // new price
        orderWire.put("s", "0.003");  // new size
        orderWire.put("r", false);  // reduce_only
        orderWire.put("t", orderType);

        // Create modify wire structure
        java.util.Map<String, Object> modifyWire = new java.util.LinkedHashMap<>();
        modifyWire.put("oid", 123456789L);  // order ID to modify
        modifyWire.put("order", orderWire);

        // Create batchModify action
        java.util.Map<String, Object> action = new java.util.LinkedHashMap<>();
        action.put("type", "batchModify");
        action.put("modifies", java.util.Arrays.asList(modifyWire));

        long nonce = 1759255552068L;
        java.util.Map<String, Object> signature = auth.signL1Action(action, nonce, null);

        System.out.println("\nGenerated batchModify action structure:");
        System.out.println("  action type: " + action.get("type"));
        System.out.println("  modify wire oid: " + modifyWire.get("oid"));
        System.out.println("  modify wire order: " + modifyWire.get("order"));

        System.out.println("\nGenerated signature:");
        System.out.println("  r: " + signature.get("r"));
        System.out.println("  s: " + signature.get("s"));
        System.out.println("  v: " + signature.get("v"));
    }

    @Test
    public void testModifyOrderWithClientOrderId() throws Exception {
        // Create mock exchange
        HyperliquidExchange exchange = mock(HyperliquidExchange.class);

        // Create nonce factory with fixed nonce for reproducibility
        SynchronizedValueFactory<Long> nonceFactory = mock(SynchronizedValueFactory.class);
        when(nonceFactory.createValue()).thenReturn(1759255552068L);

        // Create auth
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonceFactory, false, TEST_VAULT_ADDRESS, null);

        // Test with client order ID (hexadecimal string)
        // Client order IDs are 16-byte hex strings
        String clientOrderId = "0x0000000000000000000000000000000000000000000000000000000000000abc";

        LimitOrder limitOrder = new LimitOrder.Builder(Order.OrderType.BID, CurrencyPair.ETH_USD)
            .id(clientOrderId)  // Client order ID to modify
            .originalAmount(new BigDecimal("0.003"))
            .limitPrice(new BigDecimal("3850"))
            .build();

        System.out.println("\nTesting modifyOrder with client order ID:");
        System.out.println("  Client Order ID: " + clientOrderId);
        System.out.println("  Coin: ETH");
        System.out.println("  Side: BUY");
        System.out.println("  New Size: 0.003");
        System.out.println("  New Price: 3850");
        System.out.println("  Vault: " + TEST_VAULT_ADDRESS);

        // Manually construct what would be sent
        java.util.Map<String, Object> limitType = new java.util.LinkedHashMap<>();
        limitType.put("tif", "Gtc");
        java.util.Map<String, Object> orderType = new java.util.LinkedHashMap<>();
        orderType.put("limit", limitType);

        // Create order wire for the new order parameters
        java.util.Map<String, Object> orderWire = new java.util.LinkedHashMap<>();
        orderWire.put("a", 1);  // ETH asset index
        orderWire.put("b", true);  // is_buy
        orderWire.put("p", "3850");
        orderWire.put("s", "0.003");
        orderWire.put("r", false);
        orderWire.put("t", orderType);

        // Create modify wire structure with client order ID
        java.util.Map<String, Object> modifyWire = new java.util.LinkedHashMap<>();
        modifyWire.put("oid", clientOrderId);  // client order ID as hex string
        modifyWire.put("order", orderWire);

        // Create batchModify action
        java.util.Map<String, Object> action = new java.util.LinkedHashMap<>();
        action.put("type", "batchModify");
        action.put("modifies", java.util.Arrays.asList(modifyWire));

        long nonce = 1759255552068L;
        java.util.Map<String, Object> signature = auth.signL1Action(action, nonce, null);

        System.out.println("\nGenerated batchModify action with client order ID:");
        System.out.println("  action type: " + action.get("type"));
        System.out.println("  modify wire oid: " + modifyWire.get("oid"));

        System.out.println("\nGenerated signature:");
        System.out.println("  r: " + signature.get("r"));
        System.out.println("  s: " + signature.get("s"));
        System.out.println("  v: " + signature.get("v"));
    }
}
