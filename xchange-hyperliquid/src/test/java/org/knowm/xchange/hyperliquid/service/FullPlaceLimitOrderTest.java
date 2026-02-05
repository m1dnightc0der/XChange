package org.knowm.xchange.hyperliquid.service;

import com.fasterxml.jackson.databind.ObjectMapper;
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
 * Full integration-style test for placeLimitOrder
 */
public class FullPlaceLimitOrderTest {

    private static final String TEST_PRIVATE_KEY = "";
    private static final String TEST_VAULT_ADDRESS = "";

    @Test
    public void testActualOrderPlacement() throws Exception {
        System.out.println("=== Full PlaceLimitOrder Test ===");
        System.out.println("Testing: ETH buy 0.003 at 3800");
        System.out.println("Vault: " + TEST_VAULT_ADDRESS);

        // Create auth
        SynchronizedValueFactory<Long> nonceFactory = mock(SynchronizedValueFactory.class);
        when(nonceFactory.createValue()).thenReturn(System.currentTimeMillis());

        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonceFactory, false, TEST_VAULT_ADDRESS,null);

        // Show what the complete request would look like
        java.util.Map<String, Object> limitType = new java.util.LinkedHashMap<>();
        limitType.put("tif", "Gtc");
        java.util.Map<String, Object> orderType = new java.util.LinkedHashMap<>();
        orderType.put("limit", limitType);

        java.util.Map<String, Object> orderWire = new java.util.LinkedHashMap<>();
        orderWire.put("a", 1);  // ETH
        orderWire.put("b", true);  // buy
        orderWire.put("p", "3800");
        orderWire.put("s", "0.003");
        orderWire.put("r", false);
        orderWire.put("t", orderType);

        java.util.Map<String, Object> action = new java.util.LinkedHashMap<>();
        action.put("type", "order");
        action.put("orders", java.util.Arrays.asList(orderWire));
        action.put("grouping", "na");

        long nonce = System.currentTimeMillis();
        java.util.Map<String, Object> signature = auth.signL1Action(action, nonce, null);

        java.util.Map<String, Object> requestBody = new java.util.LinkedHashMap<>();
        requestBody.put("action", action);
        requestBody.put("nonce", nonce);
        requestBody.put("signature", signature);
        requestBody.put("vaultAddress", TEST_VAULT_ADDRESS);
        requestBody.put("expiresAfter", null);

        ObjectMapper mapper = new ObjectMapper();
        String requestJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestBody);

        System.out.println("\nComplete request body that would be sent:");
        System.out.println(requestJson);

        System.out.println("\nRequest body structure:");
        System.out.println("- action.type: " + action.get("type"));
        System.out.println("- action.orders[0].a (asset): " + orderWire.get("a"));
        System.out.println("- action.orders[0].b (is_buy): " + orderWire.get("b"));
        System.out.println("- action.orders[0].p (price): " + orderWire.get("p"));
        System.out.println("- action.orders[0].s (size): " + orderWire.get("s"));
        System.out.println("- nonce: " + nonce);
        System.out.println("- signature.r: " + signature.get("r"));
        System.out.println("- signature.s: " + signature.get("s"));
        System.out.println("- signature.v: " + signature.get("v"));
        System.out.println("- vaultAddress: " + TEST_VAULT_ADDRESS);

        // Now compare with what Python would send
        System.out.println("\n=== For comparison, run this Python code: ===");
        System.out.println("from hyperliquid.exchange import Exchange");
        System.out.println("from eth_account import Account");
        System.out.println("wallet = Account.from_key('" + TEST_PRIVATE_KEY + "')");
        System.out.println("exchange = Exchange(wallet, base_url='https://api.hyperliquid-testnet.xyz', vault_address='" + TEST_VAULT_ADDRESS + "')");
        System.out.println("result = exchange.order('ETH', True, 0.003, 3800.0, {'limit': {'tif': 'Gtc'}}, False)");
        System.out.println("print(result)");
    }
}
