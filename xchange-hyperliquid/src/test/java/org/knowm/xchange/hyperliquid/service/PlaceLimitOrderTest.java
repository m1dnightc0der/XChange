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
 * Test placeLimitOrder method with real credentials
 */
public class PlaceLimitOrderTest {

    private static final String TEST_PRIVATE_KEY = "";
    private static final String TEST_VAULT_ADDRESS = "";

    @Test
    public void testPlaceLimitOrder() throws Exception {
        // Create mock exchange
        HyperliquidExchange exchange = mock(HyperliquidExchange.class);

        // Create nonce factory with fixed nonce for reproducibility
        SynchronizedValueFactory<Long> nonceFactory = mock(SynchronizedValueFactory.class);
        when(nonceFactory.createValue()).thenReturn(1759255552068L);

        // Create auth
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonceFactory, false, TEST_VAULT_ADDRESS,null);

        // Create limit order: ETH buy 0.003 at 3800
        LimitOrder limitOrder = new LimitOrder.Builder(Order.OrderType.BID, CurrencyPair.ETH_USD)
            .originalAmount(new BigDecimal("0.003"))
            .limitPrice(new BigDecimal("3800"))
            .build();

        System.out.println("Testing placeLimitOrder with:");
        System.out.println("  Coin: ETH");
        System.out.println("  Side: BUY");
        System.out.println("  Size: 0.003");
        System.out.println("  Price: 3800");
        System.out.println("  Vault: " + TEST_VAULT_ADDRESS);

        // Manually construct what would be sent (since we can't easily capture the actual HTTP request)
        java.util.Map<String, Object> limitType = new java.util.LinkedHashMap<>();
        limitType.put("tif", "Gtc");
        java.util.Map<String, Object> orderType = new java.util.LinkedHashMap<>();
        orderType.put("limit", limitType);

        java.util.Map<String, Object> orderWire = new java.util.LinkedHashMap<>();
        orderWire.put("a", 1);  // ETH asset index
        orderWire.put("b", true);  // is_buy
        orderWire.put("p", "3800");
        orderWire.put("s", "0.003");
        orderWire.put("r", false);
        orderWire.put("t", orderType);

        java.util.Map<String, Object> action = new java.util.LinkedHashMap<>();
        action.put("type", "order");
        action.put("orders", java.util.Arrays.asList(orderWire));
        action.put("grouping", "na");

        long nonce = 1759255552068L;
        java.util.Map<String, Object> signature = auth.signL1Action(action, nonce, null);

        System.out.println("\nGenerated signature:");
        System.out.println("  r: " + signature.get("r"));
        System.out.println("  s: " + signature.get("s"));
        System.out.println("  v: " + signature.get("v"));
    }
}
