package org.knowm.xchange.hyperliquid.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import si.mazi.rescu.SynchronizedValueFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test to compare complete request body structure with Python SDK
 */
public class RequestBodyComparisonTest {

    private static final String TEST_PRIVATE_KEY = "";
    private static final String TEST_VAULT_ADDRESS = "";

    @Test
    public void testRequestBodyStructure() throws Exception {
        // Create nonce factory
        SynchronizedValueFactory<Long> nonceFactory = mock(SynchronizedValueFactory.class);
        when(nonceFactory.createValue()).thenReturn(1759255552068L);  // Match Python nonce

        // Create auth
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonceFactory, false, TEST_VAULT_ADDRESS,null);

        // Manually construct what placeOrderRaw creates
        System.out.println("Java placeOrderRaw() request body structure:");

        // Create order type
        Map<String, Object> limitType = new LinkedHashMap<>();
        limitType.put("tif", "Gtc");
        Map<String, Object> orderType = new LinkedHashMap<>();
        orderType.put("limit", limitType);

        Map<String, Object> orderWire = new LinkedHashMap<>();
        orderWire.put("a", 1);  // ETH asset index
        orderWire.put("b", true);
        orderWire.put("p", "3000");  // Match Python formatting
        orderWire.put("s", "0.1");
        orderWire.put("r", false);
        orderWire.put("t", orderType);

        Map<String, Object> action = new LinkedHashMap<>();
        action.put("type", "order");
        action.put("orders", java.util.Arrays.asList(orderWire));
        action.put("grouping", "na");

        long nonce = 1759255552068L;
        Map<String, Object> signature = auth.signL1Action(action, nonce, null);

        Map<String, Object> requestBody = new LinkedHashMap<>();
        requestBody.put("action", action);
        requestBody.put("nonce", nonce);
        requestBody.put("signature", signature);
        requestBody.put("vaultAddress", TEST_VAULT_ADDRESS);
        requestBody.put("expiresAfter", null);

        ObjectMapper mapper = new ObjectMapper();
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(requestBody));

        System.out.println("\n\nExpected from Python:");
        System.out.println("r: 0xd97466a861d92d9cef31dced8546ffe05277d9db8b9ba2c7db5b1be8427ef046");
        System.out.println("s: 0x7801c73c4e1b3aa833e7e87ebc82ba0f6efae39bf2e309c25df3fe66b7800adb");
        System.out.println("v: 27");
    }
}
