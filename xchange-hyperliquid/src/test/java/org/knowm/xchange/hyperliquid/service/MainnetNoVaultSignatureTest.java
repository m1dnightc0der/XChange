package org.knowm.xchange.hyperliquid.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import si.mazi.rescu.SynchronizedValueFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test signature generation for mainnet with NO vault (matching Python's account_address usage)
 */
public class MainnetNoVaultSignatureTest {

    private static final String TEST_PRIVATE_KEY = "";
    private static final long FIXED_NONCE = 1759255552068L;

    @Test
    public void testMainnetSignatureWithoutVault() throws Exception {
        System.out.println("=== Mainnet Signature WITHOUT Vault (matching Python test.py) ===\n");

        // Create nonce factory
        SynchronizedValueFactory<Long> nonceFactory = mock(SynchronizedValueFactory.class);
        when(nonceFactory.createValue()).thenReturn(FIXED_NONCE);

        // Create auth for MAINNET with NO vault (null)
        HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonceFactory, true, null,null);

        // Create order action for ETH buy 0.003 at 3800
        Map<String, Object> limitType = new LinkedHashMap<>();
        limitType.put("tif", "Gtc");
        Map<String, Object> orderType = new LinkedHashMap<>();
        orderType.put("limit", limitType);

        Map<String, Object> orderWire = new LinkedHashMap<>();
        orderWire.put("a", 1);  // ETH
        orderWire.put("b", true);
        orderWire.put("p", "3800");
        orderWire.put("s", "0.003");
        orderWire.put("r", false);
        orderWire.put("t", orderType);

        Map<String, Object> action = new LinkedHashMap<>();
        action.put("type", "order");
        action.put("orders", java.util.Arrays.asList(orderWire));
        action.put("grouping", "na");

        // Sign with mainnet, no vault
        Map<String, Object> signature = auth.signL1Action(action, FIXED_NONCE, null);

        ObjectMapper mapper = new ObjectMapper();

        System.out.println("Java Signature (mainnet, no vault):");
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(signature));

        System.out.println("\nExpected Python signature (from test.py with account_address):");
        System.out.println("r: 0xf0a99964206ae435f96b10db64f56b113af43e225d4204cfd3e6ecda18b207d9");
        System.out.println("s: 0x1eb31ed41eb705435f5c87c74c4f3b448de17b19289dd3d724a0031e53d0a171");
        System.out.println("v: 27");

        System.out.println("\nThese should match!");
    }
}
