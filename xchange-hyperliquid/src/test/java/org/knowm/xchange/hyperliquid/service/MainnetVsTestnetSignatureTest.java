package org.knowm.xchange.hyperliquid.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import si.mazi.rescu.SynchronizedValueFactory;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test to verify mainnet vs testnet signature differences
 */
public class MainnetVsTestnetSignatureTest {

    private static final String TEST_PRIVATE_KEY = "";
    private static final String TEST_VAULT_ADDRESS = "";
    private static final long FIXED_NONCE = 1759255552068L;

    @Test
    public void testMainnetVsTestnetSignatures() throws Exception {
        System.out.println("=== Testing Mainnet vs Testnet Signature Differences ===\n");

        // Create nonce factory
        SynchronizedValueFactory<Long> nonceFactory = mock(SynchronizedValueFactory.class);
        when(nonceFactory.createValue()).thenReturn(FIXED_NONCE);

        // Create auth for TESTNET
        HyperliquidAuth testnetAuth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonceFactory, false, TEST_VAULT_ADDRESS,null);

        // Create auth for MAINNET
        HyperliquidAuth mainnetAuth = HyperliquidAuth.createHyperliquidAuth(
            TEST_PRIVATE_KEY, nonceFactory, true, TEST_VAULT_ADDRESS,null);

        // Create same order action
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

        // Sign with testnet
        Map<String, Object> testnetSignature = testnetAuth.signL1Action(action, FIXED_NONCE, null);

        // Sign with mainnet
        Map<String, Object> mainnetSignature = mainnetAuth.signL1Action(action, FIXED_NONCE, null);

        ObjectMapper mapper = new ObjectMapper();

        System.out.println("TESTNET Signature:");
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(testnetSignature));

        System.out.println("\nMAINNET Signature:");
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mainnetSignature));

        System.out.println("\nSignatures are different: " +
            !testnetSignature.equals(mainnetSignature));

        System.out.println("\n=== Python comparison ===");
        System.out.println("Run this Python code to verify:\n");
        System.out.println("from hyperliquid.utils.signing import sign_l1_action");
        System.out.println("from eth_account import Account");
        System.out.println("");
        System.out.println("wallet = Account.from_key('" + TEST_PRIVATE_KEY + "')");
        System.out.println("vault_address = '" + TEST_VAULT_ADDRESS + "'");
        System.out.println("nonce = " + FIXED_NONCE);
        System.out.println("");
        System.out.println("action = {");
        System.out.println("    'type': 'order',");
        System.out.println("    'orders': [{");
        System.out.println("        'a': 1, 'b': True, 'p': '3800',");
        System.out.println("        's': '0.003', 'r': False,");
        System.out.println("        't': {'limit': {'tif': 'Gtc'}}");
        System.out.println("    }],");
        System.out.println("    'grouping': 'na'");
        System.out.println("}");
        System.out.println("");
        System.out.println("testnet_sig = sign_l1_action(wallet, action, vault_address, nonce, None, False)");
        System.out.println("mainnet_sig = sign_l1_action(wallet, action, vault_address, nonce, None, True)");
        System.out.println("");
        System.out.println("print('Python TESTNET:', testnet_sig)");
        System.out.println("print('Python MAINNET:', mainnet_sig)");
    }
}
