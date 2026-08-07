package org.knowm.xchange.hyperliquid.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bouncycastle.crypto.digests.KeccakDigest;
import org.bouncycastle.crypto.params.ECPrivateKeyParameters;
import org.bouncycastle.crypto.signers.ECDSASigner;
import org.bouncycastle.math.ec.ECPoint;
import org.bouncycastle.asn1.sec.SECNamedCurves;
import org.bouncycastle.asn1.x9.X9ECParameters;
import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.util.encoders.Hex;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.hyperliquid.HyperliquidAdapters;
import org.knowm.xchange.service.BaseParamsDigest;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import si.mazi.rescu.RestInvocation;
import si.mazi.rescu.SynchronizedValueFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.*;

/**
 * HyperliquidAuth generates EIP-712 signatures for Hyperliquid L1 actions.
 *
 * <p>This class implements the Hyperliquid authentication protocol, which uses EIP-712
 * structured data signing for all trading operations. It supports both personal account
 * trading and vault trading through a unified authentication mechanism.</p>
 *
 * <h2>Vault Trading Support</h2>
 * <p>To trade on behalf of a vault, configure the vault address when creating the exchange:</p>
 * <pre>
 * ExchangeSpecification spec = new HyperliquidExchange().getDefaultExchangeSpecification();
 * spec.setSecretKey("0x...");  // Your private key
 * spec.setExchangeSpecificParametersItem("vaultAddress", "0x...");  // Vault address
 * </pre>
 *
 * <p>When a vault address is configured:</p>
 * <ul>
 *   <li>The vault address is cryptographically bound to the signature (included in action hash)</li>
 *   <li>The vault address is sent in the API request body</li>
 *   <li>All operations execute on the vault instead of your personal account</li>
 * </ul>
 *
 * <h2>Supported Operations</h2>
 * <p>All methods using {@link #createSignedActionRequest(Map, Long)} automatically support vault addresses:</p>
 * <ul>
 *   <li>Place orders - {@code placeOrderRaw()} (REST), {@code placeLimitOrder()} (WebSocket)</li>
 *   <li>Modify orders - {@code modifyOrderRaw()} (REST)</li>
 *   <li>Cancel orders - {@code cancel()}, {@code cancelByCloid()} (REST)</li>
 * </ul>
 *
 * <h2>Signature Algorithm</h2>
 * <p>The signing process follows the Hyperliquid protocol:</p>
 * <ol>
 *   <li>Serialize action with MessagePack</li>
 *   <li>Create action hash: keccak256(msgpack(action) + nonce + vault_flag + vault_address)</li>
 *   <li>Construct phantom agent with action hash</li>
 *   <li>Sign using EIP-712 structured data signature</li>
 * </ol>
 *
 * <p>The vault flag and address are included in the action hash calculation:</p>
 * <pre>
 * if (vaultAddress == null) {
 *     buffer.put((byte) 0x00);  // No vault
 * } else {
 *     buffer.put((byte) 0x01);  // Vault present
 *     buffer.put(hexToBytes(vaultAddress));  // 20-byte address
 * }
 * </pre>
 *
 * <h2>Thread Safety</h2>
 * <p>This class is thread-safe. Multiple threads can safely call {@link
 * #createSignedActionRequest(Map, Long)} concurrently. Each invocation generates a unique nonce
 * using the provided nonce factory.</p>
 *
 * @see org.knowm.xchange.hyperliquid.HyperliquidAdapters#createSignedRequestBody
 * @see org.knowm.xchange.hyperliquid.service.HyperliquidTradeServiceRaw
 * @see info.bitrich.xchangestream.hyperliquid.HyperliquidStreamingTradeService
 */
public class HyperliquidAuth extends BaseParamsDigest {
    
    private final String privateKeyHex;
    private final SynchronizedValueFactory<Long> nonce;
    private final boolean isMainnet;
    private final String vaultAddress;

    private final String walletAddress;
    private final ObjectMapper msgPackMapper;
    private final BigInteger privateKey;
    private final ECDomainParameters domainParams;
    private final ECPoint publicKey;
    
    // EIP-712 Domain for Hyperliquid L1 Actions
    private static final String DOMAIN_NAME = "Exchange";
    private static final String DOMAIN_VERSION = "1";
    private static final int CHAIN_ID = 1337;
    private static final String VERIFYING_CONTRACT = "0x0000000000000000000000000000000000000000";
    
    // EIP-712 Type Hash for Agent
    private static final byte[] AGENT_TYPE_HASH = keccak256("Agent(string source,bytes32 connectionId)".getBytes(StandardCharsets.UTF_8));
    
    // EIP-712 Domain Separator Hash  
    private static final byte[] DOMAIN_SEPARATOR = calculateDomainSeparator();
    
    private static byte[] calculateDomainSeparator() {
        // keccak256(abi.encode(TYPE_HASH, keccak256(name), keccak256(version), chainId, verifyingContract))
        byte[] typeHash = keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)".getBytes(StandardCharsets.UTF_8));
        byte[] nameHash = keccak256(DOMAIN_NAME.getBytes(StandardCharsets.UTF_8));
        byte[] versionHash = keccak256(DOMAIN_VERSION.getBytes(StandardCharsets.UTF_8));

        ByteBuffer buffer = ByteBuffer.allocate(32 * 5);  // 5 32-byte words
        buffer.put(typeHash);           // bytes 0-31
        buffer.put(nameHash);            // bytes 32-63
        buffer.put(versionHash);         // bytes 64-95

        // chainId as uint256 (32 bytes, big endian)
        byte[] chainIdBytes = new byte[32];
        ByteBuffer.wrap(chainIdBytes).putInt(28, CHAIN_ID);  // Put in last 4 bytes
        buffer.put(chainIdBytes);        // bytes 96-127

        // verifyingContract as address (32 bytes, padded on left with zeros)
        byte[] addressBytes = new byte[32];
        byte[] addressRaw = hexToBytes(VERIFYING_CONTRACT);
        System.arraycopy(addressRaw, 0, addressBytes, 32 - addressRaw.length, addressRaw.length);
        buffer.put(addressBytes);        // bytes 128-159

        return keccak256(buffer.array());
    }

    /**
     * Private constructor for HyperliquidAuth.
     *
     * <p>Use {@link #createHyperliquidAuth} factory method to create instances.</p>
     *
     * @param privateKeyHex Private key in hex format (with or without 0x prefix)
     * @param nonce Nonce factory for generating unique nonces
     * @param isMainnet True for mainnet, false for testnet
     * @param vaultAddress Optional vault address (null for personal account trading)
     * @param walletAddress Optional wallet address (null to auto-derive from private key)
     */
    private HyperliquidAuth(String privateKeyHex, SynchronizedValueFactory<Long> nonce,
                           boolean isMainnet, String vaultAddress,String walletAddress) {
        super(privateKeyHex, HMAC_SHA_256); // Dummy algorithm, we override digestParams
        // Normalize private key format (remove 0x prefix if present)
        this.privateKeyHex = privateKeyHex.startsWith("0x") ? privateKeyHex.substring(2) : privateKeyHex;
        BigInteger parsedPrivateKey = null;
        ECDomainParameters parsedDomainParams = null;
        ECPoint parsedPublicKey = null;
        try {
            parsedPrivateKey = new BigInteger(this.privateKeyHex, 16);
            X9ECParameters curveParams = SECNamedCurves.getByName("secp256k1");
            parsedDomainParams = new ECDomainParameters(
                curveParams.getCurve(),
                curveParams.getG(),
                curveParams.getN()
            );
            parsedPublicKey = parsedDomainParams.getG().multiply(parsedPrivateKey).normalize();
        } catch (NumberFormatException ignored) {
            // Preserve legacy behavior: invalid private keys fail when used, not during construction.
        }
        this.privateKey = parsedPrivateKey;
        this.domainParams = parsedDomainParams;
        this.publicKey = parsedPublicKey;
        this.nonce = nonce;
        this.isMainnet = isMainnet;

        // Validate vault address format if provided
        if (vaultAddress != null && !vaultAddress.isEmpty()) {
            if (!vaultAddress.matches("^0x[a-fA-F0-9]{40}$")) {
                throw new IllegalArgumentException(
                    "Invalid vault address format: " + vaultAddress +
                    ". Must be 0x followed by 40 hexadecimal characters (e.g., 0x0f0d9105b88e938df7816b23e7ad4d1660568a0e)"
                );
            }
        }

        this.vaultAddress = vaultAddress;
        this.walletAddress=walletAddress;
        this.msgPackMapper = new ObjectMapper(new MessagePackFactory());
    }

    /**
     * Factory method to create a HyperliquidAuth instance.
     *
     * <h3>Vault Trading Configuration</h3>
     * <p>To enable vault trading, provide the vault address parameter:</p>
     * <pre>
     * HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
     *     privateKey,
     *     nonceFactory,
     *     true,  // mainnet
     *     "0x0f0d9105b88e938df7816b23e7ad4d1660568a0e",  // vault address
     *     "0x..."  // wallet address
     * );
     * </pre>
     *
     * <p>For personal account trading, pass null for vaultAddress:</p>
     * <pre>
     * HyperliquidAuth auth = HyperliquidAuth.createHyperliquidAuth(
     *     privateKey,
     *     nonceFactory,
     *     true,  // mainnet
     *     null,  // no vault
     *     "0x..."  // wallet address
     * );
     * </pre>
     *
     * @param privateKeyHex Private key in hex format (with or without 0x prefix)
     * @param nonce Nonce factory for generating unique nonces
     * @param isMainnet True for mainnet, false for testnet
     * @param vaultAddress Optional vault address for trading on behalf of a vault (null for personal account)
     * @param walletAddress Optional wallet address (null to auto-derive from private key)
     * @return HyperliquidAuth instance, or null if privateKeyHex is null
     */
    public static HyperliquidAuth createHyperliquidAuth(String privateKeyHex,
                                                       SynchronizedValueFactory<Long> nonce,
                                                       boolean isMainnet,
                                                       String vaultAddress,String walletAddress) {
        return privateKeyHex == null ? null : 
            new HyperliquidAuth(privateKeyHex, nonce, isMainnet, vaultAddress,walletAddress);
    }

    @Override
    public String digestParams(RestInvocation restInvocation) {
        try {
            // Extract action from request body
            Map<String, Object> action = extractActionFromRequest(restInvocation);

            // Generate nonce and expiration
            long nonceValue = this.nonce.createValue();
            Long expiresAfter = null; // Can be set if needed

            // Create action hash
            byte[] actionHash = createActionHash(action, vaultAddress, nonceValue, expiresAfter);

            // Construct phantom agent
            Map<String, Object> phantomAgent = constructPhantomAgent(actionHash, isMainnet);

            // Create EIP-712 signature
            String signature = signEIP712(phantomAgent);

            return signature;

        } catch (Exception e) {
            throw new ExchangeException("Could not sign Hyperliquid request", e);
        }
    }

    /**
     * Sign an action for Hyperliquid L1
     * Public method to generate signature for inclusion in request body
     * Matches Python SDK's sign_l1_action function
     *
     * @param action The action to sign (e.g., order, cancel, etc.)
     * @param nonce Timestamp in milliseconds
     * @param expiresAfter Optional expiration timestamp
     * @return EIP-712 signature as Map with r, s, v fields (matching Python SDK)
     */
    /**
     * Sign an action for Hyperliquid L1
     * Matches Python SDK's sign_l1_action function exactly:
     *   hash = action_hash(action, active_pool, nonce, expires_after)
     *   phantom_agent = construct_phantom_agent(hash, is_mainnet)
     *   data = l1_payload(phantom_agent)
     *   return sign_inner(wallet, data)
     */
    public Map<String, Object> createSignedActionRequest(
            Map<String, Object> action, Long expiresAfter) {
        long nonceValue = nonce.createValue();
        Map<String, Object> signature = signL1Action(action, nonceValue, expiresAfter);
        return HyperliquidAdapters.createSignedRequestBody(
                action, nonceValue, signature, vaultAddress, expiresAfter);
    }

    public Map<String, Object> signL1Action(Map<String, Object> action, long nonce, Long expiresAfter) {
        try {
            // Step 1: action_hash(action, active_pool, nonce, expires_after)
            byte[] hash = createActionHash(action, vaultAddress, nonce, expiresAfter);

            // Step 2: construct_phantom_agent(hash, is_mainnet)
            Map<String, Object> phantomAgent = constructPhantomAgent(hash, isMainnet);

            // Step 3: l1_payload(phantom_agent)
            Map<String, Object> data = l1Payload(phantomAgent);

            // Step 4: sign_inner(wallet, data)
            return signInner(data);

        } catch (Exception e) {
            throw new ExchangeException("Could not sign Hyperliquid action", e);
        }
    }

    /**
     * Create l1_payload structure
     * Matches Python SDK's l1_payload() function
     */
    private Map<String, Object> l1Payload(Map<String, Object> phantomAgent) {
        Map<String, Object> payload = new LinkedHashMap<>();

        // Domain
        Map<String, Object> domain = new LinkedHashMap<>();
        domain.put("chainId", 1337);
        domain.put("name", "Exchange");
        domain.put("verifyingContract", "0x0000000000000000000000000000000000000000");
        domain.put("version", "1");
        payload.put("domain", domain);

        // Types
        Map<String, Object> types = new LinkedHashMap<>();
        List<Map<String, String>> agents = new ArrayList<>();
        Map<String, String> source = new LinkedHashMap<>();
        Map<String, String> connection = new LinkedHashMap<>();

        source.put("name", "source");
        source.put("type", "string");
        agents.add(source);
        connection.put("name", "connectionId");
        connection.put("type", "bytes32");
        agents.add(connection);


        types.put("Agent", agents);
        List<Map<String, String>> domains = new ArrayList<>();

        Map<String, String> name = new LinkedHashMap<>();
        Map<String, String> version = new LinkedHashMap<>();
        Map<String, String> chainId = new LinkedHashMap<>();
        Map<String, String> verifyingContract = new LinkedHashMap<>();


        name.put("name", "name");
        name.put("type", "string");
        domains.add(name);


        version.put("name", "version");
        version.put("type", "string");
        domains.add(version);
        chainId.put("name", "chainId");
        chainId.put("type", "uint256");
        domains.add(chainId);
        verifyingContract.put("name", "verifyingContract");
        verifyingContract.put("type", "address");
        domains.add(verifyingContract);
        types.put("EIP712Domain", domains);
        domain.put("name", "Exchange");
        domain.put("verifyingContract", "0x0000000000000000000000000000000000000000");
        domain.put("version", "1");


        payload.put("types", types);

        // Primary type
        payload.put("primaryType", "Agent");

        // Message
        payload.put("message", phantomAgent);

        return payload;
    }

    /**
     * Sign the payload (sign_inner equivalent)
     * Matches Python SDK's sign_inner() function
     */
        private Map<String, Object> signInner(Map<String, Object> data) throws Exception {
        // Create EIP-712 structured data hash
        Map<String, Object> phantomAgent = (Map<String, Object>) data.get("message");
        byte[] structHash = createStructHash(phantomAgent);
        byte[] digest = createEIP712Hash(structHash);

        // Sign with ECDSA
        ECDSASignature signature = signECDSA(digest);

        // Return as structured map matching Python SDK format
        return formatSignatureStructured(signature, digest);
    }

    /**
     * Extract action from REST request body
     */
    private Map<String, Object> extractActionFromRequest(RestInvocation restInvocation) 
            throws IOException {
        String requestBody = restInvocation.getRequestBody();
        if (requestBody == null || requestBody.isEmpty()) {
            throw new ExchangeException("Request body is required for Hyperliquid authentication");
        }
        
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> fullRequest = mapper.readValue(requestBody, Map.class);
        
        // Extract the action from the request body
        Object action = fullRequest.get("action");
        if (action instanceof Map) {
            return (Map<String, Object>) action;
        } else {
            // Fallback: if no action wrapper, assume the whole body is the action
            return fullRequest;
        }
    }

    /**
     * Create action hash using MessagePack + Keccak256
     * Equivalent to action_hash() in Python SDK
     */
    private byte[] createActionHash(Map<String, Object> action, String vaultAddress,
                                  long nonce, Long expiresAfter) throws IOException {
        // Serialize action with MessagePack
        byte[] data = msgPackMapper.writeValueAsBytes(action);

        // Append nonce (8 bytes, big endian)
        ByteBuffer buffer = ByteBuffer.allocate(data.length + 9 +
            (vaultAddress != null ? 20 : 0) +
            (expiresAfter != null ? 9 : 0));

        buffer.put(data);
        buffer.putLong(nonce);

        // Handle vault address
        if (vaultAddress == null) {
            buffer.put((byte) 0x00);
        } else {
            buffer.put((byte) 0x01);
            buffer.put(hexToBytes(vaultAddress));
        }

        // Handle expiration
        if (expiresAfter != null) {
            buffer.put((byte) 0x00);
            buffer.putLong(expiresAfter);
        }

        return keccak256(buffer.array());
    }

    /**
     * Construct phantom agent
     * Equivalent to construct_phantom_agent() in Python SDK
     */
    private Map<String, Object> constructPhantomAgent(byte[] hash, boolean isMainnet) {
        Map<String, Object> phantomAgent = new LinkedHashMap<>();
        phantomAgent.put("source", isMainnet ? "a" : "b");
        phantomAgent.put("connectionId", "0x" + Hex.toHexString(hash));
        return phantomAgent;
    }

    /**
     * Sign using EIP-712 standard (returns hex string for legacy use)
     * Equivalent to l1_payload() + sign_inner() in Python SDK
     */
    private String signEIP712(Map<String, Object> phantomAgent) throws Exception {
        // Create EIP-712 structured data hash
        byte[] structHash = createStructHash(phantomAgent);
        byte[] digest = createEIP712Hash(structHash);

        // Sign with ECDSA
        ECDSASignature signature = signECDSA(digest);

        // Return as hex string (r + s + v format)
        return formatSignatureHex(signature, digest);
    }

    /**
     * Sign using EIP-712 standard (returns structured Map matching Python SDK)
     * Equivalent to l1_payload() + sign_inner() in Python SDK
     * Returns Map with {"r": "0x...", "s": "0x...", "v": int}
     */
    private Map<String, Object> signEIP712Structured(Map<String, Object> phantomAgent) throws Exception {
        // Create EIP-712 structured data hash
        byte[] structHash = createStructHash(phantomAgent);
        byte[] digest = createEIP712Hash(structHash);

        // Sign with ECDSA
        ECDSASignature signature = signECDSA(digest);

        // Return as structured map matching Python SDK format
        return formatSignatureStructured(signature, digest);
    }

    /**
     * Create struct hash for Agent type
     */
    private byte[] createStructHash(Map<String, Object> phantomAgent) {
        String source = (String) phantomAgent.get("source");
        String connectionId = (String) phantomAgent.get("connectionId");
        
        // Hash struct: keccak256(abi.encode(AGENT_TYPE_HASH, keccak256(source), connectionId))
        byte[] sourceHash = keccak256(source.getBytes(StandardCharsets.UTF_8));
        byte[] connectionIdBytes = hexToBytes(connectionId);
        
        ByteBuffer buffer = ByteBuffer.allocate(32 + 32 + 32);
        buffer.put(AGENT_TYPE_HASH);
        buffer.put(sourceHash);
        buffer.put(connectionIdBytes);
        
        return keccak256(buffer.array());
    }

    /**
     * Create final EIP-712 hash
     */
    private byte[] createEIP712Hash(byte[] structHash) {
        // "\x19\x01" + DOMAIN_SEPARATOR + structHash
        ByteBuffer buffer = ByteBuffer.allocate(2 + 32 + 32);
        buffer.put((byte) 0x19);
        buffer.put((byte) 0x01);
        buffer.put(DOMAIN_SEPARATOR);
        buffer.put(structHash);
        
        return keccak256(buffer.array());
    }

    /**
     * Sign digest with ECDSA and calculate recovery ID
     * Uses deterministic signing (RFC 6979) to match Python's eth_account library
     * Applies low-s normalization (EIP-2) to match Ethereum standard
     */
    private ECDSASignature signECDSA(byte[] digest) {
        if (privateKey == null || domainParams == null || publicKey == null) {
            throw new ExchangeException("Invalid private key");
        }
        ECPrivateKeyParameters keyParams = new ECPrivateKeyParameters(privateKey, domainParams);

        // Use deterministic ECDSA (RFC 6979) to match Python's eth_account
        org.bouncycastle.crypto.signers.HMacDSAKCalculator kCalculator =
            new org.bouncycastle.crypto.signers.HMacDSAKCalculator(new org.bouncycastle.crypto.digests.SHA256Digest());
        org.bouncycastle.crypto.signers.ECDSASigner signer =
            new org.bouncycastle.crypto.signers.ECDSASigner(kCalculator);
        signer.init(true, keyParams);

        BigInteger[] signature = signer.generateSignature(digest);
        BigInteger r = signature[0];
        BigInteger s = signature[1];

        // Calculate recovery ID before normalization
        int recoveryId = calculateRecoveryId(r, s, digest, domainParams);

        // Apply low-s normalization (EIP-2) to match Ethereum standard
        // If s > n/2, use n - s instead (prevents signature malleability)
        BigInteger n = domainParams.getN();
        BigInteger halfN = n.shiftRight(1);
        if (s.compareTo(halfN) > 0) {
            s = n.subtract(s);
            // When we flip s, we need to flip the recovery ID as well
            // Convert from Ethereum format (27/28) to raw (0/1), flip, then back
            int rawRecovery = recoveryId - 27;
            rawRecovery = rawRecovery ^ 1;
            recoveryId = rawRecovery + 27;
        }

        return new ECDSASignature(r, s, recoveryId);
    }

    /**
     * Format signature as hex string (legacy format)
     */
    private String formatSignatureHex(ECDSASignature signature, byte[] digest) {
        // Format as: r (32 bytes) + s (32 bytes) + v (1 byte)
        String r = String.format("%064x", signature.r);
        String s = String.format("%064x", signature.s);
        String v = String.format("%02x", signature.v);

        return "0x" + r + s + v;
    }

    /**
     * Format signature as structured Map matching Python SDK
     * Returns {"r": "0x...", "s": "0x...", "v": int}
     * Uses minimal hex representation (no zero-padding) to match Python's to_hex()
     */
    private Map<String, Object> formatSignatureStructured(ECDSASignature signature, byte[] digest) {
        Map<String, Object> result = new LinkedHashMap<>();
        // Use minimal hex representation (no padding), matching Python's to_hex()
        result.put("r", "0x" + signature.r.toString(16));
        result.put("s", "0x" + signature.s.toString(16));
        result.put("v", signature.v);
        return result;
    }

    /**
     * Calculate recovery ID for signature (matching Ethereum's v parameter)
     * Implements ECDSA public key recovery to determine correct v value
     */
    private int calculateRecoveryId(BigInteger r, BigInteger s, byte[] digest, ECDomainParameters domainParams) {
        // Convert digest to BigInteger
        BigInteger e = new BigInteger(1, digest);
        BigInteger n = domainParams.getN();

        // Try both recovery IDs (0 and 1) and see which one recovers to the correct public key
        for (int recovery = 0; recovery < 2; recovery++) {
            try {
                ECPoint recovered = recoverPublicKey(e, r, s, recovery, domainParams);
                if (recovered != null) {
                    // Normalize both points for comparison
                    recovered = recovered.normalize();
                    // Compare coordinates
                    if (publicKey.getXCoord().toBigInteger().equals(recovered.getXCoord().toBigInteger()) &&
                        publicKey.getYCoord().toBigInteger().equals(recovered.getYCoord().toBigInteger())) {
                        return recovery + 27; // Ethereum uses 27/28 instead of 0/1
                    }
                }
            } catch (Exception e1) {
                // Continue to next recovery ID
            }
        }

        // Fallback to 27 (should not reach here with correct implementation)
        return 27;
    }

    /**
     * Recover public key from ECDSA signature
     * Implements SEC1 4.1.6 public key recovery
     */
    private ECPoint recoverPublicKey(BigInteger e, BigInteger r, BigInteger s, int recovery, ECDomainParameters domainParams) {
        BigInteger n = domainParams.getN();
        BigInteger i = BigInteger.valueOf(recovery / 2);
        BigInteger x = r.add(i.multiply(n));

        // Check if x is valid
        org.bouncycastle.math.ec.ECCurve curve = domainParams.getCurve();
        BigInteger p = curve.getField().getCharacteristic();
        if (x.compareTo(p) >= 0) {
            return null;
        }

        // Decompress point - get R from x coordinate
        // y^2 = x^3 + ax + b (for secp256k1: y^2 = x^3 + 7)
        ECPoint R = decompressPoint(x, (recovery & 1) == 1, curve);
        if (R == null) {
            return null;
        }

        // Check nR = point at infinity
        if (!R.multiply(n).isInfinity()) {
            return null;
        }

        // Compute Q = r^-1 (sR - eG)
        BigInteger eInv = BigInteger.ZERO.subtract(e).mod(n);
        BigInteger rInv = r.modInverse(n);
        BigInteger srInv = rInv.multiply(s).mod(n);
        BigInteger eInvrInv = rInv.multiply(eInv).mod(n);

        ECPoint q = domainParams.getG().multiply(eInvrInv).add(R.multiply(srInv));

        return q;
    }

    /**
     * Decompress an EC point from x coordinate
     */
    private ECPoint decompressPoint(BigInteger x, boolean yBit, org.bouncycastle.math.ec.ECCurve curve) {
        // For secp256k1: y^2 = x^3 + 7
        BigInteger p = curve.getField().getCharacteristic();
        BigInteger a = curve.getA().toBigInteger();
        BigInteger b = curve.getB().toBigInteger();

        // Calculate y^2 = x^3 + ax + b
        BigInteger ySquared = x.modPow(BigInteger.valueOf(3), p)
                               .add(a.multiply(x))
                               .add(b)
                               .mod(p);

        // Calculate y = sqrt(y^2) mod p
        // For p ≡ 3 (mod 4), we can use y = (y^2)^((p+1)/4) mod p
        BigInteger y = ySquared.modPow(p.add(BigInteger.ONE).divide(BigInteger.valueOf(4)), p);

        // Verify y^2 = ySquared
        if (!y.modPow(BigInteger.valueOf(2), p).equals(ySquared)) {
            return null;
        }

        // Choose correct y based on yBit (even/odd)
        if (y.testBit(0) != yBit) {
            y = p.subtract(y);
        }

        return curve.createPoint(x, y);
    }

    /**
     * Compute Keccak-256 hash
     */
    private static byte[] keccak256(byte[] input) {
        KeccakDigest digest = new KeccakDigest(256);
        digest.update(input, 0, input.length);
        byte[] result = new byte[digest.getDigestSize()];
        digest.doFinal(result, 0);
        return result;
    }

    /**
     * Compute Keccak-256 hash of string
     */
    private static String keccak256(String input) {
        return Hex.toHexString(keccak256(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Convert hex string to bytes
     */
    private static byte[] hexToBytes(String hex) {
        if (hex.startsWith("0x")) {
            hex = hex.substring(2);
        }
        return Hex.decode(hex);
    }

    /**
     * Get vault address if configured for vault trading.
     *
     * <p>Returns the vault address that was configured when creating this HyperliquidAuth instance.
     * When a vault address is present, all signed operations will execute on behalf of the vault
     * instead of the personal account.</p>
     *
     * @return Vault address (0x + 40 hex characters), or null if trading on personal account
     */
    public String getVaultAddress() {
        return vaultAddress;
    }

    public String getWalletAddress() {
        return walletAddress;
    }

    /**
     * Get Ethereum address from private key
     * @return Ethereum address as hex string with 0x prefix
     */
    public String getEthereumAddress() {
        try {
            BigInteger privateKey = new BigInteger(privateKeyHex, 16);
            
            // Get secp256k1 curve parameters
            X9ECParameters curveParams = SECNamedCurves.getByName("secp256k1");
            ECDomainParameters domainParams = new ECDomainParameters(
                curveParams.getCurve(), 
                curveParams.getG(), 
                curveParams.getN()
            );
            
            // Get the public key point from private key
            ECPoint publicKeyPoint = domainParams.getG().multiply(privateKey).normalize();
            
            // Extract x and y coordinates (32 bytes each, big endian)
            byte[] xBytes = publicKeyPoint.getXCoord().toBigInteger().toByteArray();
            byte[] yBytes = publicKeyPoint.getYCoord().toBigInteger().toByteArray();
            
            // Ensure exactly 32 bytes each (remove leading zeros or pad if necessary)
            byte[] xBytes32 = new byte[32];
            byte[] yBytes32 = new byte[32];
            
            System.arraycopy(xBytes, Math.max(0, xBytes.length - 32), 
                           xBytes32, Math.max(0, 32 - xBytes.length), 
                           Math.min(32, xBytes.length));
            System.arraycopy(yBytes, Math.max(0, yBytes.length - 32), 
                           yBytes32, Math.max(0, 32 - yBytes.length), 
                           Math.min(32, yBytes.length));
            
            // Concatenate x and y coordinates (64 bytes total)
            byte[] publicKeyBytes = new byte[64];
            System.arraycopy(xBytes32, 0, publicKeyBytes, 0, 32);
            System.arraycopy(yBytes32, 0, publicKeyBytes, 32, 32);
            
            // Hash the public key with Keccak256
            byte[] hash = keccak256(publicKeyBytes);
            
            // Take last 20 bytes as Ethereum address
            byte[] addressBytes = new byte[20];
            System.arraycopy(hash, hash.length - 20, addressBytes, 0, 20);
            
            return "0x" + Hex.toHexString(addressBytes);
            
        } catch (Exception e) {
            throw new ExchangeException("Failed to derive Ethereum address from private key", e);
        }
    }

    /**
     * ECDSA signature container with recovery ID
     */
    private static class ECDSASignature {
        final BigInteger r;
        final BigInteger s;
        final int v;
        
        ECDSASignature(BigInteger r, BigInteger s, int v) {
            this.r = r;
            this.s = s;
            this.v = v;
        }
    }
}