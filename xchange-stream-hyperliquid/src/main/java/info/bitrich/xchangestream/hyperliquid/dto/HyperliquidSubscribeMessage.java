package info.bitrich.xchangestream.hyperliquid.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HyperliquidSubscribeMessage {
    
    @JsonProperty("method")
    private String method;
    
    @JsonProperty("subscription")
    private Subscription subscription;
    
    public HyperliquidSubscribeMessage() {}
    
    public HyperliquidSubscribeMessage(String method, Subscription subscription) {
        this.method = method;
        this.subscription = subscription;
    }
    
    public String getMethod() {
        return method;
    }
    
    public void setMethod(String method) {
        this.method = method;
    }
    
    public Subscription getSubscription() {
        return subscription;
    }
    
    public void setSubscription(Subscription subscription) {
        this.subscription = subscription;
    }
    
    public static class Subscription {
        @JsonProperty("type")
        private String type;

        @JsonProperty("coin")
        private String coin;

        @JsonProperty("user")
        private String user;

        public Subscription() {}

        /**
         * Constructor for market data subscriptions (coin-based)
         */
        public Subscription(String type, String coin) {
            this.type = type;
            this.coin = coin;
        }

        /**
         * Constructor for user-specific subscriptions
         * @param type The subscription type (e.g., "userFills", "userEvents")
         * @param identifier The user address or coin
         * @param isUserSubscription True if this is a user-based subscription (uses "user" field instead of "coin")
         */
        public Subscription(String type, String identifier, boolean isUserSubscription) {
            this.type = type;
            if (isUserSubscription) {
                this.user = identifier;
            } else {
                this.coin = identifier;
            }
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getCoin() {
            return coin;
        }

        public void setCoin(String coin) {
            this.coin = coin;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }
    }
}