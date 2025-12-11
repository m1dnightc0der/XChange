package org.knowm.xchange.examples.hyperliquid.marketdata;

import info.bitrich.xchangestream.core.StreamingExchange;
import io.reactivex.rxjava3.disposables.Disposable;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.examples.hyperliquid.HyperliquidDemoUtils;
import org.knowm.xchange.instrument.Instrument;

import java.util.concurrent.TimeUnit;

public class HyperliquidStreamingMarketdataDemo {


    public static void main(String[] args) throws InterruptedException {

        // Create the streaming exchange instance using utility
        StreamingExchange exchange = HyperliquidDemoUtils.createStreamingExchange();

        // Use BTC as the instrument (Hyperliquid uses single coins, not pairs)
        Instrument btcInstrument = new FuturesContract(CurrencyPair.BTC_USD, "PERP");

        System.out.println("Connecting to Hyperliquid WebSocket API...");
        exchange.connect().blockingAwait();
        System.out.println("Connected successfully!");

        // Demonstrate streaming order book data
        System.out.println("Starting BTC order book stream...");
/*        Disposable orderBookSubscription = exchange
                .getStreamingMarketDataService()
                .getOrderBook(btcInstrument)
                .subscribe(
                        orderBook -> {
                            System.out.println("BTC OrderBook - Bids: " + orderBook.getBids().size() + 
                                             ", Asks: " + orderBook.getAsks().size() + 
                                             ", Time: " + orderBook.getTimeStamp());
                        },
                        error -> System.out.println("Error in order book stream: " + error)
                );*/

        // Demonstrate streaming trades data
/*        System.out.println("Starting BTC trades stream...");
        Disposable tradesSubscription = exchange
                .getStreamingMarketDataService()
                .getTrades(btcInstrument)
                .subscribe(
                        trade -> {
                            System.out.println("BTC Trade - " + trade.getType() + 
                                             " " + trade.getOriginalAmount() + 
                                             " @ $" + trade.getPrice() + 
                                             " at " + trade.getTimestamp() + 
                                             ", ID: " + trade.getId());
                        },
                        error -> System.out.println("Error in trades stream: " + error)
                );*/

        System.out.println("Starting BTC ticker stream...");
        Disposable tickerSubscription = exchange
                .getStreamingMarketDataService()
                .getTicker(btcInstrument)
                .subscribe(
                        ticker -> {
                            System.out.println("BTC Ticker - " + ticker.getBid() +
                                    " " + ticker.getAsk() +

                                    " at " + ticker.getTimestamp());
                        },
                        error -> System.out.println("Error in ticker stream: " + error)
                );

        // Let the streams run for 60 seconds
        TimeUnit.SECONDS.sleep(60);

        // Clean up subscriptions
        System.out.println("Disposing subscriptions...");
        // orderBookSubscription.dispose();
        // tradesSubscription.dispose();
        tickerSubscription.dispose();
        // Disconnect from the exchange
        System.out.println("Disconnecting from exchange...");
        exchange.disconnect().blockingAwait();
        System.out.println("Disconnected successfully!");
    }
}