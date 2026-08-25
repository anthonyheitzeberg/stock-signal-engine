package com.anthonyheitzeberg.stocksignal.source;

import com.anthonyheitzeberg.stocksignal.model.StockTickEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class StockTickSource implements SourceFunction<StockTickEvent> {

    private static final String[] SYMBOLS = {"AAPL", "GOOG", "TSLA", "AMZN"};

    private volatile  boolean isRunning = true;

    @Override
    public void run(SourceContext<StockTickEvent> ctx) throws Exception {
        Random random = new Random();

        // seed a starting price per symbol
        Map<String, Double> prices = new HashMap<>();
        for (String symbol : SYMBOLS) {
            prices.put(symbol, 100 + random.nextDouble() * 2000); // $100-$300
        }

        while (isRunning) {
            String symbol = SYMBOLS[random.nextInt(SYMBOLS.length)];

            // random walk: nudge price by up to +/-0.5%
            double currentPrice = prices.get(symbol);
            double changePct = (random.nextDouble() - 0.5) * 0.01; // -0.5% to +0.5%
            double newPrice = currentPrice * (1 + changePct);
            prices.put(symbol, newPrice);

            long volume = 100 + random.nextInt(5000);
            long timestamp = System.currentTimeMillis();

            StockTickEvent event = new StockTickEvent(symbol, newPrice, volume, timestamp);

            // synchronize on the checkpoint lock, this is required for correctness
            // whenever a source touches shared/mutable state and Flink checkpointing is enabled
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(event);
            }

            Thread.sleep(200);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
