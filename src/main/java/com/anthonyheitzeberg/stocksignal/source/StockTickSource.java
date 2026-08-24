package com.anthonyheitzeberg.stocksignal.source;

import com.anthonyheitzeberg.stocksignal.model.StockTickEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class StockTickSource implements SourceFunction<StockTickEvent> {

    private static final String[] SYMBOLS = {"AAPL", "GOOG", "TSLA", "AMZN"};

    private volatile  boolean isRunning = true;

    @Override
    public void run(SourceContext<StockTickEvent> ctx) throws Exception {
        // TODO: Implement this function to simulate transmitting event
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
