package com.anthonyheitzeberg.stocksignal.model;

import java.util.Objects;

public class StockTickEvent {

    private String symbol;
    private double price;
    private long volume;
    private long timestamp; // epoch millis, event time

    // Flink POJOs require a no-arg constructor
    public StockTickEvent() {}

    public StockTickEvent(String symbol, double price, long volume, long timestamp) {
        this.symbol = symbol;
        this.price = price;
        this.volume = volume;
        this.timestamp = timestamp;
    }

    public String getSymbol() { return symbol; }
    public double getPrice() { return price; }
    public long getVolume() { return volume; }
    public long getTimestamp() { return timestamp; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StockTickEvent)) return false;
        StockTickEvent that = (StockTickEvent) o;
        return Double.compare(that.price, price) == 0
                && volume == that.volume
                && timestamp == that.timestamp
                && Objects.equals(symbol, that.symbol);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol, price, volume, timestamp);
    }

    @Override
    public String toString() {
        return String.format("StockTickEvent{symbol='%s', price=%.2f, volume=%d, timestamp=%d}",
                symbol, price, volume, timestamp);
    }
}
