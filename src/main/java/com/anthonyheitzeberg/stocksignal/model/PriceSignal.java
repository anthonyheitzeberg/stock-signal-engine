package com.anthonyheitzeberg.stocksignal.model;

public class PriceSignal {

    private String symbol;
    private double avgPrice;
    private long windowEnd;

    public PriceSignal() {}

    public PriceSignal(String symbol, double avgPrice, long windowEnd) {
        this.symbol = symbol;
        this.avgPrice = avgPrice;
        this.windowEnd = windowEnd;
    }

    public String getSymbol() { return symbol; }
    public void setSymbol(String symbol) { this.symbol = symbol; }

    public double getAvgPrice() { return avgPrice; }
    public void setAvgPrice(double avgPrice) { this.avgPrice = avgPrice; }

    public long getWindowEnd() { return windowEnd; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd;}

    @Override
    public String toString() {
        return String.format("PriceSignal{symbol='%s', avgPrice=%.2f, windowEnd=%d}",
                symbol, avgPrice, windowEnd);
    }
}
