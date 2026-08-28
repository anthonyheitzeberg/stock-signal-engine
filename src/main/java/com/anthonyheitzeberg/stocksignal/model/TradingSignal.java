package com.anthonyheitzeberg.stocksignal.model;

public class TradingSignal {
    private String symbol;
    private double startPrice;
    private double spikePrice;
    private long startVolume;
    private long spikeVolume;
    private long timestamp;

    public TradingSignal() {}

    public TradingSignal(String symbol, double startPrice, double spikePrice, long startVolume, long spikeVolume, long timestamp) {
        this.symbol = symbol;
        this.startPrice = startPrice;
        this.spikePrice = spikePrice;
        this.startVolume = startVolume;
        this.spikeVolume = spikeVolume;
        this.timestamp = timestamp;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public double getStartPrice() {
        return startPrice;
    }

    public void setStartPrice(double startPrice) {
        this.startPrice = startPrice;
    }

    public double getSpikePrice() {
        return spikePrice;
    }

    public void setSpikePrice(double spikePrice) {
        this.spikePrice = spikePrice;
    }

    public long getStartVolume() {
        return startVolume;
    }

    public void setStartVolume(long startVolume) {
        this.startVolume = startVolume;
    }

    public long getSpikeVolume() {
        return spikeVolume;
    }

    public void setSpikeVolume(long spikeVolume) {
        this.spikeVolume = spikeVolume;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return String.format(
                "🚨 TradingSignal{%s: price %.2f -> %.2f, volume %d -> %d}",
                symbol, startPrice, spikePrice, startVolume, spikeVolume);
    }
}
