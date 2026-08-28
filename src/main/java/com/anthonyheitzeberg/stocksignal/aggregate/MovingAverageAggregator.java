package com.anthonyheitzeberg.stocksignal.aggregate;

import com.anthonyheitzeberg.stocksignal.model.PriceSignal;
import com.anthonyheitzeberg.stocksignal.model.StockTickEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class MovingAverageAggregator implements AggregateFunction<StockTickEvent, MovingAverageAggregator.Accumulator, PriceSignal> {

    public static  class Accumulator {
        String symbol;
        double sum;
        long count;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(StockTickEvent value, Accumulator accumulator) {
        // carry the symbol through so getResult(_ has it later
        // getResult only sees the accumulator, never the raw events
        accumulator.symbol = value.getSymbol();
        accumulator.sum += value.getPrice();
        accumulator.count += 1;
        return accumulator;
    }

    @Override
    public PriceSignal getResult(Accumulator accumulator) {
        double avg = accumulator.count == 0 ? 0.0 : accumulator.sum / accumulator.count;
        // windowEnd gets set properly once we wire this into a ProcessWindowFunction later
        // for now we timestamp it at aggregation time
        return new PriceSignal(accumulator.symbol, avg, System.currentTimeMillis());
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        Accumulator merged = new Accumulator();
        merged.symbol = a.symbol; // same key, so a.symbol == b.symbol
        merged.sum = a.sum + b.sum;
        merged.count = a.count + b.count;
        return merged;
    }
}
