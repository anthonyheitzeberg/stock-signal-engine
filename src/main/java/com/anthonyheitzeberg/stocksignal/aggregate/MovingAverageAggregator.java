package com.anthonyheitzeberg.stocksignal.aggregate;

import com.anthonyheitzeberg.stocksignal.model.PriceSignal;
import com.anthonyheitzeberg.stocksignal.model.StockTickEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class MovingAverageAggregator implements AggregateFunction<StockTickEvent, MovingAverageAggregator.Accumulator, Double> {

    public static  class Accumulator {
        double sum;
        long count;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Accumulator add(StockTickEvent value, Accumulator accumulator) {
        accumulator.sum += value.getPrice();
        accumulator.count += 1;
        return accumulator;
    }

    @Override
    public Double getResult(Accumulator accumulator) {
        return accumulator.count == 0 ? 0.0 : accumulator.sum / accumulator.count;
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        Accumulator merged = new Accumulator();
        merged.sum = a.sum + b.sum;
        merged.count = a.count + b.count;
        return merged;
    }
}
