package com.anthonyheitzeberg.stocksignal.aggregate;

import com.anthonyheitzeberg.stocksignal.model.PriceSignal;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class PriceSignalWindowFunction extends ProcessWindowFunction<Double, PriceSignal, String, TimeWindow> {


    @Override
    public void process(String symbol, Context context, Iterable<Double> averages, Collector<PriceSignal> out) throws Exception {
        // averages contains exactly ONE element here — the AggregateFunction's
        // pre-computed result — because aggregation already happened upstream.
        // ProcessWindowFunction just enriches it with window metadata.
        double avgPrice = averages.iterator().next();
        long windowEnd = context.window().getEnd();

        out.collect(new PriceSignal(symbol, avgPrice, windowEnd));
    }
}
