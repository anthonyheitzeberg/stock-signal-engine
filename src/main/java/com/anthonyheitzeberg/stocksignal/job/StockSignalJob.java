package com.anthonyheitzeberg.stocksignal.job;

import com.anthonyheitzeberg.stocksignal.aggregate.MovingAverageAggregator;
import com.anthonyheitzeberg.stocksignal.aggregate.PriceSignalWindowFunction;
import com.anthonyheitzeberg.stocksignal.model.PriceSignal;
import com.anthonyheitzeberg.stocksignal.model.StockTickEvent;
import com.anthonyheitzeberg.stocksignal.model.TradingSignal;
import com.anthonyheitzeberg.stocksignal.pattern.SurgePatternFactory;
import com.anthonyheitzeberg.stocksignal.source.StockTickSource;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class StockSignalJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // --- Source ---
        DataStream<StockTickEvent> ticks = env.addSource(new StockTickSource());

        // --- Event-time watermarking ---
        WatermarkStrategy<StockTickEvent> watermarkStrategy = WatermarkStrategy
                .<StockTickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        DataStream<StockTickEvent> ticksWithWatermarks =
                ticks.assignTimestampsAndWatermarks(watermarkStrategy);

        // --- Key by symbol (shared by both branches below) ---
        KeyedStream<StockTickEvent, String> keyedTicks =
                ticksWithWatermarks.keyBy(StockTickEvent::getSymbol);

        // --- Branch 1: sliding-window moving average ---
        WindowedStream<StockTickEvent, String, TimeWindow> windowedTicks =
                keyedTicks.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)));

        DataStream<PriceSignal> movingAverages =
                windowedTicks.aggregate(new MovingAverageAggregator(), new PriceSignalWindowFunction());

        movingAverages.print("AVG");

        // --- Branch 2: CEP price-surge + volume-spike pattern ---
        Pattern<StockTickEvent, ?> surgePattern = SurgePatternFactory.build();

        PatternStream<StockTickEvent> patternStream = CEP.pattern(keyedTicks, surgePattern);

        DataStream<TradingSignal> signals = patternStream.process(
                new PatternProcessFunction<StockTickEvent, TradingSignal>() {
                    @Override
                    public void processMatch(Map<String, List<StockTickEvent>> match,
                                             Context ctx, Collector<TradingSignal> out) {
                        StockTickEvent start = match.get("start").get(0);
                        StockTickEvent spike = match.get("spike").get(0);

                        out.collect(new TradingSignal(
                                spike.getSymbol(),
                                start.getPrice(), spike.getPrice(),
                                start.getVolume(), spike.getVolume(),
                                spike.getTimestamp()
                        ));
                    }
                }
        );

        signals.print("SIGNAL");

        env.execute("Stock Signal Engine");
    }
}