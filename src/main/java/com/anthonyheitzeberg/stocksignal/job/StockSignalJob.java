package com.anthonyheitzeberg.stocksignal.job;

import com.anthonyheitzeberg.stocksignal.aggregate.MovingAverageAggregator;
import com.anthonyheitzeberg.stocksignal.model.PriceSignal;
import com.anthonyheitzeberg.stocksignal.model.StockTickEvent;
import com.anthonyheitzeberg.stocksignal.source.StockTickSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class StockSignalJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockTickEvent> ticks = env.addSource(new StockTickSource());

        WatermarkStrategy<StockTickEvent> watermarkStrategy = WatermarkStrategy
                .<StockTickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        DataStream<StockTickEvent> ticksWithWatermarks =
                ticks.assignTimestampsAndWatermarks(watermarkStrategy);

        KeyedStream<StockTickEvent, String> keyedTicks =
                ticksWithWatermarks.keyBy(StockTickEvent::getSymbol);

        WindowedStream<StockTickEvent, String, TimeWindow> windowedTicks =
                keyedTicks.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)));

        DataStream<PriceSignal> movingAverages =
                windowedTicks.aggregate(new MovingAverageAggregator());

        movingAverages.print();

        env.execute("Stock Signal Engine");
    }
}