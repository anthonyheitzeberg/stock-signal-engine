package com.anthonyheitzeberg.stocksignal.job;

import com.anthonyheitzeberg.stocksignal.model.StockTickEvent;
import com.anthonyheitzeberg.stocksignal.source.StockTickSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StockSignalJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<StockTickEvent> ticks = env.addSource(new StockTickSource());

        ticks.print();

        env.execute("Stock Signal Engine");
    }
}
