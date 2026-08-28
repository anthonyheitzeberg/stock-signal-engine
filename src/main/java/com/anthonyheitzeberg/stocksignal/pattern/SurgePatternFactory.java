package com.anthonyheitzeberg.stocksignal.pattern;

import com.anthonyheitzeberg.stocksignal.model.StockTickEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SurgePatternFactory {

    private static final double PRICE_INCREASE_THRESHOLD = 1.001; // 0.1% up
    private static final double VOLUME_SPIKE_MULTIPLIER = 1.5; // 50% more volume

    public static Pattern<StockTickEvent, ?> build() {
        return Pattern.<StockTickEvent>begin("start")
                .followedBy("spike")
                .where(new IterativeCondition<StockTickEvent>() {
                    @Override
                    public boolean filter(StockTickEvent event, Context<StockTickEvent> ctx) throws Exception {
                        Iterable<StockTickEvent> startEvents = ctx.getEventsForPattern("start");
                        StockTickEvent start = startEvents.iterator().next();

                        boolean priceUp = event.getPrice() > start.getPrice() * PRICE_INCREASE_THRESHOLD;
                        boolean volumeSpiked = event.getVolume() > start.getVolume() * VOLUME_SPIKE_MULTIPLIER;

                        return priceUp && volumeSpiked;
                    }
                }).within(Time.seconds(15));
    }
}
