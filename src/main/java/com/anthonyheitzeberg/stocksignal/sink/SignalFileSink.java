package com.anthonyheitzeberg.stocksignal.sink;

import com.anthonyheitzeberg.stocksignal.model.TradingSignal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;

public class SignalFileSink extends RichSinkFunction<TradingSignal> {

    private String filePath;
    private transient BufferedWriter writer;

    public SignalFileSink(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // append mode (true) so restarts don't wipe prior signals
        writer = new BufferedWriter(new FileWriter(filePath, true));
    }

    @Override
    public void invoke(TradingSignal signal, Context context) throws IOException {
        writer.write(Instant.now() + " | " + signal);
        writer.newLine();
        writer.flush();
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            writer.close();
        }
        super.close();
    }
}
