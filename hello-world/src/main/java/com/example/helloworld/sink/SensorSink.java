package com.example.helloworld.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.helloworld.source.SensorReading;

import lombok.Builder;

@Builder
public class SensorSink extends RichSinkFunction<SensorReading>{
    private static final Logger LOGGER = LoggerFactory.getLogger(SensorReading.class);

    @Override
    public void invoke(SensorReading value, Context context) throws Exception {
        LOGGER.info("sensor reading: {}", value);
    }
}
