package com.example.helloworld.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SensorSource extends RichParallelSourceFunction<SensorReading> {
    private final int sensorCount = 10;
    private boolean running = true;

    @Override
    public void run(SourceContext<SensorReading> sourceContext) throws Exception {
        Random random = new Random();
        int taskIndex = this.getRuntimeContext().getIndexOfThisSubtask();
        List<String> sensorIdList = IntStream.range(0, sensorCount)
                .mapToObj(index -> "sensor_" + (taskIndex * sensorCount + index))
                .collect(Collectors.toList());
        List<Double> currentTemperatureList = sensorIdList.stream()
                .map(id -> 65 + (random.nextGaussian() * 20))
                .collect(Collectors.toList());
        while (running) {
            IntStream.range(0, sensorCount)
                    .forEach(index -> {
                        String sensorId = sensorIdList.get(index);
                        double newTemperature = currentTemperatureList.get(index) + random.nextGaussian() * 0.5;
                        currentTemperatureList.set(index, newTemperature);
                        sourceContext.collect(SensorReading.builder()
                                .id(sensorId)
                                .timestamp(Calendar.getInstance().getTimeInMillis())
                                .temperature(newTemperature)
                                .build());
                    });
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
