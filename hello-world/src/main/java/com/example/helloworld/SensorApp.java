package com.example.helloworld;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import com.example.helloworld.sink.SensorSink;
import com.example.helloworld.source.SensorReading;
import com.example.helloworld.source.SensorSource;
import com.example.helloworld.source.SensorTimeAssigner;
import com.google.common.base.Preconditions;

public class SensorApp {
	public static void main(String[] args) throws Exception {
		Preconditions.checkArgument(0 == args.length, "this app accepts no arguments");
		// set up the streaming execution environment
		// in Flink 1.12 the default stream time characteristic has been changed to
		// TimeCharacteristic#EventTime
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// configure watermark interval
		env.getConfig().setAutoWatermarkInterval(1000L);
		DataStream<SensorReading> sensorReadingStream = env
				.addSource(new SensorSource())
				.assignTimestampsAndWatermarks(
						new AssignerWithPeriodicWatermarksAdapter.Strategy<>(new SensorTimeAssigner()));
		DataStream<SensorReading> transformedReadingStream = sensorReadingStream
				.filter(reading -> reading.getTemperature() >= 25)
				.keyBy(SensorReading::getId)
				// add window?
				.reduce((r1, r2) -> r1.getTemperature() > r2.getTemperature() ? r1 : r2);
		transformedReadingStream.addSink(SensorSink.builder().build());
		// execute program
		env.execute("transform example of sensor reading");
	}
}
