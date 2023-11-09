package com.example.helloworld;

import java.util.stream.StreamSupport;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;

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
				.window(TumblingEventTimeWindows.of(Time.seconds(5)))
				.process(new ProcessWindowFunction<SensorReading, SensorReading, String, TimeWindow>() {
					@Override
					public void process(
							String key,
							Context context,
							Iterable<SensorReading> elements,
							Collector<SensorReading> collector) throws Exception {
						double average = StreamSupport.stream(elements.spliterator(), false)
								.mapToDouble(element -> element.getTemperature())
								.average()
								.orElse(0.0);
						TimeWindow currentWindow = context.window();
						collector.collect(SensorReading.builder()
								.id(key)
								.timestamp((currentWindow.getStart() + currentWindow.getEnd()) / 2)
								.temperature(average)
								.build());
					}
				});
		transformedReadingStream.addSink(SensorSink.builder().build());
		// execute program
		env.execute("transform example of sensor reading");
	}
}
