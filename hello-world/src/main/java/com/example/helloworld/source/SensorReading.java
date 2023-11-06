package com.example.helloworld.source;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Builder
@Getter
@Setter
@ToString
public class SensorReading {
    private final String id;
    private final long timestamp;
    private final double temperature;
}
