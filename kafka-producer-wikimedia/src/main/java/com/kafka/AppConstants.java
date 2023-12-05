package com.kafka.producer;

import org.apache.kafka.common.protocol.types.Field;

public class AppConstants {
    public static String LOCAL_SERVER="localhost:2021";
    public static String REMOTE_SERVER="dear-anchovy-9404-eu1-kafka.upstash.io:9092";
    public static String PRODUCER_URL="https://stream.wikimedia.org/v2/stream/recentchange";
    public static String TOPIC = "wikimedia";
    public static String TOPIC_ONLINE = "remoteWikimedia-1";
}
