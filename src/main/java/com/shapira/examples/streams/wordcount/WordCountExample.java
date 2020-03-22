package com.shapira.examples.streams.wordcount;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * 高版本kafka streams的写法
 * 赏析Kafka Streams程序的优雅关闭
 * https://blog.csdn.net/lzufeng/article/details/81319365
 *
 *
 * RocksDB保存本地状态
 * @formatter:off
 * "StreamThread-1@1646" prio=5 tid=0xd nid=NA runnable
 *   java.lang.Thread.State: RUNNABLE
 * 	  at org.rocksdb.RocksDB.open(RocksDB.java:231)
 * 	  at org.apache.kafka.streams.state.internals.RocksDBStore.openDB(RocksDBStore.java:179)
 * 	  at org.apache.kafka.streams.state.internals.RocksDBStore.openDB(RocksDBStore.java:156)
 * 	  at org.apache.kafka.streams.state.internals.RocksDBStore.init(RocksDBStore.java:161)
 * 	  at org.apache.kafka.streams.state.internals.ChangeLoggingKeyValueBytesStore.init(ChangeLoggingKeyValueBytesStore.java:40)
 * 	  at org.apache.kafka.streams.state.internals.MeteredKeyValueStore$7.run(MeteredKeyValueStore.java:100)
 * 	  at org.apache.kafka.streams.processor.internals.StreamsMetricsImpl.measureLatencyNs(StreamsMetricsImpl.java:188)
 * 	  at org.apache.kafka.streams.state.internals.MeteredKeyValueStore.init(MeteredKeyValueStore.java:131)
 * 	  at org.apache.kafka.streams.state.internals.CachingKeyValueStore.init(CachingKeyValueStore.java:63)
 * 	  at org.apache.kafka.streams.processor.internals.AbstractTask.initializeStateStores(AbstractTask.java:85)
 * 	  at org.apache.kafka.streams.processor.internals.StreamTask.<init>(StreamTask.java:142)
 * 	  at org.apache.kafka.streams.processor.internals.StreamThread.createStreamTask(StreamThread.java:903)
 * 	  at org.apache.kafka.streams.processor.internals.StreamThread$TaskCreator.createTask(StreamThread.java:1280)
 * 	  at org.apache.kafka.streams.processor.internals.StreamThread$AbstractTaskCreator.retryWithBackoff(StreamThread.java:1252)
 * 	  at org.apache.kafka.streams.processor.internals.StreamThread.addStreamTasks(StreamThread.java:1008)
 * 	  at org.apache.kafka.streams.processor.internals.StreamThread.access$500(StreamThread.java:69)
 * 	  at org.apache.kafka.streams.processor.internals.StreamThread$1.onPartitionsAssigned(StreamThread.java:267)
 * 	  at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.onJoinComplete(ConsumerCoordinator.java:261)
 * 	  at org.apache.kafka.clients.consumer.internals.AbstractCoordinator.joinGroupIfNeeded(AbstractCoordinator.java:355)
 * 	  at org.apache.kafka.clients.consumer.internals.AbstractCoordinator.ensureActiveGroup(AbstractCoordinator.java:306)
 * 	  at org.apache.kafka.clients.consumer.internals.ConsumerCoordinator.poll(ConsumerCoordinator.java:292)
 * 	  at org.apache.kafka.clients.consumer.KafkaConsumer.pollOnce(KafkaConsumer.java:1030)
 * 	  at org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:996)
 * 	  at org.apache.kafka.streams.processor.internals.StreamThread.runLoop(StreamThread.java:629)
 * 	  at org.apache.kafka.streams.processor.internals.StreamThread.run(StreamThread.java:395)
 *
 * @formatter:on
 */

public class WordCountExample {

    public static void main(String[] args) throws Exception{

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // work-around for an issue around timing of creating internal topics
        // Fixed in Kafka 0.10.2.0
        // don't use in large production apps - this increases network load
        // props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);

        KStreamBuilder builder = new KStreamBuilder();

        // input topic
        KStream<String, String> source = builder.stream("wordcount-input");


        final Pattern pattern = Pattern.compile("\\W+");
        KStream counts  = source.flatMapValues(value-> Arrays.asList(pattern.split(value.toLowerCase())))
                .map((key, value) -> new KeyValue<Object, Object>(value, value))
                .filter((key, value) -> (!value.equals("the")))
                .groupByKey()
                .count("CountStore").mapValues(value->Long.toString(value)).toStream();
        // output topic
        counts.to("wordcount-output");

        KafkaStreams streams = new KafkaStreams(builder, props);

        // This is for reset to work. Don't use in production - it causes the app to re-load the state from Kafka on every start
        streams.cleanUp();

        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(1_000L);

        streams.close();

    }
}
