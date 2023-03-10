package com.atguigu.utils;

/*
kafka flink 消费者和生产者工具类
 */
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class MyKafkaUtil {

    private static final String KAFKA_SERVER = "hadoop102:9092";
    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topics,String group_id){
        Properties pros = new Properties();
        pros.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        pros.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
//        pros.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,)
        return new FlinkKafkaConsumer<String>(topics,
//                new SimpleStringSchema(), // 该反序列化方法 中的String 入参必须非null 由于过来的业务数据 不能保证是否为null 所以不能直接使用该SimpleStringSchema
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        if (consumerRecord == null || consumerRecord.value() == null) {
                            return "";
                        } else {
                            return new String(consumerRecord.value(),StandardCharsets.UTF_8);
                        }
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                },
                pros);

    }
}
