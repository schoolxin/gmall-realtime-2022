package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DwdTrafficUserJumpDetail {

    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数

        //TODO 读取kafka页面主题信息
        String topic = "dwd_traffic_page_log";
        String groupId = "user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //TODO 将每行数据解析为Json对象
        SingleOutputStreamOperator<JSONObject> JsonObj = kafkaDS.map(line -> JSON.parseObject(line));


        //TODO 提取事件时间按照mid分组

        KeyedStream<JSONObject, String> keyedStream = JsonObj
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                System.out.println(element.getLong("ts"));
                                return element.getLong("ts");
                            }
                        }))
                .keyBy(json -> json.getJSONObject("common").getString("mid"));
        //TODO 定义CEP 模式
        Pattern<JSONObject, JSONObject> jsonObjectPattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));

        //TODO 将模式作用于数据流
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, jsonObjectPattern);

        //TODO 提取事件(匹配上的和超时的)
        OutputTag<String> outTag = new OutputTag<String>("timeOut"){};
        SingleOutputStreamOperator<String> selectDS = patternStream.select(outTag, new PatternTimeoutFunction<JSONObject, String>() {
                    @Override
                    public String timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                },
                new PatternSelectFunction<JSONObject, String>() {
                    @Override
                    public String select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                }
        );

        //TODO 将两个事件进行合并
        DataStream<String> timeOutDS = selectDS.getSideOutput(outTag);
        DataStream<String> unionDS = selectDS.union(timeOutDS);
        //TODO 将数据写到kafka
        selectDS.print("select");
        timeOutDS.print("timeout");
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));

        env.execute();

    }
}
