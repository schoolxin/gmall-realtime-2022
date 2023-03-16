package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {
    public static void main(String[] args) {
        //TODO 获取执行环境
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数

        //TODO 消费topic_log 主题
        String topic = "topic_log";
        String groupId = "base_log_app";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 过滤掉 非json格式的数据  并将每行数据转化为json对象 要保留脏数据 那么用侧输出流将脏数据进行输出
        OutputTag<String> dirtyTag = new OutputTag<>("dirty");

        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, s);
//                    e.printStackTrace();
                }

            }
        });
        //获取脏数据
        DataStream<String> dirtyDS = jsonObjDs.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty=========>");


        //TODO 按mid分组
        KeyedStream<JSONObject, String> JsonObjKeyedStream = jsonObjDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 使用状态编程做新老访客标记校验
        JsonObjKeyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
                super.open(parameters);
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //获取is_new 标记 和时间戳
                String isNew = value.getJSONObject("common").getString("is_new");
                Long ts = value.getLong("ts");
                //将时间戳转化为YYYYMMDD
                String curDate = DateFormatUtil.toDate(ts);
                //获取状态中的日期
                String lastDate = lastVisitState.value();
                if ("1".equals(isNew)) {
                    if (lastDate == null) {
                        lastVisitState.update(curDate);
                    } else if (!lastDate.equals(curDate)) {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastDate == null) {
                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                }
                return value;
            }
        });

        //TODO 使用侧输出流进行分流处理

        //TODO 提取各个侧输出流数据

        //TODO 将数据打印到对应的主题

        //TODO 启动任务
    }
}
