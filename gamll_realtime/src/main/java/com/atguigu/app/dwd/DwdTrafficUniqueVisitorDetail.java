package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数


        //TODO 2读取页面主题 创建流
        //添加数据源
        String topic = "dwd_traffic_page_log";
        String groupId = "unique_visitor_detail";
        DataStreamSource<String> kfsDs = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        //TODO 3 过滤掉上一跳不为null的数据 并将每行数据转化为json

        SingleOutputStreamOperator<JSONObject> jsonObjDs = kfsDs.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    //获取上一跳页面id
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null) {
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println(value);
                }

            }
        });

        //TODO 4 按mid 进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDs.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5 使用状态编程 实现Mid的去重

        SingleOutputStreamOperator<JSONObject> UvDs = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> lastVistDate;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                lastVistDate = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastVisite", String.class));
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //获取状态数据  已经当前数据中的时间戳
                String lastDate = lastVistDate.value();
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);
                if (lastDate == null || !lastDate.equals(curDate)) {
                    lastVistDate.update(curDate);
                    return true;
                }
                return false;

            }
        });

        //TODO 6 将数据写到kafka
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        UvDs.print(">>>>>>>>");
        UvDs.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));
        env.execute("DwdTrafficUniqueVisitorDetail");
    }
}
