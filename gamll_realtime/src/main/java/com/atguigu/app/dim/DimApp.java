package com.atguigu.app.dim;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DimApp {

    public static void main(String[] args) {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数

        //添加数据源
        String topic = "topic_db";
        String groupId = "dim_app";
        DataStreamSource<String> kfsDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        
        //对数据进行过滤  过滤掉非json的字符串  过滤掉删除 /maxwell初始化数据中启动和完成的json字符串
        kfsDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {


                try{

                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String operate_type = jsonObject.getString("type");
                    if("insert".equals(operate_type)||"update".equals(operate_type)||"bootstrap-insert".equals(operate_type))
                    {
                        out.collect(jsonObject);
                    }

                }catch (Exception e)
                {
                    System.out.println("发现脏数据：" + value);
                }

            }
        });
    }
}
