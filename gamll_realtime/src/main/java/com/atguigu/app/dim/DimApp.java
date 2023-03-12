package com.atguigu.app.dim;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.domain.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class DimApp {

    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数

        //添加数据源
        String topic = "topic_db";
        String groupId = "dim_app";
        DataStreamSource<String> kfsDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));
        
        //对数据进行过滤  过滤掉非json的字符串  过滤掉删除 /maxwell初始化数据中启动和完成的json字符串
        SingleOutputStreamOperator<JSONObject> filterJsonObjDS = kfsDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {


                try {

                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String operate_type = jsonObject.getString("type");
                    if ("insert".equals(operate_type) || "update".equals(operate_type) || "bootstrap-insert".equals(operate_type)) {
                        out.collect(jsonObject);
                    }

                } catch (Exception e) {
                    System.out.println("发现脏数据：" + value);
                }

            }
        });
        //使用FlinkCDC读取MySQL配置信息表创建配置流
        MySqlSource<String> mysqlSources = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())  //读取的binglog需要反序列化器
                .build();
        //添加数据源
        DataStreamSource<String> mysqlSourceDS = env.fromSource(mysqlSources, WatermarkStrategy.noWatermarks(), "MysqlSource");
        //将配置流 处理为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("config-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlSourceDS.broadcast(mapStateDescriptor);
        //主流连接广播流
        BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = filterJsonObjDS.connect(broadcastStream);
        //处理连接流
        SingleOutputStreamOperator<JSONObject> dimDS = broadcastConnectedStream.process(new TableProcessFunction(mapStateDescriptor));
        dimDS.print(">>>>>>");
        //将数据输出到phoenix
        dimDS.addSink(new DimSinkFunction());
        env.execute();
    }
}
