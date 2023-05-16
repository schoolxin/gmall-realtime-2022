package com.atguigu.dws;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2 使用DDL方式读取Kafka page_log 主题的数据创建表并且提取时间戳生成Watermark
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_source_keyword_page_view_window";
        tableEnv.executeSql("" +
                        "create table page_log( " +
                        "    `page` map<string,string>, " +
                        "    `ts` bigint, " +
                        "    `rt` as to_timestamp(from_unixtime(ts/1000)), " +
                        "     watermark for rt as rt - interval '2' second " +
                        " ) " + MyKafkaUtil.getKafkaDDL(topic,groupId)
                
                );
        //TODO 3.过滤出搜索数据


    }
}
