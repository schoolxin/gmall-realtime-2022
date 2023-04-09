package com.atguigu.app.dwd;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //生产环境中设置为Kafka主题的分区数
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用DDL方式读取 topic_db 主题的数据创建表
        tableEnv.executeSql(MyKafkaUtil.getTopicDb("cart_add"));
        //TODO 3.过滤出加购数据
        Table cartAddTable = tableEnv.sqlQuery("" +
                "select " +
                "    `data`['id'] id, " +
                "    `data`['user_id'] user_id, " +
                "    `data`['sku_id'] sku_id, " +
                "    `data`['cart_price'] cart_price, " +
                "    if(`type`='insert',`data`['sku_num'],cast(cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int) as string)) sku_num, " +
                "    `data`['sku_name'] sku_name, " +
                "    `data`['is_checked'] is_checked, " +
                "    `data`['create_time'] create_time, " +
                "    `data`['operate_time'] operate_time, " +
                "    `data`['is_ordered'] is_ordered, " +
                "    `data`['order_time'] order_time, " +
                "    `data`['source_type'] source_type, " +
                "    `data`['source_id'] source_id, " +
                "    pt " +
                "from topic_db " +
                "where `database` = 'gmall' " +
                "and `table` = 'cart_info' " +
                "and (`type` = 'insert' " +
                "or (`type` = 'update'  " +
                "    and  " +
                "    `old`['sku_num'] is not null  " +
                "    and  " +
                "    cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))");
        // 将表转化为流打印
        tableEnv.toDataStream(cartAddTable).print(">>>>>");

        //将加购事实表注册为表
        tableEnv.createTemporaryView("cart_info_table",cartAddTable);
        //TODO 4  读取MySQL的 base_dic 表作为LookUp表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());



        //TODO 5 关联两张表 取出相关维度
        Table cartAddWithDicTable = tableEnv.sqlQuery("" +
                "select " +
                "    ci.id, " +
                "    ci.user_id, " +
                "    ci.sku_id, " +
                "    ci.cart_price, " +
                "    ci.sku_num, " +
                "    ci.sku_name, " +
                "    ci.is_checked, " +
                "    ci.create_time, " +
                "    ci.operate_time, " +
                "    ci.is_ordered, " +
                "    ci.order_time, " +
                "    ci.source_type source_type_id, " +
                "    dic.dic_name source_type_name, " +
                "    ci.source_id " +
                "from cart_info_table ci " +
                "join base_dic FOR SYSTEM_TIME AS OF ci.pt as dic " +
                "on ci.source_type = dic.dic_code");


        //TODO 6  使用DDL方式创建加购事实表 进行输出的表
        tableEnv.executeSql("" +
                "create table dwd_cart_add( " +
                "    `id` STRING, " +
                "    `user_id` STRING, " +
                "    `sku_id` STRING, " +
                "    `cart_price` STRING, " +
                "    `sku_num` STRING, " +
                "    `sku_name` STRING, " +
                "    `is_checked` STRING, " +
                "    `create_time` STRING, " +
                "    `operate_time` STRING, " +
                "    `is_ordered` STRING, " +
                "    `order_time` STRING, " +
                "    `source_type_id` STRING, " +
                "    `source_type_name` STRING, " +
                "    `source_id` STRING " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));


        //TODO 7 将加购事实表 进行输出  可以将cartAddWithDicTable注册为表 然后使用insert into xx select xx 的语法
        tableEnv.createTemporaryView("cart_add_dic_table",cartAddWithDicTable);
        /* tableEnv.executeSql("insert into xx select * from xxxx"); */
        cartAddWithDicTable.executeInsert("dwd_cart_add");


        env.execute();


    }
}
