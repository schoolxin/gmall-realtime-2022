package com.atguigu.app.dwd;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeOrderDetail {

    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //生产环境中设置为Kafka主题的分区数
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2 创建读取kafka中 订单预处理表主题

        tableEnv.executeSql("" +
                "create table dwd_order_pre( " +
                "    `id` string, " +
                "    `order_id` string, " +
                "    `sku_id` string, " +
                "    `sku_name` string, " +
                "    `order_price` string, " +
                "    `sku_num` string, " +
                "    `create_time` string, " +
                "    `source_type_id` string, " +
                "    `source_type_name` string, " +
                "    `source_id` string, " +
                "    `split_total_amount` string, " +
                "    `split_activity_amount` string, " +
                "    `split_coupon_amount` string, " +
                "    `consignee` string, " +
                "    `consignee_tel` string, " +
                "    `total_amount` string, " +
                "    `order_status` string, " +
                "    `user_id` string, " +
                "    `payment_way` string, " +
                "    `delivery_address` string, " +
                "    `order_comment` string, " +
                "    `out_trade_no` string, " +
                "    `trade_body` string, " +
                "    `operate_time` string, " +
                "    `expire_time` string, " +
                "    `process_status` string, " +
                "    `tracking_no` string, " +
                "    `parent_order_id` string, " +
                "    `province_id` string, " +
                "    `activity_reduce_amount` string, " +
                "    `coupon_reduce_amount` string, " +
                "    `original_total_amount` string, " +
                "    `feight_fee` string, " +
                "    `feight_fee_reduce` string, " +
                "    `refundable_time` string, " +
                "    `order_detail_activity_id` string, " +
                "    `activity_id` string, " +
                "    `activity_rule_id` string, " +
                "    `order_detail_coupon_id` string, " +
                "    `coupon_id` string, " +
                "    `coupon_use_id` string, " +
                "    `type` string, " +
                "    `old` map<string,string>, " +
                "    `row_op_ts` TIMESTAMP_LTZ(3) " +
                ")"
                + MyKafkaUtil.getKafkaDDL("dwd_trade_order_pre_process", "order_detail")
        );

        //TODO 3 从dwd_order_pre 表中过滤出订单明细表
        Table filteredTable = tableEnv.sqlQuery("" +
                "select " +
                "id, " +
                "order_id, " +
                "user_id, " +
                "sku_id, " +
                "sku_name, " +
                "sku_num, " +                 //+++
                "order_price, " +             //+++
                "province_id, " +
                "activity_id, " +
                "activity_rule_id, " +
                "coupon_id, " +
                //"date_id, " +
                "create_time, " +
                "source_id, " +
                "source_type_id, " +           //"source_type source_type_code, " +
                "source_type_name, " +
                //"sku_num, " +
                //"split_original_amount, " +
                "split_activity_amount, " +
                "split_coupon_amount, " +
                "split_total_amount, " +           //删除","
                //"od_ts ts, " +
                "row_op_ts " +
                "from dwd_order_pre " +
                "where `type`='insert'");
        //将过滤出来的订单明细表注册为临时试图
        tableEnv.createTemporaryView("filtered_table", filteredTable);

        // TODO 4 创建输出到kafka的表
        tableEnv.executeSql("" +
                "create table dwd_trade_order_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "sku_num string, " +              //+++
                "order_price string, " +          //+++
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                //"date_id string, " +
                "create_time string, " +
                "source_id string, " +
                "source_type_id string, " +     //"source_type_code string, " +
                "source_type_name string, " +
                //"sku_num string, " +
                //"split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string, " +  //删除","
                //"ts string, " +
                "row_op_ts timestamp_ltz(3) " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_order_detail"));

        //TODO 6 将订单明细表数据进行输出
        tableEnv.executeSql("insert into dwd_trade_order_detail as select * from filtered_table");

        //TODO 7 启动任务
        env.execute();

    }
}
