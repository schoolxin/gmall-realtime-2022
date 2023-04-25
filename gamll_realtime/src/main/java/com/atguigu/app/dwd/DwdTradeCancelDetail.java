package com.atguigu.app.dwd;

import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCancelDetail {
    public static void main(String[] args) {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);


        //TODO 2 从kafka中读取订单预处理表
        tblEnv.executeSql("" +
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
                "    `old` map<string,string> " +
                ")" + MyKafkaUtil.getKafkaDDL("dwd_trade_order_pre_process", "cancel_detail"));

        //TODO 3 从订单预处理表中过滤出取消的订单数据
        Table cancelTbl = tblEnv.sqlQuery("" +
                "select " +
                "id, " +
                "order_id, " +
                "user_id, " +
                "sku_id, " +
                "sku_name, " +
                "province_id, " +
                "activity_id, " +
                "activity_rule_id, " +
                "coupon_id, " +
                //"operate_date_id date_id, " +
                "operate_time cancel_time, " +
                "source_id, " +
                "source_type_id, " +  //"source_type source_type_code, " +
                "source_type_name, " +
                "sku_num, " +
                "order_price, " +        //+++
                //"split_original_amount, " +
                "split_activity_amount, " +
                "split_coupon_amount, " +
                "split_total_amount " +   //删除","
                //"oi_ts ts, " +
                //"row_op_ts " +
                "from dwd_order_pre " +
                "where `type` = 'update' " +
                "and `old`['order_status'] is not null " +
                "and order_status = '1003'");

        tblEnv.createTemporaryView("cancelTbl",cancelTbl);


        // TODO 4 创建订单取消明细数据 的输出表
        tblEnv.executeSql("create table dwd_trade_cancel_detail( " +
                "id string, " +
                "order_id string, " +
                "user_id string, " +
                "sku_id string, " +
                "sku_name string, " +
                "province_id string, " +
                "activity_id string, " +
                "activity_rule_id string, " +
                "coupon_id string, " +
                //"date_id string, " +
                "cancel_time string, " +
                "source_id string, " +
                "source_type_id string, " +  // "source_type_code string, " +
                "source_type_name string, " +
                "sku_num string, " +
                "order_price string, " +
                //"split_original_amount string, " +
                "split_activity_amount string, " +
                "split_coupon_amount string, " +
                "split_total_amount string " +  //删除","
                //"ts string, " +
                //"row_op_ts timestamp_ltz(3) " +
                ")" + MyKafkaUtil.getKafkaSinkDDL("dwd_trade_cancel_detail"));

        // TODO 5 将数据插入到sink表中
        tblEnv.executeSql("insert into dwd_trade_cancel_detail select * from cancelTbl");

    }
}
