package com.atguigu.dws;

import com.atguigu.app.func.SplitFunction;
import com.atguigu.domain.KeywordBean;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DwsTrafficSourceKeywordPageViewWindow {

    public static void main(String[] args) throws Exception {
        //TODO 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //表执行环境
        StreamTableEnvironment tblEnv = StreamTableEnvironment.create(env);
        //TODO 通过DDL 语句 读取页面数据
        String topic = "dwd_traffic_page_log";
        String groupID = "dws_traffic_source_keyword_page_view_window";
        tblEnv.executeSql("create table page_log (" +
                " page map<String,String>, " +
                " ts bigint, " +
                " rt as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                " WATERMARK FOR rt AS rt - INTERVAL '2' SECOND" +
                " ) " + MyKafkaUtil.getKafkaDDL(topic,groupID));
        //TODO 从page_log中过滤出搜索数据
        Table filterTable = tblEnv.sqlQuery("select " +
                " page['item'] as item, " +
                " rt " +
                " from page_log " +
                " where page['last_page_id'] = 'search' " +
                " and page['item_type'] = 'keyword' " +
                " and page['item'] is not null ");

        tblEnv.createTemporaryView("filter_table", filterTable);

        //TODO 注册UDTF 切词
        tblEnv.createTemporaryFunction("SplitFunction",SplitFunction.class);

        //TODO 使用LATERAL
        Table splitTable = tblEnv.sqlQuery("" +
                "SELECT " +
                "    word, " +
                "    rt " +
                "FROM filter_table,  " +
                "LATERAL TABLE(SplitFunction(item))");
        tblEnv.createTemporaryView("split_table", splitTable);
        tblEnv.toAppendStream(splitTable, Row.class).print("Split>>>>>>");

        //TODO 开窗分组

        Table resultTable = tblEnv.sqlQuery("SELECT " +
                " DATE_FORMAT(window_start,'yyyy-MM-dd HH:mm:ss') as stt, " +
                " DATE_FORMAT(window_end,'yyyy-MM-dd HH:mm:ss') as edt, " +
                " word as  keyword," +
                " 'search' source," +
                " count(*) keyword_count," +
                " UNIX_TIMESTAMP()*1000 ts " +
                " FROM TABLE (" +
                " TUMBLE ( TABLE split_table, DESCRIPTOR(rt), INTERVAL '10' SECOND)" +
                " )" +
                "GROUP BY window_start, window_end,word");
        //TODO 6.将动态表转换为流
        DataStream<KeywordBean> keywordBeanDataStream = tblEnv.toAppendStream(resultTable, KeywordBean.class);
        keywordBeanDataStream.print(">>>>>>>>>>>>");

        //TODO 7.将数据写出到ClickHouse
        keywordBeanDataStream.addSink(
                MyClickHouseUtil.getSinkFunction("insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)")
        );
//        tblEnv.createTemporaryView("resultTable",resultTable);
        // 创建输出到clickhouse的表  目前flink 官方不支持clickhouse的jdbc链接器 所以目前只能按流的方式写出
//        tblEnv.executeSql("create table dws_traffic_source_keyword_page_view_window() with ('connector' = 'jdbc', )");

//        tblEnv.executeSql("insert into select * from resultTable");
        //TODO 8.启动任务
        env.execute("DwsTrafficSourceKeywordPageViewWindow");



    }
}
