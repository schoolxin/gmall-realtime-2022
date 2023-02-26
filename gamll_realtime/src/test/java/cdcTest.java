import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.domain.TableProcess;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class cdcTest {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数

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
        DataStreamSource<String> mysqlSourceDS = env.fromSource(mysqlSources, WatermarkStrategy.noWatermarks(), "MysqlSource");

        SingleOutputStreamOperator<TableProcess> tableProcess = mysqlSourceDS.flatMap(new FlatMapFunction<String, TableProcess>() {
            @Override
            public void flatMap(String value, Collector<TableProcess> out) throws Exception {

//                String replace = value.replace("sourceTable", "vvvvvvv");
                JSONObject jsonObject = JSON.parseObject(value);


                TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

                out.collect(tableProcess);


            }
        });

        tableProcess.print();


        env.execute();


    }
}
