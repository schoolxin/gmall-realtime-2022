import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Flink03_WaterMarkTest2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //1001,23.6,1324
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(line -> {
            String[] split = line.split(",");
            return new WaterSensor(split[0],
                    Double.parseDouble(split[1]),
                    Long.parseLong(split[2]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        }));

        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(WaterSensor::getId);

        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum("vc")
                .print(">>>>>>>>");

        env.execute();

    }

}
