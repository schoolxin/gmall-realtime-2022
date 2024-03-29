package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Iterator;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 获取执行环境
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数

        //TODO 消费topic_log 主题
        String topic = "topic_log";
        String groupId = "base_log_app";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 过滤掉 非json格式的数据  并将每行数据转化为json对象 要保留脏数据 那么用侧输出流将脏数据进行输出
        OutputTag<String> dirtyTag = new OutputTag<String>("dirty"){};

        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, s);
//                    e.printStackTrace();
                }

            }
        });
        //获取脏数据
        DataStream<String> dirtyDS = jsonObjDs.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty=========>");


        //TODO 按mid分组
        KeyedStream<JSONObject, String> JsonObjKeyedStream = jsonObjDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 使用状态编程做新老访客标记校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = JsonObjKeyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
                super.open(parameters);
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //获取is_new 标记 和时间戳
                String isNew = value.getJSONObject("common").getString("is_new");
                Long ts = value.getLong("ts");
                //将时间戳转化为YYYYMMDD
                String curDate = DateFormatUtil.toDate(ts);
                //获取状态中的日期
                String lastDate = lastVisitState.value();
                if ("1".equals(isNew)) {
                    if (lastDate == null) {
                        lastVisitState.update(curDate);
                    } else if (!lastDate.equals(curDate)) {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastDate == null) {
                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                }
                return value;
            }
        });

        //TODO 使用侧输出流进行分流处理 页面日志放到主流 启动 曝光 动作 错误放到侧输出流
        //错误 日志有可能是 启动 有可能是页面   页面中 包含曝光 和 动作
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};
        OutputTag<String> actionTag = new OutputTag<String>("action"){};
        OutputTag<String> errorTag = new OutputTag<String>("error"){};
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                //获取错误流 跟启动和页面都有可能有
                String err = value.getString("err");
                if (err != null) {
                    ctx.output(errorTag, value.toJSONString());
                }
                //移除错误信息 可能跟页面有关系
                value.remove("err");
                //获取启动信息  启动信息和页面信息  没有关系
                String start = value.getString("start");
                if (start != null) {
                    ctx.output(startTag, value.toJSONString());
                } else {
                    //获取common  page_id  ts 补充displays 和actions
                    String common = value.getString("common");
                    String page_id = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");
                    //尝试获取 displays 和 actions
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page_id", page_id);
                            display.put("ts", ts);
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i > actions.size(); i++) {
                            JSONObject aciton = actions.getJSONObject(i);
                            aciton.put("common", common);
                            aciton.put("page_id", page_id);
                            ctx.output(actionTag, aciton.toJSONString());
                        }
                    }
                }


                //将actions 和displays 页面信息只留下简单的common  page  ts 信息
                value.remove("actions");
                value.remove("displays");

                //将页面信息 作为主流写出
                out.collect(value.toJSONString());


            }
        });

        //TODO 提取各个侧输出流数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);

        //TODO 将数据打印并写入对应的主题
        pageDS.print("Page>>>>>>>>>>");
        startDS.print("Start>>>>>>>>");
        displayDS.print("Display>>>>");
        actionDS.print("Action>>>>>>");
        errorDS.print("Error>>>>>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));

        //TODO 启动任务
        env.execute();
    }
}
