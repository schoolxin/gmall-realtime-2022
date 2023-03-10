package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DruidDSUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private DruidDataSource druidDataSource = null;

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        super.invoke(value, context);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        druidDataSource = DruidDSUtil.createDataSource();
    }
}
