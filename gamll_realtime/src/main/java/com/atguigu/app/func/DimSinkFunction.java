package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DruidDSUtil;
import com.atguigu.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private DruidDataSource druidDataSource = null;

    //value:{"database":"gmall-211126-flink","table":"base_trademark","type":"update","ts":1652499176,"xid":188,"commit":true,"data":{"id":13,"tm_name":"atguigu","logo_url":"/bbb/bbb"},"old":{"logo_url":"/aaa/aaa"}}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        super.invoke(value, context);
        //获取链接
        DruidPooledConnection cnn = druidDataSource.getConnection();

        //将数据写出到Hbase
        PhoenixUtil.upsertValues(cnn,value.getString("sinkTable"),value.getJSONObject("data"));
        //释放链接 即将链接归还到池子中
        cnn.close();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        druidDataSource = DruidDSUtil.createDataSource();
    }
}
