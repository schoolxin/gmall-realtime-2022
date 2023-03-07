package com.atguigu.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.domain.TableProcess;
import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    private Connection cnn;



    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    //value:{"database":"gmall-211126-flink","table":"base_trademark","type":"update","ts":1652499176,"xid":188,"commit":true,"data":{"id":13,"tm_name":"atguigu","logo_url":"/bbb/bbb"},"old":{"logo_url":"/aaa/aaa"}}
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        //获取广播的配置数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(this.mapStateDescriptor);
        String table = value.getString("table");
        TableProcess tableProcess = broadcastState.get(table);

        //过滤字段
        if (tableProcess!=null)
        {
            filterColumns(value.getJSONObject("data"),tableProcess.getSinkColumns());
            //添加sinkTable
            value.put("sinkTable",tableProcess.getSinkTable());
            out.collect(value);

        }else
        {
            System.out.println("找不到对应的Key：" + table);
        }


    }

    /**
     * 过滤字段
     * @param data {"id":"A001","name":"dd","tname":"dd"}
     * @param sinkColumns "id,name"
     */
    private void filterColumns(JSONObject data, String sinkColumns) {

        String[] columnsArr = sinkColumns.split(",");
        List<String> columnsList = Arrays.asList(columnsArr);
        Iterator<Map.Entry<String, Object>> dataIter = data.entrySet().iterator();
        while (dataIter.hasNext())
        {
            Map.Entry<String, Object> objectEntry = dataIter.next();
            if(!columnsList.contains(objectEntry.getKey()))
            {
                dataIter.remove();
            }
        }

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        cnn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);


    }



    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> collector) throws Exception {



        JSONObject jsonObject = JSON.parseObject(value); //将字符串转为 json对象

        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class); //将字符串转java对象 转的时候以类名和json中的key名匹配转

        System.out.println(tableProcess.toString());
        //校验并创建表
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());
        //写到状态中 广播出去  广播状态在广播处理方法中获取 其他的状态在open中获取
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(this.mapStateDescriptor); //获取到状态

        broadcastState.put(tableProcess.getSourceTable(), tableProcess);




    }

    /**
     *
     * @param sinkTable
     * @param sinkColumns
     * @param sinkPk
     * @param sinkExtend
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        PreparedStatement pst=null;

        try {
            //处理特殊字段
            if (sinkPk==null||"".equals(sinkPk))
            {
                sinkPk="id";
            }
            if (sinkExtend==null)
            {
                sinkExtend="";
            }

            //拼接sql

            StringBuffer stringBuffSql = new StringBuffer();

            StringBuffer createTableSql = stringBuffSql.append("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            //拼接字段
            String[] columns = sinkColumns.split(",");

            for (int i = 0; i < columns.length; i++) {

                //取出字段
                String column = columns[i];
                //判断取出来的字段是否为主键
                if (sinkPk.equals(column))
                {
                    createTableSql.append(column).append(" varchar primary key");
                }else
                {
                    createTableSql.append(column).append(" varchar");
                }
                //判断是否为最后一个字段
                if (i<columns.length-1)
                {
                    createTableSql.append(",");
                }

            }
            //for 循环完
            createTableSql.append(")").append(sinkExtend);
            //编译sql

            System.out.println("建表语句"+createTableSql);

            pst = cnn.prepareStatement(createTableSql.toString());
            //执行sql
            pst.execute();

        } catch (SQLException e) {

            throw new RuntimeException("建表失败"+sinkTable); //手动抛出运行时异常 一旦报错 程序就退出了
        } finally {

            //释放资源
            if(pst!=null)
            {
                try {
                    pst.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }

    }
}
