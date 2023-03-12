package com.atguigu.utils;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class PhoenixUtil {

    //"data":{"id":13,"tm_name":"atguigu","logo_url":"/bbb/bbb"}
    public static void upsertValues(DruidPooledConnection cnn, String sinkTable, JSONObject datas) throws SQLException {
        //拼接sql upsert into db.xx(id,name) values();

        Set<String> keys = datas.keySet();
        Collection<Object> values = datas.values();
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + " (" + StringUtils.join(keys, ",") + ") values ('"
                + StringUtils.join(values, "','") + "')";

        //预编译SQL
        PreparedStatement pst = cnn.prepareStatement(sql);

        //执行sql
        pst.execute();
        cnn.commit();
        //释放资源
        pst.close();

    }
}
