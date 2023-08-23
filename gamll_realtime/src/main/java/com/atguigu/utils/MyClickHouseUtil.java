package com.atguigu.utils;

import com.atguigu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {

    // 泛型方法 要在返回值前 加上泛型
    public static <T> SinkFunction<T> getSinkFunction(String sql) {
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {


                        //使用反射的方式获取t对象中的方法
                        Class<?> tClz = t.getClass();
//                        Method[] methods = tClz.getMethods();
//                        for (Method method : methods) {
//                            method.invoke()
//                        }

                        Field[] fields = tClz.getDeclaredFields();
                        //遍历属性
                        for (int i = 0; i < fields.length; i++) {

                            //获取单个属性
                            Field field = fields[i];
                            field.setAccessible(true);


                            //获取属性值
                            Object value = field.get(t);
                            //给占位符设置值
                            preparedStatement.setObject(i+1,value);

                        }


                    }
                },new JdbcExecutionOptions.Builder()
                                .withBatchSize(5)
                                .withBatchIntervalMs(1000L)
                                .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }
}
