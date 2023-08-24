package com.atguigu.utils;

import com.atguigu.common.GmallConfig;
import com.atguigu.domain.TransientSink;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil_1 {

    public static <T> SinkFunction<T> getClickHouseSinkFunc(String sql)
    {
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement pst, T t) throws SQLException {

                        Class<?> tClass = t.getClass();

                        Field[] declaredFields = tClass.getDeclaredFields();
                        int offset = 0 ; // 跳过一个字段就自增一次
                        for (int i=0;i<declaredFields.length;i++) {

                            declaredFields[i].setAccessible(true);
                            //获取到字段上的注解
                            TransientSink transientSink = declaredFields[i].getAnnotation(TransientSink.class);
                            if (transientSink !=null)
                            {
                                offset++;
                                continue;
                            }
                            Object value = declaredFields[i].get(t);
                            pst.setObject(i+1-offset,value);

                        }

                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }
}
