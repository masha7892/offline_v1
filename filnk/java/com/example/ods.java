package com.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ods {
    public static void main(String[] args) throws Exception {
//        流执行环境 StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        表执行环境 StreamTableEnvironment
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
//        并行度1
        env.setParallelism(1);


        String sql = "CREATE TABLE json_source (" +
                "common row(" +
                "ar STRING," +
                "ba STRING," +
                "ch STRING," +
                "is_new STRING," +
                "md STRING," +
                "mid STRING," +
                "os STRING," +
                "uid STRING," +
                "vc STRING" +
                ") )with(" +
                "'connector' = 'filesystem'," +
                "'path' = 'hdfs://cdh01:8020/flume/events/25-03-28'," +
                "'format' = 'json'," +
                "'json.ignore-parse-errors' = 'true'," +
                "'json.fail-on-missing-field' = 'false'" +
                ")";
        tEnv.executeSql(sql);

        String sqlQuery = "SELECT common.ar,common.ba\n" +
                "FROM json_source";
        Table table = tEnv.sqlQuery(sqlQuery);

        tEnv.toDataStream(table).map(row -> row.toString().replaceAll("\\+|I|\\[|\\]", "")).print();
        env.execute();

    }
}
