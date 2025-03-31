package com.example;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ods {
    public static void main(String[] args) throws Exception {
//流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//流处理表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


//并行度1
        env.setParallelism(1);

//建表,读取hdfs的复杂json
//其中包含多条不同类型的json,启动日志,报错日志,曝光日志等
//其中部分日志还有array嵌套
//设置了
        String sql = "CREATE TABLE json_source (" +
                "    common ROW<\n" +
                "        ar STRING,\n" +
                "        ba STRING,\n" +
                "        ch STRING,\n" +
                "        is_new STRING,\n" +
                "        md STRING,\n" +
                "        mid STRING,\n" +
                "        os STRING,\n" +
                "        uid STRING,\n" +
                "        vc STRING\n" +
                "    >,\n" +
                "    err ROW<\n" +
                "        error_code STRING,\n" +
                "        msg STRING\n" +
                "    >,\n" +
                "    `start` ROW<\n" +
                "        entry STRING,\n" +
                "        loading_time STRING,\n" +
                "        open_ad_id STRING\n," +
                "        open_ad_ms STRING\n," +
                "        open_ad_skip_ms STRING\n" +
                "    >,\n" +
                "    page ROW<\n" +
                "        during_time STRING,\n" +
                "        item STRING,\n" +
                "        item_type STRING,\n" +
                "        last_page_id STRING,\n" +
                "        page_id STRING,\n" +
                "        source_type STRING\n" +
                "    >,\n" +
                "    actions ARRAY<ROW<\n" +
                "        action_id STRING,\n" +
                "        item STRING,\n" +
                "        item_type STRING,\n" +
                "        ts BIGINT\n" +
                "    >>,\n" +
                "    displays ARRAY<ROW<\n" +
                "        display_type STRING,\n" +
                "        item STRING,\n" +
                "        item_type STRING,\n" +
                "        `order` INT,\n" +
                "        pos_id INT\n" +
                "    >>,\n" +
                "    ts bigint," +
                //毫秒时间戳转年月日时间戳
                " time_ltz AS TO_TIMESTAMP_LTZ(ts, 3)," +
                //水位线
                " WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND" +
                ")with(" +
                "'connector' = 'filesystem'," +
                "'path' = 'hdfs://cdh01:8020/hive/dataWarehouse/ads/ads_all_temp'," +
                "'format' = 'json'," +
                //解析错误忽略
                "'json.ignore-parse-errors' = 'true'," +
                //允许确实
                "'json.fail-on-missing-field' = 'false'" +
                ")";
        tEnv.executeSql(sql);

//全部数据的窗口计数
        String sql_count_all = "SELECT window_start, window_end, count(*) as num\n" +
                "FROM TABLE(\n" +
                "    CUMULATE(TABLE json_source, DESCRIPTOR(time_ltz), INTERVAL '1' hours, INTERVAL '1' hours))\n" +
                "GROUP BY window_start, window_end;";
        Table table_count_all = tEnv.sqlQuery(sql_count_all);
        tEnv.toDataStream(table_count_all).print("全部数据数目:");



//过滤页面数据生成临时视图
        tEnv.executeSql("create TEMPORARY VIEW view_all AS select * from json_source " +
                "where page is not null ");
        Table table_count_page = tEnv.sqlQuery("select\n" +
                "    window_start, window_end, count(*)\n" +
                "from TABLE (CUMULATE(TABLE view_all,DESCRIPTOR(time_ltz),INTERVAL '1' hours, INTERVAL '1' hours)) " +
                "group by window_start, window_end");
        tEnv.toDataStream(table_count_page).print("页面数据数目:");



        env.execute();

    }
}
