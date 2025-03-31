use dataWarehouse;
-- 本地模式
set hive.exec.mode.local.auto = ture;

-- 创建临时表接收数据
CREATE temporary external TABLE IF NOT EXISTS ads_all_temp (
    json string
)
LOCATION "/hive/dataWarehouse/ads/ads_all_temp";
-- 导入数据
load data inpath "/flume/events/25-03-30" overwrite into table  ads_all_temp;

-- 创建orc snappy表
create external table if not exists ads_all_orc(
    json string
)stored as orc
    LOCATION "/hive/dataWarehouse/ads/ads_all_orc"
tblproperties ("orc.compress"="snappy");

--导入数据
insert into table ads_all_orc
select * from ads_all_temp;
show tables ;

-- 查看数据条数
-- 结果可知筛选出page日志需要设置where get_json_object(json,"$.page") is not null
-- 1m3s228ms
select count(get_json_object(json,"$.common")) from ads_all_orc;
-- 51s401ms
select count(*) from ads_all_orc;

-- 50s516ms
select count(get_json_object(json,"$.common")) from ads_all_orc
where get_json_object(json,"$.page") is not null ;
-- 57s789ms
select count(*) from ads_all_orc
where get_json_object(json,"$.page") is not null ;


-- 路径分析
CREATE EXTERNAL TABLE if not exists ads_page_path
(
    `dt` STRING COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `source` STRING COMMENT '跳转起始页面ID',
    `target` STRING COMMENT '跳转终到页面ID',
    `path_count` BIGINT COMMENT '跳转次数'
)COMMENT '页面浏览路径'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored as orc
LOCATION "/hive/dataWarehouse/ads/ads_page_path"
tblproperties ("orc.compress"="snappy");

-- 2020-06-14
-- count(mid) 1m39s
insert into table ads_page_path
select
    "2020-06-15" as dt,
    sub_date,
    page_id,
    next_page_id,
    count(*)
from (
         select
             mid,
             page_id,
             lead(page_id) over (partition by mid order by ts asc ) as next_page_id,
             from_unixtime(cast(substr(ts,1,10) as bigint),"yyyy-MM-dd") as page_date
         from (
                  select
                      get_json_object(json,'$.common.mid') as mid,
                      get_json_object(json,'$.page.page_id') as page_id,
                      get_json_object(json,'$.ts') as ts
                  from ads_all_orc where get_json_object(json,'$.page') is not null
              )t1
     )t2 lateral view explode(array(1,7,30)) sub_date as sub_date
where datediff("2020-06-15",page_date) between 1 and sub_date
group by sub_date,page_id,next_page_id;

select * from ads_page_path;





-- 流失用户数和回流用户数
CREATE EXTERNAL TABLE if not exists `ads_user_change` (
    `dt` STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count` BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION "/hive/dataWarehouse/ads/ads_user_change"
    tblproperties ("orc.compress"="snappy");



-- 1m44s45ms
-- 开窗资源消耗更大
with tmp as (
    select
        get_json_object(json,"$.common.mid") as mid,
        max(from_unixtime(cast(substr(get_json_object(json,"$.ts"),1,10) as bigint),"yyyy-MM-dd"))
            over (partition by get_json_object(json,"$.common.mid"))as last_date
    from ads_all_orc

)select mid,last_date from tmp group by mid,last_date;

-- 50s396ms
-- 流失用户,只包含7日前当天活跃，但最近7日未活跃的用户总数。
insert into table ads_user_change
with t1 as (
    select
        get_json_object(json,"$.common.mid") as mid,
        from_unixtime(cast(substr(get_json_object(json,"$.ts"),1,10) as bigint),"yyyy-MM-dd") as page_date
    from ads_all_orc
),t2 as(
    select
        mid,
        page_date,
        row_number() over (partition by mid order by page_date desc ) as rn
    from t1
    group by mid,page_date
),t3 as (
--     t3即为流失用户
    select
        "2020-06-14" as dt,
        count(*) as user_churn_count
    from t2
    where rn = 1 and datediff("2020-06-14",page_date) = 7
),t4 as (
    select
        page_date,
        lag(page_date) over (partition by mid order by page_date asc ) as max_lag_date
    from t2
    where rn = 1 and page_date = "2020-06-14"
),t5 as (
--     t5即为回归用户
    select count(*) as user_back_count from t4
    where datediff(page_date,max_lag_date)>7
)select t3.dt, t3.user_churn_count,t5.user_back_count from t3 cross join t5




