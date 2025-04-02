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
-- select count(get_json_object(json,"$.common")) from ads_all_orc;
-- -- 51s401ms
-- select count(*) from ads_all_orc;
--
-- -- 50s516ms
-- select count(get_json_object(json,"$.common")) from ads_all_orc
-- where get_json_object(json,"$.page") is not null ;
-- -- 57s789ms
-- select count(*) from ads_all_orc
-- where get_json_object(json,"$.page") is not null ;


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
insert overwrite  table ads_page_path
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




with t1 as (
    select
        get_json_object(json,'$.common.mid') as mid,
        from_unixtime(cast(substr(get_json_object(json,'$.ts'),1,10) as bigint),"yyyy-MM-dd") as page_date
    from ads_all_orc
),t2 as (
    select
        mid,
        page_date,
        row_number() over (partition by mid order by page_date desc) as rn
    from t1
    group by mid, page_date
)insert overwrite table ads_user_change
select
     "2020-06-14" as dt,
     -- 流失: 最后一次活跃为七天前当天
     sum(if(rn=1 and datediff("2020-06-14",page_date)=7,1,0)) as user_churn_count,
     -- 回流: 今天活跃,上一次活跃在七天前
     sum(if(((rn=1 and page_date="2020-06-14") and (rn = 2 and datediff("2020-06-14",page_date)>7)),1,0)) as user_back_count
 from t2;


select * from ads_user_change;





-- 品牌复购率
CREATE EXTERNAL TABLE `ads_repeat_purchase` (
    `dt` STRING COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id` STRING COMMENT '品牌ID',
    `tm_name` STRING COMMENT '品牌名称',
    `order_repeat_rate` DECIMAL(16,2) COMMENT '复购率'
) COMMENT '品牌复购率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION "/hive/dataWarehouse/ads/ads_repeat_purchase"
    tblproperties ("orc.compress"="snappy");



-- 最近1,7,30天的各品牌复购率, 重复购买即购买次数大于等于2 比 购买过即购买次数大于1。
-- 用户 订单 品牌 (购买次数进行分组计数,得到购买次数)
-- 各个品牌,大于2的/大于1的
-- 使用lateral view explode求出1 7 30天
with t1 as (
    select
        ddod.user_id,
        ddod.order_id,
        ddod.dt,
        ddsi.tm_id,
        nvl(ddsi.tm_name,"未知品牌") as tm_name,
        count(*) as num
    from dwd_dim_order_detail ddod inner join dwd_dim_sku_info ddsi
    on ddod.sku_id = ddsi.sku_id
    group by ddod.user_id,
             ddod.order_id,
             ddod.dt,
             ddsi.tm_id,
             ddsi.tm_name
)insert overwrite table ads_repeat_purchase
select
     "2020-03-18" as dt,
     sub_date,
     tm_id,
     tm_name,
     sum(if(num>2,1,0))/sum(if(num>1,1,0)) as order_repeat_rate
from t1
lateral view explode(array(1,7,30))sub_date as sub_date
where datediff("2020-03-18",dt) between 1 and sub_date
group by sub_date,tm_id,tm_name;


select * from ads_repeat_purchase;



-- 4、该需求包含订单总数，订单总金额和下单总人数
create external table if not exists ads_orderCount_totalAmount_headCount(
    orderCount bigint,
    totalAmount bigint,
    headCount bigint
) COMMENT '订单总数,订单总金额和下单总人数'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION "/hive/dataWarehouse/ads/ads_orderCount_totalAmount_headCount"
    tblproperties ("orc.compress"="snappy");



with t1 as (
    select
        ddod.order_id,
        ddod.final_total_amount
    from dwd_dim_order_detail ddod
    group by ddod.order_id,ddod.final_total_amount
),t2 as (
    select
        count(order_id) as order_count,
        sum(final_total_amount) as total_amount
    from t1
),t3 as (
    select
        count(distinct user_id) as head_count
    from dwd_dim_order_detail
)insert overwrite table ads_orderCount_totalAmount_headCount
select
     t2.order_count,t2.total_amount,t3.head_count
 from t2 cross join t3;


select * from ads_orderCount_totalAmount_headCount;




-- 5、该需求包含各省份订单总数和订单总金额
create external table if not exists ads_province_orderCount_totalAmount(
    provinceId string,
    provinceName string,
    orderCount bigint,
    totalAmount double
)COMMENT '各省份订单总数和订单总金额'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    stored as orc
    LOCATION "/hive/dataWarehouse/ads/ads_province_orderCount_totalAmount"
    tblproperties ("orc.compress"="snappy");

with t1 as (
    select
        ddod.province_id,
        obp.name,
        ddod.order_id,
        ddod.final_total_amount
    from dwd_dim_order_detail ddod left join ods_base_province obp
                                             on ddod.province_id = obp.id
    group by ddod.province_id,obp.name,ddod.order_id,ddod.final_total_amount
)insert overwrite table ads_province_orderCount_totalAmount
select
     province_id,
     name,
     count(order_id),
     sum(final_total_amount)
from t1
group by province_id,name;

select  * from ads_province_orderCount_totalAmount;



