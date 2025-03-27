-- 创建数据库并使用
create database if not exists dataWarehouse location "/hive/dataWarehouse";
use dataWarehouse;

-- 本地模式
set hive.exec.mode.local.auto = ture;

-- 删除表
drop table if exists ods_sku_info;
drop table if exists ods_base_trademark;
drop table if exists ods_spu_info;
drop table if exists ods_base_category3;
drop table if exists ods_base_category2;
drop table if exists ods_base_category1;
drop table if exists ods_order_info;
drop table if exists ods_order_detail;
show tables ;

-- ods_sku_info
create  external table if not exists ods_sku_info(
    `id` bigint,
    `spu_id` bigint,
    `price` decimal(10,0),
    `sku_name` varchar(200),
    `sku_desc` varchar(200),
    `weight` decimal(10,0),
    `tm_id` bigint,
    `category3_id` bigint,
    `sku_default_img` varchar(200),
    `create_time` string
)row format delimited fields terminated by ","
location "/hive/dataWarehouse/ods/ods_sku_info";


-- ods_base_trademark
create external table if not exists ods_base_trademark(
    `tm_id` varchar(20),
    `tm_name` varchar(20)
)row format delimited fields terminated by ","
location "/hive/dataWarehouse/ods/ods_base_trademark";

-- ods_spu_info
create external table if not exists ods_spu_info(
    `id` bigint,
    `spu_name` varchar(200),
    `description` varchar(1000),
    `category3_id` bigint,
    `tm_id` bigint
)row format delimited fields terminated by ","
location "/hive/dataWarehouse/ods/ods_spu_info";


-- ods_base_category3
create external table if not exists ods_base_category3(
    `id` bigint,
    `name` varchar(200),
    `category2_id` bigint
)row format delimited fields terminated by ","
location "/hive/dataWarehouse/ods/ods_base_category3";


-- ods_base_category2
create external table if not exists ods_base_category2(
    `id` bigint,
    `name` varchar(200),
    `category1_id` bigint
)row format delimited fields terminated by ","
location "/hive/dataWarehouse/ods/ods_base_category2";


-- ods_base_category1
create external table if not exists ods_base_category1(
    `id` bigint,
    `name` varchar(10)
)row format delimited fields terminated by ","
location "/hive/dataWarehouse/ods/ods_base_category1";


-- ods_order_info
create external table if not exists ods_order_info(
    `id` bigint,
    `consignee` varchar(100),
    `consignee_tel` varchar(20),
    `final_total_amount` decimal(16,2),
    `order_status` varchar(20),
    `user_id` bigint,
    `delivery_address` varchar(1000),
    `order_comment` varchar(200),
    `out_trade_no` varchar(50),
    `trade_body` varchar(200),
    `create_time` string,
    `operate_time` string,
    `expire_time` string,
    `tracking_no` varchar(100),
    `parent_order_id` bigint,
    `img_url` varchar(200),
    `province_id` TINYINT,
    `benefit_reduce_amount` decimal(16,2),
    `original_total_amount` decimal(16,2),
    `feight_fee` decimal(16,2)
)row format delimited fields terminated by ","
location "/hive/dataWarehouse/ods/ods_order_info";


-- ods_order_detail
create external table if not exists ods_order_detail(
    `id` bigint,
    `order_id` bigint,
    `sku_id` bigint,
    `sku_name` varchar(200),
    `img_url` varchar(200),
    `order_price` decimal(10,2),
    `sku_num` varchar(200),
    create_time string
)
row format delimited fields terminated by ","
location "/hive/dataWarehouse/ods/ods_order_detail";


-- 导入数据


load data inpath "/hive/mysql_data/sku_info" overwrite into table ods_sku_info;
load data inpath "/hive/mysql_data/base_trademark" overwrite into table ods_base_trademark;
load data inpath "/hive/mysql_data/spu_info" overwrite into table ods_spu_info;
load data inpath "/hive/mysql_data/base_category3" overwrite into table ods_base_category3;
load data inpath "/hive/mysql_data/base_category2" overwrite into table ods_base_category2;
load data inpath "/hive/mysql_data/base_category1" overwrite into table ods_base_category1;
load data inpath "/hive/mysql_data/order_info" overwrite into table ods_order_info;
load data inpath "/hive/mysql_data/order_detail" overwrite into table ods_order_detail;


-- 查询数据
select * from ods_sku_info;
select * from ods_base_trademark;
select * from ods_spu_info;
select * from ods_base_category3;
select * from ods_base_category2;
select * from ods_base_category1;
select * from ods_order_info;
select * from ods_order_detail;

