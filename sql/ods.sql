-- 创建数据库并使用
create database if not exists dataWarehouse location "/hive/dataWarehouse";
use dataWarehouse;

-- 本地模式
set hive.exec.mode.local.auto = ture;

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
    `create_time` date
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



-- 导入数据
load data inpath "/hive/mysql_data/sku_info" overwrite into table ods_sku_info;
load data inpath "/hive/mysql_data/base_trademark" overwrite into table ods_base_trademark;
load data inpath "/hive/mysql_data/spu_info" overwrite into table ods_spu_info;
load data inpath "/hive/mysql_data/base_category3" overwrite into table ods_base_category3;
load data inpath "/hive/mysql_data/base_category2" overwrite into table ods_base_category2;
load data inpath "/hive/mysql_data/base_category1" overwrite into table ods_base_category1;



-- 查询数据
select * from ods_sku_info;
select * from ods_base_trademark;
select * from ods_spu_info;
select * from ods_base_category3;
select * from ods_base_category2;
select * from ods_base_category1;

