-- 使用数据库
use dataWarehouse;

-- 本地模式
set hive.exec.mode.local.auto = ture;
-- 启动负载均衡
set hive.groupby.skewindata=true;


-- 商品维度宽表  dwd_dim_sku_info
create external table if not exists dwd_dim_sku_info(
    `sku_id` bigint,
    `sku_price` decimal(10,0),
    `sku_name` varchar(200),


    `spu_id` bigint,
    `spu_name` varchar(200),

    `tm_id` bigint,
    `tm_name` varchar(20),

    `category3_id` bigint,
    `category3_name` varchar(200),

    `category2_id` bigint,
    `category2_name` varchar(200),

    `category1_id` bigint,
    `category1_name` varchar(200)

)
row format delimited fields terminated by ","
location "/hive/dataWarehouse/dwd/dwd_dim_sku_info";

-- 清空外表数据
-- alter table dwd_dim_sku_info set TBLPROPERTIES('EXTERNAL'='false');
-- show create table dwd_dim_sku_info;
-- truncate table dwd_dim_sku_info;
-- alter table dwd_dim_sku_info set TBLPROPERTIES('EXTERNAL'='true');

insert into  dwd_dim_sku_info
select
    oki.id,
    oki.price,
    oki.sku_name,

    oki.spu_id,
    opi.spu_name,

    oki.tm_id,
    obt.tm_name,

    oki.category3_id,
    ob3.name,

    ob3.category2_id,
    ob2.name,

    ob2.category1_id,
    ob1.name
from ods_sku_info oki
    left join ods_base_trademark obt on oki.tm_id = obt.tm_id
    left join ods_spu_info opi on oki.spu_id = opi.id
    left join ods_base_category3 ob3 on ob3.id = opi.category3_id
    left join ods_base_category2 ob2 on ob2.id = ob3.category2_id
    left join ods_base_category1 ob1 on ob1.id = ob2.category1_id;


select * from dwd_dim_sku_info;






-- 订单事实表  dwd_dim_order_detail
create external table  dwd_dim_order_detail(
    `detail_id` bigint,
    `sku_id` bigint,
    `order_price` decimal(10,2),
    `sku_num` varchar(200),

    `order_id` bigint,
    `order_status` varchar(20),
    `user_id` bigint,
    `consignee` varchar(100),
    `delivery_address` varchar(1000),
    `province_id` TINYINT,
    `original_total_amount` decimal(16,2),
    `benefit_reduce_amount` decimal(16,2),
    `feight_fee` decimal(16,2),
    `final_total_amount` decimal(16,2),
    `create_time` string
)partitioned by (dt string)
row format delimited fields terminated by ","
location "/hive/dataWarehouse/dwd/dwd_dim_order_detail";


-- alter table dwd_dim_order_detail set tblproperties ("EXTERNAL"="false");
-- show create table dwd_dim_order_detail;
-- truncate table dwd_dim_order_detail;
-- select * from dwd_dim_order_detail;
-- alter table dwd_dim_order_detail set tblproperties ("EXTERNAL"="true");


-- 2020-03-15
insert into dwd_dim_order_detail partition (dt="2020-03-15")
select
    id1,
    sku_id,
    order_price,
    sku_num,

    id2,
    order_status,
    user_id,
    consignee,
    delivery_address,
    province_id,
    original_total_amount,
    benefit_reduce_amount,
    feight_fee,
    final_total_amount,
    create_time
from (
         select
             ood.id id1,
             ood.sku_id,
             ood.order_price,
             ood.sku_num,

             ooi.id id2,
             ooi.order_status,
             ooi.user_id,
             ooi.consignee,
             ooi.delivery_address,
             ooi.province_id,
             ooi.original_total_amount,
             ooi.benefit_reduce_amount,
             ooi.feight_fee,
             ooi.final_total_amount,
             ooi.create_time,
             split(ooi.create_time," ")[0] dt
         from ods_order_detail ood
                  left join ods_order_info ooi on ood.order_id = ooi.id
     )t
where dt = "2020-03-15";


-- 2020-03-16
insert into dwd_dim_order_detail partition (dt="2020-03-16")
select
    id1,
    sku_id,
    order_price,
    sku_num,

    id2,
    order_status,
    user_id,
    consignee,
    delivery_address,
    province_id,
    original_total_amount,
    benefit_reduce_amount,
    feight_fee,
    final_total_amount,
    create_time
from (
         select
             ood.id id1,
             ood.sku_id,
             ood.order_price,
             ood.sku_num,

             ooi.id id2,
             ooi.order_status,
             ooi.user_id,
             ooi.consignee,
             ooi.delivery_address,
             ooi.province_id,
             ooi.original_total_amount,
             ooi.benefit_reduce_amount,
             ooi.feight_fee,
             ooi.final_total_amount,
             ooi.create_time,
             split(ooi.create_time," ")[0] dt
         from ods_order_detail ood
                  left join ods_order_info ooi on ood.order_id = ooi.id
     )t
where dt = "2020-03-16";



-- 2020-03-17
insert into dwd_dim_order_detail partition (dt="2020-03-17")
select
    id1,
    sku_id,
    order_price,
    sku_num,

    id2,
    order_status,
    user_id,
    consignee,
    delivery_address,
    province_id,
    original_total_amount,
    benefit_reduce_amount,
    feight_fee,
    final_total_amount,
    create_time
from (
         select
             ood.id id1,
             ood.sku_id,
             ood.order_price,
             ood.sku_num,

             ooi.id id2,
             ooi.order_status,
             ooi.user_id,
             ooi.consignee,
             ooi.delivery_address,
             ooi.province_id,
             ooi.original_total_amount,
             ooi.benefit_reduce_amount,
             ooi.feight_fee,
             ooi.final_total_amount,
             ooi.create_time,
             split(ooi.create_time," ")[0] dt
         from ods_order_detail ood
                  left join ods_order_info ooi on ood.order_id = ooi.id
     )t
where dt = "2020-03-17";



-- 2020-03-18
insert into dwd_dim_order_detail partition (dt="2020-03-18")
select
    id1,
    sku_id,
    order_price,
    sku_num,

    id2,
    order_status,
    user_id,
    consignee,
    delivery_address,
    province_id,
    original_total_amount,
    benefit_reduce_amount,
    feight_fee,
    final_total_amount,
    create_time
from (
         select
             ood.id id1,
             ood.sku_id,
             ood.order_price,
             ood.sku_num,

             ooi.id id2,
             ooi.order_status,
             ooi.user_id,
             ooi.consignee,
             ooi.delivery_address,
             ooi.province_id,
             ooi.original_total_amount,
             ooi.benefit_reduce_amount,
             ooi.feight_fee,
             ooi.final_total_amount,
             ooi.create_time,
             split(ooi.create_time," ")[0] dt
         from ods_order_detail ood
                  left join ods_order_info ooi on ood.order_id = ooi.id
     )t
where dt = "2020-03-18";


-- 2020-03-19
insert into dwd_dim_order_detail partition (dt="2020-03-19")
select
    id1,
    sku_id,
    order_price,
    sku_num,

    id2,
    order_status,
    user_id,
    consignee,
    delivery_address,
    province_id,
    original_total_amount,
    benefit_reduce_amount,
    feight_fee,
    final_total_amount,
    create_time
from (
         select
             ood.id id1,
             ood.sku_id,
             ood.order_price,
             ood.sku_num,

             ooi.id id2,
             ooi.order_status,
             ooi.user_id,
             ooi.consignee,
             ooi.delivery_address,
             ooi.province_id,
             ooi.original_total_amount,
             ooi.benefit_reduce_amount,
             ooi.feight_fee,
             ooi.final_total_amount,
             ooi.create_time,
             split(ooi.create_time," ")[0] dt
         from ods_order_detail ood
                  left join ods_order_info ooi on ood.order_id = ooi.id
     )t
where dt = "2020-03-19";


-- 2020-03-20
insert into dwd_dim_order_detail partition (dt="2020-03-20")
select
    id1,
    sku_id,
    order_price,
    sku_num,

    id2,
    order_status,
    user_id,
    consignee,
    delivery_address,
    province_id,
    original_total_amount,
    benefit_reduce_amount,
    feight_fee,
    final_total_amount,
    create_time
from (
         select
             ood.id id1,
             ood.sku_id,
             ood.order_price,
             ood.sku_num,

             ooi.id id2,
             ooi.order_status,
             ooi.user_id,
             ooi.consignee,
             ooi.delivery_address,
             ooi.province_id,
             ooi.original_total_amount,
             ooi.benefit_reduce_amount,
             ooi.feight_fee,
             ooi.final_total_amount,
             ooi.create_time,
             split(ooi.create_time," ")[0] dt
         from ods_order_detail ood
                  left join ods_order_info ooi on ood.order_id = ooi.id
     )t
where dt = "2020-03-20";



