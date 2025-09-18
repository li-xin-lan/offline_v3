import os

import read_mysql_table_cols
import sys

host= "cdh03"
port= 3306
database= "realtime_v1"
user= "root"
password= "root"
read_hdfs="/bigdata_warehouse/bigdata_realtime_v1"
output_file = "./output_file/hive_ddl.txt"# 定义输出文件路径
hive_database = 'bigdata_realtime_v1'

tables_to_process = [
    "activity_info", "activity_rule", "base_category1", "base_category2", "base_category3",
    "base_dic", "base_province", "base_region", "base_trademark", "cart_info", "coupon_info",
    "sku_attr_value", "sku_info", "sku_sale_attr_value", "spu_info", "promotion_pos",
    "promotion_refer", "cart_info", "comment_info", "coupon_use", "favor_info",
    "order_detail", "order_detail_activity", "order_detail_coupon", "order_info",
    "order_refund_info", "order_status_log", "payment_info", "refund_payment", "user_info"
]

mysql_config = read_mysql_table_cols.get_mysql_tables_and_columns(host,port,database,user,password,tables_to_process)

with open(output_file, "a", encoding="utf-8") as f:
    # 打印结果（可选）
    if mysql_config:
        # print("\n提取结果汇总:")
        for table in mysql_config:
            # print(f"\n表名: {table['table_name']}")
            # print(f"字段: {', '.join(table['columns'])}")


            create_sql = f"""create database if not exists {hive_database};
    
    use {hive_database};
    -- # mysql to hive ddl test
    CREATE DATABASE IF NOT EXISTS {hive_database}
    COMMENT 'test-warehouse'
    LOCATION 'hdfs://cdh01:8020{read_hdfs}/'
    WITH DBPROPERTIES (
        'creator' = 'csq',
                    'created_date' = '2025-09-16'
    );
    
    drop table {hive_database}.ods_{table['table_name']};
    create external table {hive_database}.ods_{table['table_name']}(
        {' String, '.join(table['columns'])} String
    )
    PARTITIONED BY (ds STRING)
    LOCATION 'hdfs://cdh01:8020{read_hdfs}/ods_{table['table_name']}/'
    TBLPROPERTIES (
        'orc.compress' = 'SNAPPY',
                         'external.table.purge' = 'true'
    );"""

            # 写入文件
            f.write(create_sql)
            # 同时打印到控制台（可选）
            print(create_sql)










