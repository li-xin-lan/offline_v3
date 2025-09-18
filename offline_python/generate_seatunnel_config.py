import mysql.connector
from mysql.connector import Error
import os  # 引入os模块用于路径处理
def get_mysql_tables_and_columns(host, port, database, user, password, specific_tables=None):
    """
    获取MySQL数据库中表名及其字段名，支持指定特定表

    参数:
        specific_tables: 可选，指定要处理的表名列表，如["table1", "table2"]
                         若为None，则处理所有表
    """
    result = []
    connection = None

    try:
        # 建立数据库连接
        connection = mysql.connector.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # 获取所有表名
            cursor.execute("SHOW TABLES")
            all_tables = [table[0] for table in cursor.fetchall()]

            # 确定需要处理的表
            tables_to_process = all_tables
            if specific_tables:
                # 筛选出存在的表
                existing_tables = [t for t in specific_tables if t in all_tables]
                non_existing = [t for t in specific_tables if t not in all_tables]

                if non_existing:
                    print(f"警告: 以下表不存在于数据库中: {', '.join(non_existing)}")

                tables_to_process = existing_tables
                if not tables_to_process:
                    print("没有找到需要处理的有效表")
                    return result

            # 遍历每个表，获取字段名
            for table in tables_to_process:
                cursor.execute(f"DESCRIBE {table}")
                columns = cursor.fetchall()
                column_names = [col[0] for col in columns]

                result.append({
                    'table_name': table,
                    'columns': column_names
                })

    except Error as e:
        print(f"数据库连接错误: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

    return result

def generate_seatunnel_config(
        output_path,  # 修改为完整路径
        mysql_host,
        mysql_port,
        mysql_db,
        mysql_user,
        mysql_password,
        hive_table,
        source_table,
        source_columns,
        metastore_uri,
        parallelism=2,
        job_mode="BATCH"
):
    """生成单个SeaTunnel配置文件"""
    # 拼接查询字段，添加ds分区字段
    columns_str = ", ".join(source_columns)
    query_columns = f"{columns_str}, DATE_FORMAT(NOW(), '%Y%m%d') as ds"

    # 使用三引号和f-string实现可变参数替换
    config_content = f"""# seatunnel version 2.3.10
# bigdata env CDH 6.3.2

# sync_mysql_to_hive_{source_table}.conf

env {{
    parallelism = {parallelism}
    job.mode = "{job_mode}"
}}

source{{
    Jdbc {{
        url = "jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}?serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true&useSSL=false&allowPublicKeyRetrieval=true"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "{mysql_user}"
        password = "{mysql_password}"
        query = "select {query_columns} from {mysql_db}.{source_table};"
    }}
}}

transform {{

}}

sink {{
       Hive {{
               table_name = "{hive_table}"
               metastore_uri = "{metastore_uri}"
               hive.hadoop.conf-path = "/etc/hadoop/conf"
               save_mode = "overwrite"
               partition_by = ["ds"]
               dynamic_partition = true
               orc_compress = "SNAPPY"
               tbl_properties = {{
                   "external.table.purge" = "true"
               }}
               fields = [
                   {', '.join([f'"{col}"' for col in source_columns])},
                   "ds"
               ]
           }}
}}
"""

    # 写入配置文件
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(config_content)
    print(f"配置文件已生成: {output_path}")

def generate_configs_for_tables(mysql_config, hive_metastore_uri, output_dir="./seatunnel_configs",
                                specific_tables=None, hive_db_prefix="bigdata_realtime_v1"):
    """为指定的表生成配置文件"""
    # 创建输出目录（如果不存在）
    os.makedirs(output_dir, exist_ok=True)

    # 获取指定表的信息
    tables_info = get_mysql_tables_and_columns(
        **mysql_config,
        specific_tables=specific_tables
    )

    if not tables_info:
        print("未获取到任何表信息，无法生成配置文件")
        return

    # 为每个表生成配置文件
    for table_info in tables_info:
        table_name = table_info['table_name']
        columns = table_info['columns']

        # 构建输出文件完整路径
        filename = f"sync_mysql_to_hive_{table_name}.conf"
        output_path = os.path.join(output_dir, filename)

        hive_table = f"{hive_db_prefix}.ods_{table_name}"

        # 生成配置文件
        generate_seatunnel_config(
            output_path=output_path,
            mysql_host=mysql_config['host'],
            mysql_port=mysql_config['port'],
            mysql_db=mysql_config['database'],
            mysql_user=mysql_config['user'],
            mysql_password=mysql_config['password'],
            hive_table=hive_table,
            source_table=table_name,
            source_columns=columns,
            metastore_uri=hive_metastore_uri
        )

    print(f"\n生成完成，共生成 {len(tables_info)} 个配置文件，保存目录：{os.path.abspath(output_dir)}")

if __name__ == "__main__":
    # 数据库连接信息
    mysql_config = {
        "host": "cdh03",
        "port": 3306,
        "database": "realtime_v1",# mysql 数据库名
        "user": "root",
        "password": "root"
    }

    # Hive metastore地址
    hive_metastore_uri = "thrift://cdh03:9083"

    # 自定义配置文件保存目录

    custom_output_dir = "./conf"   # 自定义配置文件保存目录

    # 方式1：指定特定表
    # tables_to_process = ["base_category1", "base_category2", "user_info"]

    # 方式2：处理所有表
    tables_to_process = ["activity_info", "activity_rule", "base_category1", "base_category2", "base_category3"
        , "base_dic", "base_province", "base_region", "base_trademark", "cart_info", "coupon_info"
        , "sku_attr_value", "sku_info", "sku_sale_attr_value", "spu_info", "promotion_pos", "promotion_refer"
        , "cart_info", "comment_info", "coupon_use", "favor_info", "order_detail", "order_detail_activity", "order_detail_coupon"
        , "order_info", "order_refund_info", "order_status_log", "payment_info", "refund_payment", "user_info"]

    # 生成配置文件
    generate_configs_for_tables(
        mysql_config=mysql_config,
        hive_metastore_uri=hive_metastore_uri,
        output_dir=custom_output_dir,  # 指定保存目录
        specific_tables=tables_to_process,
        hive_db_prefix="bigdata_realtime_v1"
    )
