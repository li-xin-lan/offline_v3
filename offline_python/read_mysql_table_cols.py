import mysql.connector
from mysql.connector import Error

def get_mysql_tables_and_columns(host, port, database, user, password, specific_tables=None):
    """
    获取MySQL数据库中表名及其字段名，支持指定特定表

    参数:
        specific_tables: 可选，指定要处理的表名列表，如["table1", "table2"]
                         若为None，则处理所有表

    返回:
        包含表名和字段名的列表，格式:
        [
            {'table_name': '表名1', 'columns': ['字段1', '字段2', ...]},
            {'table_name': '表名2', 'columns': ['字段1', '字段2', ...]}
        ]
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
                print(f"已提取表: {table}，字段数: {len(column_names)}")

    except Error as e:
        print(f"数据库连接错误: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

    return result



