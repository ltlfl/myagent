from utils.database import DatabaseManager
def main():
    db_manager = DatabaseManager()
    # 测试连接
    with db_manager.get_connection() as conn:
        if conn:
            print("数据库连接成功")
        else:
            print("数据库连接失败")
