import sqlite3
import os
import shutil
from werkzeug.security import generate_password_hash

# ================= 动态路径解析 =================
# __file__ 是当前脚本路径。向上退两级回到项目根目录 (Project_Root)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 定义核心数据库存放的具体目录：Project_Root/db/core
CORE_DB_DIR = os.path.join(BASE_DIR, 'db', 'core')

# 拼接出三大核心数据库的绝对路径
MAIN_DB_FILE = os.path.join(CORE_DB_DIR, 'students_info.db')
LOG_DB_FILE = os.path.join(CORE_DB_DIR, 'students_info_change_log.db')
USERS_DB_FILE = os.path.join(CORE_DB_DIR, 'users.db')


def setup_databases():
    """初始化/迁移三库架构：业务主库、审计日志库、用户凭证库"""
    print("开始检查并初始化校园数据平台核心数据库系统...\n")

    # 核心：确保目标文件夹存在
    os.makedirs(CORE_DB_DIR, exist_ok=True)

    # ================= 1. 初始化/迁移主业务数据库 (students_info.db) =================
    # 智能查找根目录下的旧文件并安全迁移
    old_school_data = os.path.join(BASE_DIR, 'school_data.db')
    old_students_info = os.path.join(BASE_DIR, 'students_info.db')

    conn_main = sqlite3.connect(MAIN_DB_FILE)
    cursor_main = conn_main.cursor()

    # 1.1 学生档案底表
    cursor_main.execute('''
        CREATE TABLE IF NOT EXISTS students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            custom_id CHAR(10) NOT NULL UNIQUE,
            school_id CHAR(19),
            national_id CHAR(18),
            name VARCHAR(10) NOT NULL,
            former_name VARCHAR(10),
            sex CHAR(2),
            enter_year CHAR(4) NOT NULL,
            campus VARCHAR(10) NOT NULL,
            current_class VARCHAR(5) NOT NULL,
            subject VARCHAR(10) NOT NULL,
            language_type VARCHAR(8) NOT NULL DEFAULT '英语',
            category VARCHAR(10) NOT NULL,
            major VARCHAR(10),
            at_school VARCHAR(10) NOT NULL,
            remarks TEXT,
            boarding_status VARCHAR(5) NOT NULL,
            apartment VARCHAR(8),
            dormitory VARCHAR(8),
            last_edit_at VARCHAR(19)
        )
    ''')

    # 1.2 学生奖惩管理表 (新增)
    cursor_main.execute('''
        CREATE TABLE IF NOT EXISTS student_award (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            custom_id CHAR(10) NOT NULL,
            award_date VARCHAR(10) NOT NULL,
            award_category VARCHAR(10) NOT NULL,
            award_level VARCHAR(20),
            award_name TEXT,
            description TEXT,
            issuing_authority VARCHAR(100),
            created_at VARCHAR(19),
            editor VARCHAR(50)
        )
    ''')

    # 建立 custom_id 索引提升查询性能
    cursor_main.execute('''
        CREATE INDEX IF NOT EXISTS idx_student_award_custom_id 
        ON student_award (custom_id)
    ''')

    conn_main.commit()
    conn_main.close()
    print(f"✅ 主业务数据库就绪: [{MAIN_DB_FILE}]")

    # ================= 2. 初始化/迁移审计日志库 (students_info_change_log.db) =================
    old_log_db = os.path.join(BASE_DIR, 'students_info_change_log.db')
    if not os.path.exists(LOG_DB_FILE) and os.path.exists(old_log_db):
        print(f"⚠️ 检测到根目录存在旧版审计日志库，正在自动迁移至 {CORE_DB_DIR}...")
        shutil.move(old_log_db, LOG_DB_FILE)

    conn_log = sqlite3.connect(LOG_DB_FILE)
    cursor_log = conn_log.cursor()

    # 2.1 学生档案变更日志表
    cursor_log.execute('''
            CREATE TABLE IF NOT EXISTS change_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                custom_id CHAR(10) NOT NULL,
                action_type VARCHAR(20) NOT NULL,
                details TEXT,
                timestamp VARCHAR(19) NOT NULL,
                editor VARCHAR(50) NOT NULL
            )
        ''')

    # 2.2 学生奖惩变更日志表 (新增)
    cursor_log.execute('''
            CREATE TABLE IF NOT EXISTS award_change_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                award_id INTEGER NOT NULL,
                custom_id CHAR(10) NOT NULL,
                action_type VARCHAR(20) NOT NULL,
                details TEXT,
                timestamp VARCHAR(19) NOT NULL,
                editor VARCHAR(20) NOT NULL
            )
        ''')

    conn_log.commit()
    conn_log.close()
    print(f"✅ 审计日志数据库就绪: [{LOG_DB_FILE}]")

    # ================= 3. 初始化/迁移用户凭证库 (users.db) =================
    old_users_db = os.path.join(BASE_DIR, 'users.db')
    if not os.path.exists(USERS_DB_FILE) and os.path.exists(old_users_db):
        print(f"⚠️ 检测到根目录存在旧版用户库，正在自动迁移至 {CORE_DB_DIR}...")
        shutil.move(old_users_db, USERS_DB_FILE)

    conn_users = sqlite3.connect(USERS_DB_FILE)
    cursor_users = conn_users.cursor()

    cursor_users.execute('''
        CREATE TABLE IF NOT EXISTS users (
            uid INTEGER PRIMARY KEY AUTOINCREMENT,
            username VARCHAR(50) UNIQUE NOT NULL,
            password VARCHAR(255) NOT NULL,
            real_name VARCHAR(50) NOT NULL,
            role VARCHAR(20) DEFAULT 'teacher',
            national_id CHAR(18) UNIQUE,
            department VARCHAR(50),
            telephone_number VARCHAR(20)
        )
    ''')

    # --- 冷启动机制：检查并预置超级管理员 ---
    cursor_users.execute("SELECT COUNT(*) FROM users")
    if cursor_users.fetchone()[0] == 0:
        print("💡 检测到 users 表为空，正在创建默认超级管理员账号...")
        default_hashed_password = generate_password_hash('admin123')

        cursor_users.execute('''
            INSERT INTO users (username, password, real_name, role, department)
            VALUES (?, ?, ?, ?, ?)
        ''', ('admin', default_hashed_password, '系统管理员', 'admin', '信息技术中心'))

        print("✅ 默认管理员账号创建成功！")
        print("   👉 登录账号: admin")
        print("   👉 初始密码: admin123")

    conn_users.commit()
    conn_users.close()
    print(f"✅ 用户凭证数据库就绪: [{USERS_DB_FILE}]")

    print("\n🎉 核心数据库系统全面初始化与迁移完成！")


if __name__ == '__main__':
    setup_databases()