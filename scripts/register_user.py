import os
import sqlite3
from werkzeug.security import generate_password_hash


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 定义核心数据库存放的具体目录：Project_Root/db/core
CORE_DB_DIR = os.path.join(BASE_DIR, 'db', 'core')
USERS_DB_FILE = os.path.join(CORE_DB_DIR, 'users.db')

conn_users = sqlite3.connect(USERS_DB_FILE)
cursor_users = conn_users.cursor()

reg_queue = [
                ['admin2024','admin2024','2024级管理员','admin','教务处'],
                ['admin2025','admin2025','2025级管理员','admin','教务处'],
                ['admin2026','admin2026','2026级管理员','admin','教务处'],
                ['jiaowuchu','jiaowuchu','教务管理员','admin','教务处']
             ]

for account in reg_queue:
    username = account[0]
    hashed_password = generate_password_hash(account[1])
    real_name = account[2]
    role = account[3]
    department = account[4]
    cursor_users.execute('''
                INSERT INTO users (username, password, real_name, role, department)
                VALUES (?, ?, ?, ?, ?)
            ''', (username, hashed_password, real_name, role, department))
    print(username, "注册完毕，密码为:", account[1])
conn_users.commit()
cursor_users.close()