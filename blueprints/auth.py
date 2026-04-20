import sqlite3
import os
from contextlib import closing
from flask import Blueprint, request, jsonify, session, redirect, url_for, render_template
from werkzeug.security import check_password_hash


auth_bp = Blueprint('auth', __name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
USERS_DB_FILE = os.path.join(BASE_DIR, 'db', 'core', 'users.db')

@auth_bp.route('/login', methods=['GET'])
def login_page():
    if 'uid' in session:
        return redirect(url_for('index'))
    return render_template('login.html')

@auth_bp.route('/api/login', methods=['POST'])
def api_login():
    data = request.json or {}  # 防御 NoneType 崩溃
    username = data.get('username', '').strip()
    password = data.get('password', '').strip()

    if not username or not password:
        return jsonify({"status": "error", "message": "用户名或密码不能为空"}), 400

    try:
        # 使用 closing 确保 users.db 连接安全释放
        with closing(sqlite3.connect(USERS_DB_FILE)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE username=?", (username,))
            user = cursor.fetchone()

        if user and check_password_hash(user['password'], password):
            session['uid'] = user['uid']
            session['username'] = user['username']
            session['real_name'] = user['real_name']
            session['role'] = user['role']
            return jsonify({"status": "success", "message": "登录成功"})
        else:
            return jsonify({"status": "error", "message": "用户名或密码错误"}), 401
    except Exception as e:
        return jsonify({"status": "error", "message": "数据库服务异常"}), 500

@auth_bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))