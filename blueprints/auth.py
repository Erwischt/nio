import sqlite3
import os
from flask import Blueprint, request, jsonify, session, redirect, url_for, render_template
from werkzeug.security import check_password_hash


auth_bp = Blueprint('auth', __name__)

# 获取数据库路径（通常建议在 app.py 中统一定义，这里通过相对路径获取）
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
USERS_DB_FILE = os.path.join(BASE_DIR, 'db', 'core', 'users.db')

@auth_bp.route('/login', methods=['GET'])
def login_page():
    if 'uid' in session:
        return redirect(url_for('index'))
    return render_template('login.html')

@auth_bp.route('/api/login', methods=['POST'])
def api_login():
    data = request.json
    username = data.get('username', '').strip()
    password = data.get('password', '').strip()

    conn = sqlite3.connect(USERS_DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username=?", (username,))
    user = cursor.fetchone()
    conn.close()

    if user and check_password_hash(user['password'], password):
        session['uid'] = user['uid']
        session['username'] = user['username']
        session['real_name'] = user['real_name']
        session['role'] = user['role']
        return jsonify({"status": "success", "message": "登录成功"})
    else:
        return jsonify({"status": "error", "message": "用户名或密码错误"}), 401

@auth_bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))