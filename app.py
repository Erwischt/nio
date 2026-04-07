import os
from flask import Flask, render_template, session
from blueprints.auth import auth_bp
from blueprints.students import students_bp
from blueprints.scores import scores_bp

app = Flask(__name__)
app.secret_key = 'school_data_platform_super_secret_key'

# 1. 注册蓝图
# url_prefix 会自动为蓝图下的所有路由添加前缀
app.register_blueprint(auth_bp) # 不加前缀，保留 /login, /logout
app.register_blueprint(students_bp, url_prefix='/students') # 访问变更为 /students/
app.register_blueprint(scores_bp, url_prefix='/scoresManagement') # 访问变更为 /scoresManagement/

# 2. 保留根路径首页
@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    print("🚀 校园数据平台（模块化版）启动...")
    app.run(host='0.0.0.0', port=5000, debug=True)