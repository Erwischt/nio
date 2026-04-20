import os
from flask import Flask, render_template, session
from blueprints.auth import auth_bp
from blueprints.students import students_bp
from blueprints.scores import scores_bp
from dotenv import load_dotenv # 新增
from flask import Flask, render_template, session

load_dotenv() # 新增：自动从 .env 读取并注入环境变量

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY')
if not app.secret_key:
    raise ValueError("致命错误：未配置 SECRET_KEY 环境变量，系统无法启动！")

app.register_blueprint(auth_bp)
app.register_blueprint(students_bp, url_prefix='/students')
app.register_blueprint(scores_bp, url_prefix='/scoresManagement')

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    print("🚀 校园数据平台（模块化版）启动...")
    app.run(host='0.0.0.0', port=5000, debug=False)