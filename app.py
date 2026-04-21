import os
from dotenv import load_dotenv
from flask import Flask, render_template, session
from blueprints.auth import auth_bp
from blueprints.students import students_bp
from blueprints.scores import scores_bp
from blueprints.studentAward import studentAward_bp  # 新增导入

load_dotenv()

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY')
if not app.secret_key:
    raise ValueError("致命错误：未配置 SECRET_KEY 环境变量，系统无法启动！")

app.register_blueprint(auth_bp)
app.register_blueprint(students_bp, url_prefix='/students')
app.register_blueprint(scores_bp, url_prefix='/scoresManagement')
app.register_blueprint(studentAward_bp, url_prefix='/studentAwardManagement')  # 新增注册


@app.route('/')
def index():
    # 动态扫描 banner 图片库
    banner_images = []
    banner_dir = os.path.join(app.root_path, 'static', 'images', 'banner')
    if os.path.exists(banner_dir):
        for f in os.listdir(banner_dir):
            if f.lower().endswith(('.jpg', '.jpeg', '.png')):
                banner_images.append(f)

    return render_template('index.html', banner_images=banner_images)


if __name__ == '__main__':
    print("🚀 校园数据平台（模块化版）启动...")
    app.run(host='0.0.0.0', port=5000, debug=False)