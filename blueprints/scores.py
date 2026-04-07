import sqlite3
import os
import time
import pandas as pd
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from utils.decorators import login_required

# 定义蓝图
scores_bp = Blueprint('scores', __name__)

# ================= 动态路径解析 =================
# 定位到项目根目录下的数据库目录
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CORE_DB_DIR = os.path.join(BASE_DIR, 'db', 'core')
SCORES_DB_DIR = os.path.join(BASE_DIR, 'db', 'scores')
USERS_DB_FILE = os.path.join(CORE_DB_DIR, 'users.db')


# ================= 辅助函数 =================

def get_scores_db_connection(grade):
    """动态路由：根据年级连接到对应的成绩物理分库"""
    db_path = os.path.join(SCORES_DB_DIR, f'scores_{grade}.db')
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"未找到 {grade} 级的分库文件，请先初始化！")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ================= 页面路由视图 =================

@scores_bp.route('/')
@login_required
def index():
    """成绩分析大厅主页"""
    return render_template('scoresManagement/index.html')


@scores_bp.route('/import')
@login_required
def render_import_score_page():
    """新建考试与成绩导入界面"""
    return render_template('scoresManagement/import_score.html')


@scores_bp.route('/teacher_mapping_config')
@login_required
def render_teacher_mapping_config():
    """师资快照配置页面（流水线第二步）"""
    exam_id = request.args.get('exam_id')
    grade = request.args.get('grade')
    if not exam_id or not grade:
        return "缺少关键参数 (exam_id 或 grade)，请从成绩导入页正常跳转。", 400
    return render_template('scoresManagement/teacher_mapping_config.html', exam_id=exam_id, grade=grade)


@scores_bp.route('/stable_mapping_hub')
@login_required
def render_stable_mapping_hub():
    """师资底表（排课表）管理大厅"""
    return render_template('scoresManagement/stable_mapping_hub.html')


@scores_bp.route('/stable_student_mapping_hub')
@login_required
def scores_stable_student_mapping_hub():
    """学生教学班配置大厅"""
    return render_template('scoresManagement/stable_student_mapping_hub.html')


# ================= API 接口：考试与成绩导入 =================

@scores_bp.route('/api/list', methods=['GET'])
@login_required
def list_exams():
    """拉取指定年级的考试列表"""
    grade = request.args.get('grade_year')
    semester = request.args.get('semester', '全部')
    if not grade:
        return jsonify({"status": "error", "message": "未指定年级"})

    try:
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()
        query = "SELECT * FROM exam_metadata"
        params = []
        if semester != '全部':
            query += " WHERE semester = ?"
            params.append(semester)
        query += " ORDER BY exam_date DESC"
        cursor.execute(query, params)
        exams = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify({"status": "success", "data": exams})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@scores_bp.route('/api/upload_scores', methods=['POST'])
@login_required
def upload_scores():
    """成绩落盘并生成师资快照"""
    current_editor = session.get('real_name', '未知操作者')
    try:
        exam_name = request.form.get('exam_name')
        grade = request.form.get('grade')
        semester = request.form.get('semester')
        exam_date = request.form.get('exam_date')
        exam_type = request.form.get('exam_type')

        exam_id = f"EX_{grade}_{int(time.time())}"
        file = request.files.get('score_file')

        df = pd.read_excel(file, sheet_name=1)  # 强制读取Sheet2
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION;")

        # 1. 考务元数据
        cursor.execute('''
            INSERT INTO exam_metadata (exam_id, exam_name, exam_date, semester, exam_type, creator, is_analyzed)
            VALUES (?, ?, ?, ?, ?, ?, 0)
        ''', (exam_id, exam_name, exam_date, semester, exam_type, current_editor))

        # 2. 成绩解析（简化的循环逻辑，需匹配分库表结构）
        subject_columns = ['语文', '数学', '英语', '物理', '化学', '生物', '政治', '历史', '地理']
        raw_scores_data = []
        for _, row in df.iterrows():
            internal_id = str(row.get('内部编号', '')).strip()
            student_name = str(row.get('姓名', '')).strip()
            if not student_name and internal_id in ('', 'nan', 'None'): continue

            # 身份匹配逻辑
            cursor.execute(
                "SELECT custom_id FROM stable_student_course_mapping WHERE (custom_id = ? OR name = ?) AND semester = ?",
                (internal_id, student_name, semester))
            res = cursor.fetchone()
            if not res: continue

            scores = [row.get(subj) if not pd.isna(row.get(subj)) else None for subj in subject_columns]
            raw_scores_data.append([exam_id, res['custom_id']] + scores)

        cursor.executemany('''
            INSERT INTO students_raw_scores (exam_id, custom_id, chinese, math, english, physics, chemistry, biology, politics, history, geography)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', raw_scores_data)

        # 3. 师资快照克隆
        cursor.execute('''
            INSERT INTO dynamic_class_mapping (exam_id, campus, class_type, class_name, subject, teacher_name, teacher_uid)
            SELECT ?, campus, class_type, class_name, subject, teacher_name, teacher_uid FROM stable_class_mapping WHERE semester = ?
        ''', (exam_id, semester))

        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': '导入成功', 'exam_id': exam_id})
    except Exception as e:
        if 'conn' in locals(): conn.rollback()
        return jsonify({'success': False, 'message': str(e)})


# ================= API 接口：师资配置与碰对 =================

@scores_bp.route('/api/teacher_mapping', methods=['GET'])
@login_required
def get_teacher_mapping():
    """获取考试快照并与用户库碰对"""
    exam_id = request.args.get('exam_id')
    grade = request.args.get('grade')
    try:
        users_conn = sqlite3.connect(USERS_DB_FILE)
        users_dict = {row[0]: row[1] for row in users_conn.execute("SELECT real_name, uid FROM users").fetchall()}
        users_conn.close()

        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dynamic_class_mapping WHERE exam_id = ?", (exam_id,))
        rows = [dict(r) for r in cursor.fetchall()]

        for r in rows:
            r['matched'] = r['teacher_name'] in users_dict
            r['teacher_uid'] = users_dict.get(r['teacher_name'], 0)

        conn.close()
        return jsonify({'success': True, 'data': rows})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@scores_bp.route('/api/stable_mapping', methods=['GET'])
@login_required
def get_stable_mapping():
    """拉取师资底表数据"""
    grade = request.args.get('grade')
    semester = request.args.get('semester')
    try:
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM stable_class_mapping WHERE semester = ?", (semester,))
        data = [dict(r) for r in cursor.fetchall()]
        conn.close()
        return jsonify({'success': True, 'data': data})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@scores_bp.route('/api/upload_stable_mapping', methods=['POST'])
@login_required
def upload_stable_mapping():
    """覆盖上传师资底表"""
    grade = request.form.get('grade')
    semester = request.form.get('semester')
    file = request.files.get('file')
    try:
        df = pd.read_excel(file, sheet_name=1)
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION;")
        cursor.execute("DELETE FROM stable_class_mapping WHERE semester = ?", (semester,))

        insert_data = []
        for _, row in df.iterrows():
            if pd.isna(row.get('教师姓名')): continue
            insert_data.append((row.get('校区'), row.get('班级类型'), row.get('班级名称'), row.get('学科'),
                                row.get('教师姓名'), 0, semester))

        cursor.executemany('''
            INSERT INTO stable_class_mapping (campus, class_type, class_name, subject, teacher_name, teacher_uid, semester)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', insert_data)
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': '排课表覆盖成功'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


# ================= API 接口：学生教学班矩阵 =================

@scores_bp.route('/api/student_course_mapping', methods=['GET'])
@login_required
def get_student_course_mapping():
    """拉取九科全景矩阵"""
    grade = request.args.get('grade')
    semester = request.args.get('semester')
    try:
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM stable_student_course_mapping WHERE semester=?", (semester,))
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return jsonify({"status": "success", "data": rows})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@scores_bp.route('/api/student_course_mapping/upload', methods=['POST'])
@login_required
def upload_student_course_mapping():
    """批量覆写教学班矩阵"""
    grade = request.form.get('grade')
    semester = request.form.get('semester')
    file = request.files.get('file')
    try:
        df = pd.read_excel(file, sheet_name=1).fillna('')
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()
        cursor.execute("BEGIN TRANSACTION;")
        cursor.execute("DELETE FROM stable_student_course_mapping WHERE semester=?", (semester,))

        insert_data = []
        for _, row in df.iterrows():
            cid = str(row.get('内部编号')).strip()
            if not cid: continue
            insert_data.append((cid, row.get('学生姓名'), row.get('选科'), row.get('语文教学班'), row.get('数学教学班'),
                                row.get('英语教学班'), row.get('物理教学班'), row.get('化学教学班'),
                                row.get('生物教学班'),
                                row.get('政治教学班'), row.get('历史教学班'), row.get('地理教学班'), semester))

        cursor.executemany('''
            INSERT INTO stable_student_course_mapping VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', insert_data)
        conn.commit()
        conn.close()
        return jsonify({"status": "success", "update_count": len(insert_data)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})