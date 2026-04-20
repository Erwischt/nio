import sqlite3
import os
import time
import re
import pandas as pd
from contextlib import closing
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
    if not grade or not re.match(r'^\d{4}$', str(grade)):
        raise ValueError("非法的年级参数")
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

@scores_bp.route('/dynamic_student_mapping_hub')
@login_required
def render_dynamic_student_mapping_hub():
    """学生教学班快照核对页面（流水线第三步）"""
    exam_id = request.args.get('exam_id')
    grade = request.args.get('grade')
    if not exam_id or not grade:
        return "缺少关键参数 (exam_id 或 grade)，请从流水线上一步正常跳转。", 400
    return render_template('scoresManagement/dynamic_student_mapping_hub.html', exam_id=exam_id, grade=grade)


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
        with closing(get_scores_db_connection(grade)) as conn:
            cursor = conn.cursor()
            query = "SELECT * FROM exam_metadata"
            params = []
            if semester != '全部':
                query += " WHERE semester = ?"
                params.append(semester)
            query += " ORDER BY exam_date DESC"
            cursor.execute(query, params)
            exams = [dict(row) for row in cursor.fetchall()]
        return jsonify({"status": "success", "data": exams})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@scores_bp.route('/api/upload_scores', methods=['POST'])
@login_required
def upload_scores():
    """
    【优化版】接收成绩 Excel 并落盘
    通过内存预加载技术消除循环 SQL 查询，支持数千人规模的秒级导入
    """
    current_editor = session.get('real_name', '未知操作者')

    try:
        # 1. 接收基础表单数据
        exam_name = request.form.get('exam_name')
        grade = request.form.get('grade')
        semester = request.form.get('semester')
        exam_date = request.form.get('exam_date')
        exam_type = request.form.get('exam_type')

        if not grade or not semester or not exam_name:
            return jsonify({'success': False, 'message': '表单参数缺失'})

        exam_id = f"EX_{grade}_{int(time.time())}"

        # 2. 接收并验证 Excel 文件
        file = request.files.get('score_file')
        if not file:
            return jsonify({'success': False, 'message': '未找到上传文件'})

        try:
            # 强制读取 Sheet2
            df = pd.read_excel(file, sheet_name=1)
        except Exception as e:
            return jsonify({'success': False, 'message': f'Excel 解析错误: {str(e)}'})

        # 3. 建立数据库连接并执行自动事务管理
        with closing(get_scores_db_connection(grade)) as conn:
            with conn:
                cursor = conn.cursor()

                # ================= 核心优化：内存预加载 =================
                cursor.execute(
                    "SELECT custom_id, name FROM stable_student_course_mapping WHERE semester = ?",
                    (semester,)
                )
                mapping_rows = cursor.fetchall()

                # 构造内存字典
                id_map = {str(row['custom_id']).strip(): row['custom_id'] for row in mapping_rows}
                name_map = {}
                duplicate_names = set()

                for row in mapping_rows:
                    name = str(row['name']).strip()
                    if name in name_map:
                        duplicate_names.add(name)
                    name_map[name] = row['custom_id']
                # ======================================================

                # 4. 考务元数据落盘
                cursor.execute('''
                    INSERT INTO exam_metadata (exam_id, exam_name, exam_date, semester, exam_type, creator, is_analyzed)
                    VALUES (?, ?, ?, ?, ?, ?, 0)
                ''', (exam_id, exam_name, exam_date, semester, exam_type, current_editor))

                # 5. 内存匹配与成绩清洗
                subject_columns = ['语文', '数学', '英语', '物理', '化学', '生物', '政治', '历史', '地理']
                raw_scores_data = []

                for index, row in df.iterrows():
                    internal_id = str(row.get('内部编号', '')).strip()
                    student_name = str(row.get('姓名', '')).strip()

                    if not student_name and internal_id in ('', 'nan', 'None'):
                        continue

                    # 优先使用内部编号匹配
                    custom_id = None
                    if internal_id and internal_id in id_map:
                        custom_id = id_map[internal_id]
                    # 编号匹配失败，尝试姓名匹配
                    elif student_name in name_map:
                        if student_name in duplicate_names:
                            # 抛出异常触发 with conn 的自动 rollback
                            raise ValueError(f'导入中断：检测到重名学生【{student_name}】，必须填写内部编号！')
                        custom_id = name_map[student_name]

                    if not custom_id:
                        raise ValueError(f'第{index + 2}行匹配失败：该生不在【{semester}】教学班名单中。')

                    # 分数清洗
                    scores = []
                    for subj in subject_columns:
                        val = row.get(subj, None)
                        if pd.isna(val) or str(val).strip() == '':
                            scores.append(None)
                        else:
                            try:
                                scores.append(float(val))
                            except:
                                scores.append(None)

                    raw_scores_data.append([exam_id, custom_id] + scores)

                # 批量插入（保持高效）
                cursor.executemany('''
                    INSERT INTO students_raw_scores 
                    (exam_id, custom_id, chinese, math, english, physics, chemistry, biology, politics, history, geography)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', raw_scores_data)

                # 6. 克隆师资快照
                cursor.execute('''
                    INSERT INTO dynamic_class_mapping 
                    (exam_id, campus, class_type, class_name, subject, teacher_name, teacher_uid)
                    SELECT ?, campus, class_type, class_name, subject, teacher_name, teacher_uid
                    FROM stable_class_mapping
                    WHERE semester = ?
                ''', (exam_id, semester))

                # 7. 克隆学生走班快照
                cursor.execute('''
                            INSERT INTO dynamic_student_course_mapping 
                            (exam_id, custom_id, name, subject_track, 
                             chinese_class, math_class, english_class, 
                             physics_class, chemistry_class, biology_class, 
                             politics_class, history_class, geography_class)
                            SELECT ?, custom_id, name, subject_track, 
                                   chinese_class, math_class, english_class, 
                                   physics_class, chemistry_class, biology_class, 
                                   politics_class, history_class, geography_class
                            FROM stable_student_course_mapping
                            WHERE semester = ?
                        ''', (exam_id, semester))

        return jsonify({'success': True, 'message': '导入成功', 'exam_id': exam_id})

    except ValueError as ve:
        # 捕获业务逻辑主动抛出的验证异常并返回给前端
        return jsonify({'success': False, 'message': str(ve)})
    except Exception as e:
        return jsonify({'success': False, 'message': f'服务器错误: {str(e)}'})

# ================= API 接口：师资配置与碰对 =================

@scores_bp.route('/api/teacher_mapping', methods=['GET'])
@login_required
def get_teacher_mapping():
    """【修复版】获取考试快照并与用户库碰对，同时自动将真实 UID 刷入数据库"""
    exam_id = request.args.get('exam_id')
    grade = request.args.get('grade')
    try:
        # 1. 提取所有系统用户
        with closing(sqlite3.connect(USERS_DB_FILE)) as users_conn:
            users_dict = {row[0]: row[1] for row in users_conn.execute("SELECT real_name, uid FROM users").fetchall()}

        with closing(get_scores_db_connection(grade)) as conn:
            with conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM dynamic_class_mapping WHERE exam_id = ?", (exam_id,))
                rows = [dict(r) for r in cursor.fetchall()]

                update_data = []
                for r in rows:
                    teacher_name = r['teacher_name']
                    is_matched = teacher_name in users_dict
                    real_uid = users_dict.get(teacher_name, 0)

                    r['matched'] = is_matched
                    r['teacher_uid'] = real_uid

                    # 核心修复：如果找到了真实的账号 UID，准备写入数据库更新它
                    if is_matched:
                        update_data.append((real_uid, r['id']))

                # 批量执行数据库的 UID 修复
                if update_data:
                    cursor.executemany("UPDATE dynamic_class_mapping SET teacher_uid = ? WHERE id = ?", update_data)

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
        with closing(get_scores_db_connection(grade)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM stable_class_mapping WHERE semester = ?", (semester,))
            data = [dict(r) for r in cursor.fetchall()]
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
        with closing(get_scores_db_connection(grade)) as conn:
            with conn:
                cursor = conn.cursor()
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
        with closing(get_scores_db_connection(grade)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM stable_student_course_mapping WHERE semester=?", (semester,))
            rows = [dict(row) for row in cursor.fetchall()]
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
        with closing(get_scores_db_connection(grade)) as conn:
            with conn:
                cursor = conn.cursor()
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
        return jsonify({"status": "success", "update_count": len(insert_data)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@scores_bp.route('/api/student_course_mapping/update_single', methods=['PUT'])
@login_required
def update_single_student_mapping():
    """行内闪电编辑：单独更新某学生某一科的教学班"""
    data = request.json
    grade = data.get('grade')
    semester = data.get('semester')
    custom_id = data.get('custom_id')
    field = data.get('field')
    value = data.get('value')

    # 1. 严格的安全白名单校验（防止 SQL 注入）
    allowed_fields = {'chinese_class', 'math_class', 'english_class',
                      'physics_class', 'chemistry_class', 'biology_class',
                      'politics_class', 'history_class', 'geography_class'}
    if field not in allowed_fields:
        return jsonify({"status": "error", "message": "非法的修改字段"})

    try:
        with closing(get_scores_db_connection(grade)) as conn:
            with conn:
                cursor = conn.cursor()
                # 2. 执行精准更新
                cursor.execute(f'''
                    UPDATE stable_student_course_mapping
                    SET {field} = ?
                    WHERE custom_id = ? AND semester = ?
                ''', (value, custom_id, semester))

        return jsonify({"status": "success", "message": "更新成功"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


# ================= API 接口：学生走班快照 (Dynamic Student Mapping) =================

@scores_bp.route('/api/dynamic_student_mapping', methods=['GET'])
@login_required
def get_dynamic_student_mapping():
    """拉取本次考试的学生走班快照数据 (第三步页面渲染使用)"""
    exam_id = request.args.get('exam_id')
    grade = request.args.get('grade')

    if not exam_id or not grade:
        return jsonify({"status": "error", "message": "缺少关键参数 exam_id 或 grade"})

    try:
        with closing(get_scores_db_connection(grade)) as conn:
            cursor = conn.cursor()
            # 精准查询本次考试克隆下来的所有学生状态
            cursor.execute("SELECT * FROM dynamic_student_course_mapping WHERE exam_id = ?", (exam_id,))
            rows = [dict(row) for row in cursor.fetchall()]

        return jsonify({"status": "success", "data": rows})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})


@scores_bp.route('/api/dynamic_student_mapping/update_single', methods=['PUT'])
@login_required
def update_single_dynamic_student_mapping():
    """行内闪电编辑：微调本次考试快照中某学生某一科的教学班"""
    data = request.json
    grade = data.get('grade')
    exam_id = data.get('exam_id')
    custom_id = data.get('custom_id')
    field = data.get('field')
    value = data.get('value')

    if not all([grade, exam_id, custom_id, field]):
        return jsonify({"status": "error", "message": "请求参数不完整"})

    # 1. 极其严格的白名单安全校验（防御 SQL 注入）
    allowed_fields = {
        'chinese_class', 'math_class', 'english_class',
        'physics_class', 'chemistry_class', 'biology_class',
        'politics_class', 'history_class', 'geography_class'
    }
    if field not in allowed_fields:
        return jsonify({"status": "error", "message": "非法的修改字段"})

    try:
        with closing(get_scores_db_connection(grade)) as conn:
            with conn:
                cursor = conn.cursor()
                # 2. 执行精准更新：务必使用联合主键 (exam_id AND custom_id) 锁定唯一记录
                cursor.execute(f'''
                    UPDATE dynamic_student_course_mapping
                    SET {field} = ?
                    WHERE exam_id = ? AND custom_id = ?
                ''', (value, exam_id, custom_id))

        return jsonify({"status": "success", "message": "快照更新成功"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

@scores_bp.route('/api/teacher_mapping/update_single', methods=['PUT'])
@login_required
def update_single_teacher_mapping():
    """【新增】行内编辑：手动修正某条快照的任课教师名称并重新碰对"""
    data = request.json
    grade = data.get('grade')
    mapping_id = data.get('id')
    new_teacher_name = data.get('teacher_name')

    if not mapping_id or not new_teacher_name:
        return jsonify({"status": "error", "message": "参数缺失"})

    try:
        # 1. 去用户库查询新名字是否对应了真实的 UID
        with closing(sqlite3.connect(USERS_DB_FILE)) as users_conn:
            cursor_u = users_conn.cursor()
            cursor_u.execute("SELECT uid FROM users WHERE real_name = ?", (new_teacher_name,))
            user = cursor_u.fetchone()

        new_uid = user[0] if user else 0

        # 2. 更新师资快照表
        with closing(get_scores_db_connection(grade)) as conn:
            with conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE dynamic_class_mapping
                    SET teacher_name = ?, teacher_uid = ?
                    WHERE id = ?
                ''', (new_teacher_name, new_uid, mapping_id))

        return jsonify({
            "status": "success",
            "teacher_uid": new_uid,
            "matched": bool(new_uid),
            "message": "更新成功"
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})