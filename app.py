import math
import os
import sqlite3
import time
import json
from datetime import datetime
from functools import wraps

import pandas as pd
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_file
from numpy.ma.core import where
from werkzeug.security import check_password_hash
from utils.excel_exporter import generate_students_query_excel

app = Flask(__name__)
# 【重要安全配置】：Session 加密密钥
app.secret_key = 'school_data_platform_super_secret_key'

# ================= 动态路径与模块化配置 =================
# 获取当前 app.py 所在的绝对根目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 核心业务数据库目录 (管基础档案、账号、日志)
CORE_DB_DIR = os.path.join(BASE_DIR, 'db', 'core')
MAIN_DB_FILE = os.path.join(CORE_DB_DIR, 'students_info.db')
LOG_DB_FILE = os.path.join(CORE_DB_DIR, 'students_info_change_log.db')
USERS_DB_FILE = os.path.join(CORE_DB_DIR, 'users.db')

# 成绩分析分库目录 (管每次大考的分数和快照)
SCORES_DB_DIR = os.path.join(BASE_DIR, 'db', 'scores')


# ================= 0. 权限校验与拦截器 =================
def login_required(f):
    """登录拦截器：带智能跳转记录功能"""

    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'uid' not in session:
            if request.path.startswith('/api/'):
                return jsonify({"status": "error", "message": "未登录或会话已过期，请重新登录"}), 401
            return redirect(url_for('login_page', next=request.path))
        return f(*args, **kwargs)

    return decorated_function


# ================= 辅助函数：日志、对比与数据库连接 =================
def write_log(custom_id, action_type, details, editor):
    """将操作记录安全写入独立的日志数据库（用于学籍档案变更）"""
    if not details: return
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        conn = sqlite3.connect(LOG_DB_FILE)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO change_logs (custom_id, action_type, details, timestamp, editor)
            VALUES (?, ?, ?, ?, ?)
        ''', (custom_id, action_type, details, timestamp, editor))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"⚠️ 审计日志写入失败: {e}")


def get_diff(old_dict, new_dict):
    """对比新旧字典，生成精确到字段的修改详情"""
    field_labels = {
        'school_id': '省学籍辅号', 'national_id': '国家身份证号', 'name': '姓名',
        'former_name': '曾用名', 'sex': '性别', 'enter_year': '入学年份',
        'campus': '校区', 'current_class': '班级', 'subject': '选科',
        'category': '类别', 'major': '专业', 'at_school': '在校情况',
        'remarks': '特殊情况备注', 'boarding_status': '住宿情况',
        'apartment': '公寓楼', 'dormitory': '宿舍及床位'
    }
    diffs = []
    for key, label in field_labels.items():
        old_val = str(old_dict.get(key, '') or '').strip()
        new_val = str(new_dict.get(key, '') or '').strip()
        if old_val == 'None': old_val = ''
        if new_val == 'None': new_val = ''
        if old_val != new_val:
            diffs.append(f"[{label}] 由 '{old_val}' 改为 '{new_val}'")
    return " ; ".join(diffs)


def get_scores_db_connection(grade):
    """动态路由：根据年级连接到对应的成绩物理分库"""
    db_path = os.path.join(SCORES_DB_DIR, f'scores_{grade}.db')
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"未找到 {grade} 级的分库文件，请先初始化！")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ================= 1. 用户鉴权路由 =================
@app.route('/login', methods=['GET'])
def login_page():
    if 'uid' in session:
        return redirect(url_for('index'))
    return render_template('login.html')


@app.route('/api/login', methods=['POST'])
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


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))


# ================= 2. 页面路由视图 =================
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/students')
@login_required
def students_page():
    return render_template('studentsInfoManagement/index.html')

@app.route('/scoresManagement')
@login_required
def render_score_page():
    return render_template('scoresManagement/index.html')

# 🌟 新增：指向成绩导入界面的路由
@app.route('/scoresManagement/import')
@login_required
def render_import_score_page():
    """渲染新建考试与成绩导入界面"""
    return render_template('scoresManagement/import_score.html')


# ================= 3. API 接口：学生档案管理 =================
@app.route('/api/students/query', methods=['POST'])
@login_required
def query_students():
    """多模式学生信息查询接口"""
    data = request.json or {}
    mode = data.get('mode', 'simple')
    page = int(data.get('page', 1))
    limit = int(data.get('limit', 20))
    offset = (page - 1) * limit
    query_parts, params = [], []

    if mode == 'simple':
        keyword = data.get('keyword', '').strip()
        campus = data.get('campus', '').strip()
        enter_year = data.get('enter_year', '').strip()
        current_class = data.get('current_class', '').strip()

        if keyword:
            query_parts.append("(name LIKE ? OR custom_id LIKE ? OR school_id LIKE ? OR national_id LIKE ?)")
            params.extend([f"%{keyword}%"] * 4)
        if campus:
            query_parts.append("campus = ?")
            params.append(campus)
        if enter_year:
            query_parts.append("enter_year = ?")
            params.append(enter_year)
        if current_class:
            query_parts.append("current_class = ?")
            params.append(current_class)

        where_clause = " WHERE " + " AND ".join(query_parts) if query_parts else ""

    elif mode == 'advanced':
        filters = data.get('filters', [])
        allowed_fields = {'custom_id', 'school_id', 'national_id', 'name', 'sex', 'enter_year', 'campus',
                          'current_class', 'subject', 'category', 'major', 'at_school', 'boarding_status', 'remarks'}
        allowed_ops = {'=', '!=', '>', '<', '>=', '<=', 'LIKE'}
        for i, f in enumerate(filters):
            field = f.get('field')
            op = f.get('operator')
            val = str(f.get('value', '')).strip()
            logic = f.get('logic', 'AND').upper() if i > 0 else ''

            if field in allowed_fields and op in allowed_ops and val:
                params.append(f"%{val}%" if op == 'LIKE' else val)
                condition_str = f"{field} {op} ?"
                if i == 0:
                    query_parts.append(f"({condition_str})")
                else:
                    query_parts.append(f"{logic if logic in ('AND', 'OR') else 'AND'} ({condition_str})")

        where_clause = " WHERE " + " ".join(query_parts) if query_parts else ""
    else:
        where_clause = ""

    conn = sqlite3.connect(MAIN_DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM students {where_clause}", params)
    total_count = cursor.fetchone()[0]
    cursor.execute(f"SELECT * FROM students {where_clause} ORDER BY id DESC LIMIT ? OFFSET ?", params + [limit, offset])
    rows = cursor.fetchall()
    conn.close()

    total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
    return jsonify({"status": "success", "data": [dict(r) for r in rows], "total": total_count, "page": page,
                    "total_pages": total_pages})


@app.route('/api/students', methods=['POST'])
@login_required
def add_student():
    """新增单条学生记录"""
    data = request.json
    current_editor = session.get('real_name', '未知操作者')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    enter_year = data.get('enter_year', '')
    campus = data.get('campus', '')
    year_part = enter_year[-2:] if len(enter_year) == 4 else '00'
    campus_part = '1' if campus == '本部' else ('2' if campus == '分校' else '0')

    try:
        conn = sqlite3.connect(MAIN_DB_FILE)
        cursor = conn.cursor()

        cursor.execute("SELECT custom_id FROM students WHERE custom_id LIKE ? ORDER BY custom_id DESC LIMIT 1",
                       (f"{year_part}{campus_part}%",))
        last_id_record = cursor.fetchone()
        if last_id_record:
            last_seq = int(last_id_record[0][-4:])
            new_seq = last_seq + 1
        else:
            new_seq = 1
        custom_id = f"{year_part}{campus_part}{new_seq:04d}"

        cursor.execute('''
            INSERT INTO students (
                custom_id, school_id, national_id, name, former_name, sex, enter_year, campus, 
                current_class, subject, category, major, at_school, remarks, boarding_status, 
                apartment, dormitory, last_edit_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            custom_id, data.get('school_id'), data.get('national_id'), data.get('name'),
            data.get('former_name'), data.get('sex'), enter_year, campus,
            data.get('current_class'), data.get('subject'), data.get('category'),
            data.get('major'), data.get('at_school'), data.get('remarks'),
            data.get('boarding_status'), data.get('apartment'), data.get('dormitory'), current_time
        ))
        conn.commit()
        conn.close()

        write_log(custom_id, '新增档案', f"创建了学生档案: {data.get('name')}", current_editor)
        return jsonify({"status": "success", "message": "添加成功", "custom_id": custom_id})

    except sqlite3.IntegrityError as e:
        return jsonify({"status": "error", "message": "学号或身份证号已存在，请检查"}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/students/<int:student_id>', methods=['PUT'])
@login_required
def update_student(student_id):
    """更新学生记录"""
    data = request.json
    current_editor = session.get('real_name', '未知操作者')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        conn = sqlite3.connect(MAIN_DB_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM students WHERE id=?", (student_id,))
        old_record = dict(cursor.fetchone())

        cursor.execute('''
            UPDATE students SET 
                school_id=?, national_id=?, name=?, former_name=?, sex=?, enter_year=?, campus=?, 
                current_class=?, subject=?, category=?, major=?, at_school=?, remarks=?, 
                boarding_status=?, apartment=?, dormitory=?, last_edit_at=?
            WHERE id=?
        ''', (
            data.get('school_id'), data.get('national_id'), data.get('name'), data.get('former_name'),
            data.get('sex'), data.get('enter_year'), data.get('campus'), data.get('current_class'),
            data.get('subject'), data.get('category'), data.get('major'), data.get('at_school'),
            data.get('remarks'), data.get('boarding_status'), data.get('apartment'),
            data.get('dormitory'), current_time, student_id
        ))
        conn.commit()

        cursor.execute("SELECT * FROM students WHERE id=?", (student_id,))
        new_record = dict(cursor.fetchone())
        conn.close()

        diff_str = get_diff(old_record, new_record)
        if diff_str:
            write_log(old_record['custom_id'], '修改档案', diff_str, current_editor)

        return jsonify({"status": "success", "message": "更新成功"})
    except sqlite3.IntegrityError:
        return jsonify({"status": "error", "message": "唯一标识（如学号/身份证）冲突"}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/students/<int:student_id>', methods=['DELETE'])
@login_required
def delete_student(student_id):
    """删除学生记录"""
    current_editor = session.get('real_name', '未知操作者')
    try:
        conn = sqlite3.connect(MAIN_DB_FILE)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT custom_id, name FROM students WHERE id=?", (student_id,))
        record = cursor.fetchone()

        if record:
            custom_id = record['custom_id']
            name = record['name']
            cursor.execute("DELETE FROM students WHERE id=?", (student_id,))
            conn.commit()
            write_log(custom_id, '删除档案', f"删除了学生: {name}", current_editor)

        conn.close()
        return jsonify({"status": "success", "message": "删除成功"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/students/import', methods=['POST'])
@login_required
def import_students():
    """批量导入学生档案数据 (终极防撞车与极速版)"""
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "未上传文件"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "未选择文件"}), 400

    current_editor = session.get('real_name', '未知操作者')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        # 1. 强制读取工作表2并清洗表头
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file, dtype=str)
        else:
            try:
                df = pd.read_excel(file, sheet_name=1, dtype=str)
            except ValueError:
                return jsonify({"status": "error", "message": "读取失败：未找到工作表2，请严格使用模板！"}), 400

        df.columns = [str(c).replace('*', '').strip() for c in df.columns]
        df.dropna(how='all', inplace=True)
        df = df.fillna('')
        records = df.to_dict('records')

        conn = sqlite3.connect(MAIN_DB_FILE, timeout=20.0)
        cursor = conn.cursor()
        log_conn = sqlite3.connect(LOG_DB_FILE, timeout=20.0)
        log_cursor = log_conn.cursor()

        # 拉取全量识别码字典
        cursor.execute("SELECT custom_id, national_id, school_id FROM students")
        all_existing = cursor.fetchall()

        nid_to_cid = {row[1]: row[0] for row in all_existing if row[1]}
        sid_to_cid = {row[2]: row[0] for row in all_existing if row[2]}

        prefix_counters = {}
        for row in all_existing:
            cid = row[0]
            if cid and len(cid) >= 7:
                prefix = cid[:3]
                try:
                    seq = int(cid[3:])
                    if prefix not in prefix_counters or seq > prefix_counters[prefix]:
                        prefix_counters[prefix] = seq
                except ValueError:
                    pass

        update_data, insert_data, log_data = [], [], []
        insert_count, update_count, error_count = 0, 0, 0

        # 2. 内存计算与比对
        for row in records:
            name = str(row.get('姓名', '')).strip()
            if not name or name.lower() == 'nan':
                continue

            # 🌟【修复1】：提取内部编号作为最高匹配权
            internal_id = str(row.get('内部编号', '')).strip()
            if internal_id.lower() == 'nan': internal_id = ''

            national_id = str(row.get('国家身份证号', '')).strip()
            if national_id.lower() == 'nan': national_id = ''

            school_id = str(row.get('省学籍辅号', '')).strip()
            if school_id.lower() == 'nan': school_id = ''

            enter_year = str(row.get('入学年份', '')).strip()
            if enter_year.lower() == 'nan': enter_year = ''

            campus_name = str(row.get('校区', '')).strip()
            if campus_name.lower() == 'nan': campus_name = ''

            current_class = str(row.get('当前班级', '')).strip()
            if current_class.lower() == 'nan': current_class = ''

            former_name = str(row.get('曾用名', '')).strip()
            sex = str(row.get('性别', '')).strip()
            subject = str(row.get('选科', '')).strip()
            category = str(row.get('类别', '')).strip()
            major = str(row.get('专业', '')).strip()
            at_school = str(row.get('在校情况', '')).strip()
            remarks = str(row.get('特殊情况备注', '')).strip()
            boarding_status = str(row.get('住宿情况', '')).strip()
            apartment = str(row.get('公寓', '')).strip()
            dormitory = str(row.get('宿舍及床位', '')).strip()

            target_custom_id = None

            # 🌟【修复2】：确立三级降维匹配逻辑 (内部编号 > 身份证 > 学籍号)
            if internal_id:
                target_custom_id = internal_id
            elif national_id and national_id in nid_to_cid:
                target_custom_id = nid_to_cid[national_id]
            elif school_id and school_id in sid_to_cid:
                target_custom_id = sid_to_cid[school_id]

            # 🌟【修复3】：致命错误阻断 - 将空字符串转为 None (NULL)，避开 UNIQUE 冲突
            db_national_id = national_id if national_id else None
            db_school_id = school_id if school_id else None

            if target_custom_id:
                # 更新操作
                update_data.append((
                    name, former_name, sex, enter_year, campus_name, current_class, subject,
                    category, major, at_school, remarks, boarding_status, apartment, dormitory, current_time,
                    target_custom_id
                ))
                log_data.append(
                    (target_custom_id, '批量导入更新', f'通过Excel覆盖更新了资料', current_time, current_editor))
                update_count += 1
            else:
                # 新增操作，分配全新ID
                year_part = enter_year[-2:] if len(enter_year) >= 2 else '00'
                campus_part = '1' if campus_name == '本部' else ('2' if campus_name == '分校' else '0')
                prefix = f"{year_part}{campus_part}"

                if prefix not in prefix_counters:
                    prefix_counters[prefix] = 0
                prefix_counters[prefix] += 1

                final_custom_id = f"{prefix}{prefix_counters[prefix]:04d}"

                if national_id: nid_to_cid[national_id] = final_custom_id
                if school_id: sid_to_cid[school_id] = final_custom_id

                # 💡 这里强制使用转换后的 db_school_id 和 db_national_id
                insert_data.append((
                    final_custom_id, db_school_id, db_national_id, name, former_name, sex, enter_year, campus_name,
                    current_class, subject, category, major, at_school, remarks, boarding_status, apartment, dormitory,
                    current_time
                ))
                log_data.append(
                    (final_custom_id, '批量导入新增', f'通过Excel导入了新学生：{name}', current_time, current_editor))
                insert_count += 1

        # 3. 极速批量执行
        try:
            if update_data:
                cursor.executemany('''
                    UPDATE students SET 
                        name=?, former_name=?, sex=?, enter_year=?, campus=?, current_class=?, subject=?, 
                        category=?, major=?, at_school=?, remarks=?, boarding_status=?, apartment=?, dormitory=?, last_edit_at=?
                    WHERE custom_id=?
                ''', update_data)

            if insert_data:
                cursor.executemany('''
                    INSERT INTO students (custom_id, school_id, national_id, name, former_name, sex, enter_year, campus, current_class, subject, category, major, at_school, remarks, boarding_status, apartment, dormitory, last_edit_at) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', insert_data)

            if log_data:
                log_cursor.executemany('''
                    INSERT INTO change_logs (custom_id, action_type, details, timestamp, editor)
                    VALUES (?, ?, ?, ?, ?)
                ''', log_data)

            conn.commit()
            log_conn.commit()

        except sqlite3.IntegrityError as e:
            conn.rollback()
            log_conn.rollback()
            print(f"事务被数据库约束熔断: {str(e)}")  # 打印具体原因到终端，方便排错
            error_count += len(insert_data) + len(update_data)
            insert_count, update_count = 0, 0

        finally:
            conn.close()
            log_conn.close()

        return jsonify({"status": "success", "insert_count": insert_count, "update_count": update_count,
                        "error_count": error_count})

    except Exception as e:
        import traceback
        print(f"导入报错: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": f"解析异常: {str(e)}"}), 500


@app.route('/api/students/export_query', methods=['GET'])
@login_required
def export_query_students():
    """根据前端查询条件（完美适配动态 JSON 高级条件与普通模式），导出标准 Excel"""

    is_advanced = request.args.get('is_advanced', 'false').lower() == 'true'

    query = "SELECT * FROM students WHERE 1=1"
    params = []

    # =========== 组装 SQL 条件 ===========
    if is_advanced:
        # 【高级模式】：接收并解析前端发来的 JSON 动态条件数组
        adv_conditions_str = request.args.get('adv_conditions', '[]')
        try:
            conditions = json.loads(adv_conditions_str)
        except Exception:
            conditions = []

        # 安全防御：防 SQL 注入的白名单映射
        valid_fields = [
            'custom_id', 'school_id', 'national_id', 'name', 'former_name',
            'sex', 'enter_year', 'campus', 'current_class', 'subject',
            'category', 'major', 'at_school', 'boarding_status', 'apartment',
            'dormitory', 'remarks'
        ]
        valid_ops = ['=', '!=', 'LIKE']

        if conditions:
            query += " AND ("
            for i, cond in enumerate(conditions):
                logic = cond.get('logic', 'AND')
                if i == 0: logic = ''  # 第一行的逻辑运算词忽略

                field = cond.get('field')
                op = cond.get('op')
                val = cond.get('value')

                # 只允许白名单内的字段和符号进入 SQL 拼接
                if field in valid_fields and op in valid_ops:
                    if logic:
                        query += f" {logic} "

                    if op == 'LIKE':
                        query += f"{field} LIKE ?"
                        params.append(f"%{val}%")
                    else:
                        query += f"{field} {op} ?"
                        params.append(val)

            query += ")"

    else:
        # 【普通模式】：处理传统的模糊搜索框
        search = request.args.get('search', '').strip()
        campus = request.args.get('campus', '').strip()
        enter_year = request.args.get('enter_year', '').strip()
        current_class = request.args.get('current_class', '').strip()

        if search:
            query += " AND (name LIKE ? OR custom_id LIKE ? OR national_id LIKE ?)"
            params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

        if campus and campus != '全部':
            query += " AND campus = ?"
            params.append(campus)
        if enter_year and enter_year != '全部':
            query += " AND enter_year = ?"
            params.append(enter_year)
        if current_class and current_class != '全部':
            query += " AND current_class = ?"
            params.append(current_class)

    # 固定排序，保证导出名单的可读性
    query += " ORDER BY enter_year DESC, campus, current_class, custom_id"

    # =========== 执行查询与导出 ===========
    conn = sqlite3.connect(MAIN_DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    students_data = [dict(row) for row in rows]

    # 调用位于 utils/excel_exporter.py 的外部工具生成 Excel
    template_path = os.path.join(app.root_path, 'static', 'files', '学生信息批量修改模板.xlsx')

    try:
        excel_io = generate_students_query_excel(students_data, template_path)
    except FileNotFoundError as e:
        return jsonify({"status": "error", "message": str(e)}), 404

    return send_file(
        excel_io,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='查询结果批量下载.xlsx'
    )

# ================= 4. API 接口：考务与成绩管理-考试数据初始化 （第一步）=================
@app.route('/api/upload_scores', methods=['POST'])
@login_required
def upload_scores():
    """
    接收前端表单与 .xlsx Excel，处理成绩落盘并生成师资快照
    (注意：此操作无须进行学籍档案的日志审计)
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
            return jsonify({'success': False, 'message': '表单参数缺失，请填写完整的考务信息'})

        # 生成唯一的 exam_id (如 EX_2024_1716...)
        exam_id = f"EX_{grade}_{int(time.time())}"
        
        # 2. 接收并严格验证 Excel 文件
        if 'score_file' not in request.files:
            return jsonify({'success': False, 'message': '未找到上传的文件'})
        
        file = request.files['score_file']
        if file.filename == '':
            return jsonify({'success': False, 'message': '文件名为空'})
            
        # 严格要求读取 .xlsx，且强制读取工作表2 (sheet_name=1)
        try:
            df = pd.read_excel(file, sheet_name=1)
        except ValueError:
            return jsonify({'success': False, 'message': '文件读取失败：未找到第二个工作表(Sheet2)。请严格使用包含Sheet2的.xlsx模板！'})
        except Exception as e:
            return jsonify({'success': False, 'message': f'文件解析错误，请确认上传的是合法的 .xlsx 文件。({str(e)})'})
        
        # 动态连接到对应年级的分库
        try:
            conn = get_scores_db_connection(grade)
        except FileNotFoundError as e:
            return jsonify({'success': False, 'message': str(e)})
            
        cursor = conn.cursor()
        
        # 开启数据库事务，确保一致性防雷
        cursor.execute("BEGIN TRANSACTION;")
        
        # ================= 阶段 1：考务元数据落盘 =================
        cursor.execute('''
            INSERT INTO exam_metadata (exam_id, exam_name, exam_date, semester, exam_type, creator, is_analyzed)
            VALUES (?, ?, ?, ?, ?, ?, 0)
        ''', (exam_id, exam_name, exam_date, semester, exam_type, current_editor))
        
        # ================= 阶段 2：智能解析与清洗成绩 =================
        subject_columns = ['语文', '数学', '英语', '物理', '化学', '生物', '政治', '历史', '地理']
        raw_scores_data = []
        
        for index, row in df.iterrows():
            internal_id = str(row.get('内部编号', '')).strip()
            student_name = str(row.get('姓名', '')).strip()
            
            # 过滤掉完全空行的无效数据
            if not student_name and internal_id in ('', 'nan', 'None'):
                continue
            
            # 身份匹配逻辑 (依靠第 6 张表 student_class_assignments)
            custom_id = None
            if internal_id and internal_id.lower() not in ('nan', 'none'):
                cursor.execute("SELECT custom_id FROM student_class_assignments WHERE custom_id = ? AND semester = ?", (internal_id, semester))
                res = cursor.fetchone()
                if not res:
                    conn.rollback()
                    return jsonify({'success': False, 'message': f'第{index+2}行匹配失败：系统中找不到内部编号为 {internal_id} 的学生。'})
                custom_id = res['custom_id']
            else:
                cursor.execute("SELECT custom_id FROM student_class_assignments WHERE student_name = ? AND semester = ?", (student_name, semester))
                res = cursor.fetchall()
                if len(res) == 0:
                    conn.rollback()
                    return jsonify({'success': False, 'message': f'第{index+2}行匹配失败：在本学期底表中找不到名为 {student_name} 的学生。'})
                elif len(res) > 1:
                    conn.rollback()
                    return jsonify({'success': False, 'message': f'导入中断：发现重名学生 {student_name}，请在 Excel 中填写【内部编号】加以区分！'})
                else:
                    custom_id = res[0]['custom_id']
            
            # 分数清洗逻辑：处理 NULL(未选考) 和 -1(缺考)
            scores = []
            for subj in subject_columns:
                val = row.get(subj, None)
                if pd.isna(val) or str(val).strip() == '':
                    scores.append(None) # 转换为数据库的标准 NULL
                else:
                    try:
                        float_val = float(val)
                        scores.append(float_val) # 如果是 -1.0，原样存入
                    except ValueError:
                        scores.append(None)

            raw_scores_data.append([exam_id, custom_id] + scores)
            
        # 批量插入成绩到底表
        cursor.executemany('''
            INSERT INTO students_raw_scores 
            (exam_id, custom_id, chinese, math, english, physics, chemistry, biology, politics, history, geography)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', raw_scores_data)
        
        # ================= 阶段 3：克隆师资快照 =================
        cursor.execute('''
            INSERT INTO dynamic_class_mapping 
            (exam_id, campus, class_type, class_name, subject, teacher_name, teacher_uid)
            SELECT ?, campus, class_type, class_name, subject, teacher_name, teacher_uid
            FROM stable_class_mapping
            WHERE semester = ?
        ''', (exam_id, semester))
        
        # 所有操作成功，提交事务！
        conn.commit()
        
        return jsonify({
            'success': True, 
            'message': '成绩导入成功，快照生成完毕！',
            'exam_id': exam_id 
        })

    except Exception as e:
        import traceback
        print(f"成绩导入异常: {traceback.format_exc()}")
        if 'conn' in locals():
            conn.rollback()
        return jsonify({'success': False, 'message': f'服务器内部错误: {str(e)}'})
    finally:
        if 'conn' in locals():
            conn.close()

# ================= 4. API 接口：考务与成绩管理-师资快照配置 (第二步) =================

@app.route('/scoresManagement/teacher_mapping_config')
@login_required
def render_teacher_mapping_config():
    """渲染师资快照配置页面"""
    exam_id = request.args.get('exam_id')
    grade = request.args.get('grade')
    if not exam_id or not grade:
        return "缺少关键参数 (exam_id 或 grade)，请从成绩导入页正常跳转。", 400
    # 将参数传递给前端模板
    return render_template('scoresManagement/teacher_mapping_config.html', exam_id=exam_id, grade=grade)

@app.route('/api/scores/teacher_mapping', methods=['GET'])
@login_required
def get_teacher_mapping():
    """获取指定考试的师资快照，并与系统账号库实时碰对"""
    exam_id = request.args.get('exam_id')
    grade = request.args.get('grade')
    
    try:
        # 1. 提取全局用户库的姓名映射，放在内存中提高比对速度
        users_conn = sqlite3.connect(USERS_DB_FILE)
        users_cursor = users_conn.cursor()
        users_cursor.execute("SELECT real_name, uid FROM users")
        # 形成字典：{'王建国': 10, '赵铁柱': 12, ...}
        users_dict = {row[0]: row[1] for row in users_cursor.fetchall()}
        users_conn.close()
        
        # 2. 查询该场考试的快照数据
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM dynamic_class_mapping WHERE exam_id = ?", (exam_id,))
        rows = cursor.fetchall()
        
        result = []
        # 3. 遍历比对状态
        for r in rows:
            row_data = dict(r)
            t_name = row_data['teacher_name']
            if t_name in users_dict:
                row_data['matched'] = True
                row_data['teacher_uid'] = users_dict[t_name]
            else:
                row_data['matched'] = False
                row_data['teacher_uid'] = 0  # 0 代表无账号
                
            result.append(row_data)
            
            # 顺手将最新比对到的 UID 同步回底表，保证数据绝对一致
            cursor.execute("UPDATE dynamic_class_mapping SET teacher_uid = ? WHERE id = ?", 
                           (row_data['teacher_uid'], row_data['id']))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})

@app.route('/api/scores/update_teacher_mapping', methods=['POST'])
@login_required
def update_teacher_mapping():
    """处理前端传来的行内编辑：修改快照中的教师姓名，并返回比对结果"""
    data = request.json
    mapping_id = data.get('id')
    new_teacher_name = data.get('teacher_name', '').strip()
    grade = data.get('grade')
    
    if not mapping_id or not new_teacher_name:
        return jsonify({'success': False, 'message': '参数不完整'})
        
    try:
        # 1. 去全局库查一下新改的名字有没有账号
        users_conn = sqlite3.connect(USERS_DB_FILE)
        users_cursor = users_conn.cursor()
        users_cursor.execute("SELECT uid FROM users WHERE real_name = ?", (new_teacher_name,))
        user_row = users_cursor.fetchone()
        users_conn.close()
        
        new_uid = user_row[0] if user_row else 0
        matched = bool(user_row)
        
        # 2. 无论有没有账号，都必须把名字更新进快照表（因为哪怕没账号，也是真实代课人）
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()
        cursor.execute("UPDATE dynamic_class_mapping SET teacher_name = ?, teacher_uid = ? WHERE id = ?", 
                       (new_teacher_name, new_uid, mapping_id))
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'matched': matched, 
            'teacher_name': new_teacher_name,
            'message': '匹配成功' if matched else '警告：未找到该账号'
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/scoresManagement/stable_mapping_hub')
@login_required
def render_stable_mapping_hub():
    """渲染师资底表管理大厅页面"""
    return render_template('scoresManagement/stable_mapping_hub.html')


@app.route('/api/scores/stable_mapping', methods=['GET'])
@login_required
def get_stable_mapping():
    """获取指定年级和学期的师资底表数据，并与系统账号实时核对"""
    grade = request.args.get('grade')
    semester = request.args.get('semester')

    if not grade or not semester:
        return jsonify({'success': False, 'message': '缺少搜索条件'})

    try:
        # 1. 提取全局用户库映射字典
        users_conn = sqlite3.connect(USERS_DB_FILE)
        users_cursor = users_conn.cursor()
        users_cursor.execute("SELECT real_name, uid FROM users")
        users_dict = {row[0]: row[1] for row in users_cursor.fetchall()}
        users_conn.close()

        # 2. 动态路由到对应年级的分库，拉取底表
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM stable_class_mapping WHERE semester = ?", (semester,))
        rows = cursor.fetchall()

        result = []
        for r in rows:
            row_data = dict(r)
            t_name = row_data['teacher_name']
            # 实时碰对
            if t_name in users_dict:
                row_data['matched'] = True
                row_data['teacher_uid'] = users_dict[t_name]
            else:
                row_data['matched'] = False
                row_data['teacher_uid'] = 0

            result.append(row_data)
            # 更新最新 UID 到底表
            cursor.execute("UPDATE stable_class_mapping SET teacher_uid = ? WHERE id = ?",
                           (row_data['teacher_uid'], row_data['id']))

        conn.commit()
        conn.close()

        return jsonify({'success': True, 'data': result})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/scores/update_stable_mapping', methods=['POST'])
@login_required
def update_stable_mapping():
    """处理前端行内微调：单条修改底表的教师姓名"""
    data = request.json
    mapping_id = data.get('id')
    new_teacher_name = data.get('teacher_name', '').strip()
    grade = data.get('grade')

    if not mapping_id or not new_teacher_name or not grade:
        return jsonify({'success': False, 'message': '参数不完整'})

    try:
        # 核验新账号
        users_conn = sqlite3.connect(USERS_DB_FILE)
        users_cursor = users_conn.cursor()
        users_cursor.execute("SELECT uid FROM users WHERE real_name = ?", (new_teacher_name,))
        user_row = users_cursor.fetchone()
        users_conn.close()

        new_uid = user_row[0] if user_row else 0
        matched = bool(user_row)

        # 写入物理分库
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()
        cursor.execute("UPDATE stable_class_mapping SET teacher_name = ?, teacher_uid = ? WHERE id = ?",
                       (new_teacher_name, new_uid, mapping_id))
        conn.commit()
        conn.close()

        return jsonify({
            'success': True,
            'matched': matched,
            'teacher_name': new_teacher_name,
            'message': '匹配成功' if matched else '警告：未找到该账号'
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/scores/upload_stable_mapping', methods=['POST'])
@login_required
def upload_stable_mapping():
    """接收上传的Excel文件，清空并覆盖对应学期的排课底表 (新增防重名熔断)"""
    grade = request.form.get('grade')
    semester = request.form.get('semester')
    
    if 'file' not in request.files or not grade or not semester:
        return jsonify({'success': False, 'message': '参数不完整或未上传文件'})
        
    file = request.files['file']
    if not file.filename.endswith(('.xlsx', '.xls')):
        return jsonify({'success': False, 'message': '请上传 Excel 文件'})
        
    try:
        # 【修改点1】强制读取 Sheet2 (索引为 1)
        try:
            df = pd.read_excel(file, sheet_name=1)
        except ValueError:
            return jsonify({'success': False, 'message': '文件格式错误：未找到第二个工作表(Sheet2)。请严格使用排课模板！'})
        
        # 【修改点2】校验最新的表头结构
        required_cols = ['教师编号', '教师姓名', '校区', '班级类型', '班级名称', '学科']
        if not all(col in df.columns for col in required_cols):
            return jsonify({'success': False, 'message': 'Excel 缺少必要列，请确保包含：教师编号、教师姓名、校区、班级类型、班级名称、学科'})
            
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()
        
        # 连接全局账号库，准备防重名核验
        users_conn = sqlite3.connect(USERS_DB_FILE)
        users_cursor = users_conn.cursor()
        
        cursor.execute("BEGIN TRANSACTION;")
        # 暴力清洗老数据
        cursor.execute("DELETE FROM stable_class_mapping WHERE semester = ?", (semester,))
        
        insert_data = []
        for index, row in df.iterrows():
            teacher_id_raw = str(row.get('教师编号', '')).strip()
            teacher_name = str(row.get('教师姓名', '')).strip()
            campus = str(row.get('校区', '')).strip()
            class_type = str(row.get('班级类型', '')).strip()
            class_name = str(row.get('班级名称', '')).strip()
            subject = str(row.get('学科', '')).strip()
            
            # 过滤无效空行
            if not class_name or class_name in ('nan', 'None') or not teacher_name or teacher_name in ('nan', 'None'):
                continue
                
            # 【修改点3】核心防重名熔断逻辑
            uid = 0
            if teacher_id_raw and teacher_id_raw.lower() not in ('nan', 'none'):
                # 场景A：有编号，直接按编号（uid）精准查找
                users_cursor.execute("SELECT uid FROM users WHERE uid = ?", (teacher_id_raw,))
                res = users_cursor.fetchone()
                if res:
                    uid = res[0]
            else:
                # 场景B：无编号，按姓名查找
                users_cursor.execute("SELECT uid FROM users WHERE real_name = ?", (teacher_name,))
                res = users_cursor.fetchall()
                if len(res) == 0:
                    uid = 0 # 没账号（允许导入，标记为0）
                elif len(res) > 1:
                    # 发现重名，立即回滚并熔断！
                    conn.rollback()
                    users_conn.close()
                    conn.close()
                    return jsonify({'success': False, 'message': f'第{index+2}行导入中断：发现重名教师【{teacher_name}】，请在 Excel 中填写【教师编号】加以区分！'})
                else:
                    uid = res[0][0] # 唯一匹配
            
            insert_data.append((campus, class_type, class_name, subject, teacher_name, uid, semester))
            
        # 批量插入新数据
        cursor.executemany('''
            INSERT INTO stable_class_mapping 
            (campus, class_type, class_name, subject, teacher_name, teacher_uid, semester)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', insert_data)
        
        conn.commit()
        users_conn.close()
        conn.close()
        
        return jsonify({'success': True, 'message': f'成功覆盖！共载入 {len(insert_data)} 条师资记录。'})
        
    except Exception as e:
        import traceback
        print(f"底表上传异常: {traceback.format_exc()}")
        if 'conn' in locals():
            conn.rollback()
        if 'users_conn' in locals():
            users_conn.close()
        return jsonify({'success': False, 'message': f'解析失败: {str(e)}'})


# ==================== API接口：学生教学班管理模块 =======================

@app.route('/scoresManagement/stable_student_mapping_hub')
@login_required
def scores_stable_student_mapping_hub():
    """渲染学生教学班配置大厅页面"""
    return render_template('scoresManagement/stable_student_mapping_hub.html')


@app.route('/api/scores/student_course_mapping', methods=['GET'])
@login_required
def get_student_course_mapping():
    """拉取指定年级、学期的九科教学班全景数据"""
    grade = request.args.get('grade')
    semester = request.args.get('semester')

    if not grade or not semester:
        return jsonify({"status": "error", "message": "缺失年级或学期参数"}), 400

    try:
        # 严格遵守原则：使用 app.py 中已有的工厂函数连接对应的分库
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()

        # 查询该学期的排班数据
        cursor.execute("SELECT * FROM stable_student_course_mapping WHERE semester=? ORDER BY custom_id", (semester,))
        rows = cursor.fetchall()

        return jsonify({"status": "success", "data": [dict(row) for row in rows]})
    except FileNotFoundError as e:
        return jsonify({"status": "error", "message": str(e)}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if 'conn' in locals():
            conn.close()


@app.route('/api/scores/student_course_mapping/update_single', methods=['PUT'])
@login_required
def update_single_student_course():
    """行内闪电微调：单格数据更新"""
    data = request.json
    grade = data.get('grade')
    semester = data.get('semester')
    custom_id = data.get('custom_id')
    field = data.get('field')
    value = data.get('value', '').strip()

    # 安全白名单防御：防止 SQL 注入恶意篡改表结构
    allowed_fields = [
        'chinese_class', 'math_class', 'english_class',
        'physics_class', 'chemistry_class', 'biology_class',
        'politics_class', 'history_class', 'geography_class'
    ]
    if field not in allowed_fields:
        return jsonify({"status": "error", "message": "非法的修改字段"}), 400

    try:
        # 连接对应年级分库
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()

        # 更新单格数据
        query = f"UPDATE stable_student_course_mapping SET {field} = ? WHERE custom_id = ? AND semester = ?"
        cursor.execute(query, (value, custom_id, semester))
        conn.commit()
        return jsonify({"status": "success"})
    except FileNotFoundError as e:
        return jsonify({"status": "error", "message": str(e)}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        if 'conn' in locals():
            conn.close()


@app.route('/api/scores/student_course_mapping/upload', methods=['POST'])
@login_required
def upload_student_course_mapping():
    """极速上传并全盘覆盖学期教学班数据"""
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "无文件"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "未选择文件"}), 400

    grade = request.form.get('grade')
    semester = request.form.get('semester')
    if not grade or not semester:
        return jsonify({"status": "error", "message": "缺失年级或学期参数"}), 400

    try:
        # 1. 强制读取 Sheet2 (索引为 1)
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file, dtype=str)
        else:
            try:
                df = pd.read_excel(file, sheet_name=1, dtype=str)
            except ValueError:
                return jsonify({"status": "error", "message": "读取失败：未找到工作表2，请严格使用模板！"}), 400

        # 清洗表头并剔除空行
        df.columns = [str(c).replace('*', '').strip() for c in df.columns]
        df.dropna(how='all', inplace=True)
        df = df.fillna('')
        records = df.to_dict('records')

        # 连接对应年级分库
        conn = get_scores_db_connection(grade)
        cursor = conn.cursor()

        # 2. 开启事务：全盘换血模式
        cursor.execute("BEGIN TRANSACTION;")
        # 删除该学期旧数据（确保 Excel 中被删掉的学生，系统里也被清空，保证绝对同步）
        cursor.execute("DELETE FROM stable_student_course_mapping WHERE semester=?", (semester,))

        insert_data = []
        for row in records:
            custom_id = str(row.get('内部编号', '')).strip()
            if not custom_id: continue  # 【内部编号】是唯一命脉，无编号直接跳过

            # 兼容表头可能叫 "学生姓名" 或 "姓名"
            name = str(row.get('学生姓名', '')).strip() or str(row.get('姓名', '')).strip()
            track = str(row.get('选科', '')).strip()

            chinese = str(row.get('语文教学班', '')).strip()
            math_c = str(row.get('数学教学班', '')).strip()
            english = str(row.get('英语教学班', '')).strip()
            physics = str(row.get('物理教学班', '')).strip()
            chemistry = str(row.get('化学教学班', '')).strip()
            biology = str(row.get('生物教学班', '')).strip()
            politics = str(row.get('政治教学班', '')).strip()
            history = str(row.get('历史教学班', '')).strip()
            geography = str(row.get('地理教学班', '')).strip()

            insert_data.append((
                custom_id, name, track,
                chinese, math_c, english, physics, chemistry, biology, politics, history, geography,
                semester
            ))

        # 3. 极速批量写入大宽表
        cursor.executemany('''
            INSERT INTO stable_student_course_mapping 
            (custom_id, name, subject_track, chinese_class, math_class, english_class, physics_class, chemistry_class, biology_class, politics_class, history_class, geography_class, semester)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', insert_data)

        conn.commit()
        return jsonify({"status": "success", "update_count": len(insert_data)})

    except FileNotFoundError as e:
        return jsonify({"status": "error", "message": str(e)}), 404
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()  # 出现异常时回滚，避免污染数据库
        import traceback
        print(f"教学班矩阵上传异常: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": f"解析或入库失败: {str(e)}"}), 500
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    print("🚀 启动校园数据平台后端服务...")
    app.run(host='0.0.0.0', port=5000, debug=True)