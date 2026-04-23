import sqlite3
import os
import math
import json
import pandas as pd
from datetime import datetime
from contextlib import closing  # 引入资源上下文管理器
from flask import Blueprint, render_template, request, jsonify, session, send_file
from utils.decorators import login_required
from utils.excel_exporter import generate_students_query_excel

# 定义蓝图
students_bp = Blueprint('students', __name__)

# ================= 动态路径解析 =================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN_DB_FILE = os.path.join(BASE_DIR, 'db', 'core', 'students_info.db')
LOG_DB_FILE = os.path.join(BASE_DIR, 'db', 'core', 'students_info_change_log.db')


# ================= 辅助函数 =================

def write_log(custom_id, action_type, details, editor):
    """将操作记录写入独立的日志数据库，不掩盖异常，确保审计的强制性"""
    if not details: return
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with closing(sqlite3.connect(LOG_DB_FILE)) as conn:
        with conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO change_logs (custom_id, action_type, details, timestamp, editor)
                VALUES (?, ?, ?, ?, ?)
            ''', (custom_id, action_type, details, timestamp, editor))


def get_diff(old_dict, new_dict):
    """对比新旧字典，生成精确到字段的修改详情"""
    field_labels = {
        'school_id': '省学籍辅号', 'national_id': '国家身份证号', 'name': '姓名',
        'former_name': '曾用名', 'sex': '性别', 'enter_year': '所在年级',
        'campus': '校区', 'current_class': '班级', 'subject': '选科', 'language_type': '外语种类',
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


# ================= 路由接口 =================

@students_bp.route('/')
@login_required
def index():
    """渲染学生管理主页"""
    return render_template('studentsInfoManagement/index.html')


@students_bp.route('/api/query', methods=['POST'])
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
                          'current_class', 'subject', 'language_type', 'category', 'major', 'at_school',
                          'boarding_status', 'remarks', 'former_name'}
        allowed_ops = {'=', '!=', '>', '<', '>=', '<=', 'LIKE', 'IS_EMPTY', 'IS_NOT_EMPTY'}
        # ================= 跨库虚拟字段拦截器 =================
        # 遍历条件列表，查找 field = log_query 的目标条件
        virtual_condition = None
        for condition in filters:
            # 安全获取字段值，避免键不存在报错
            field_value = condition.get('field')
            if field_value == 'log_query':
                virtual_condition = condition
                break
        if virtual_condition:
            keyword_val = virtual_condition.get('value', '').strip()
            # 开启日志库独立查询
            with closing(sqlite3.connect(LOG_DB_FILE)) as log_conn:
                log_cursor = log_conn.cursor()
                log_cursor.execute("SELECT DISTINCT custom_id FROM change_logs WHERE details LIKE ?",
                                   (f'%{keyword_val}%',))
                found_ids = [row[0] for row in log_cursor.fetchall() if row[0]]

            # 将条件转化为底表的 IN 查询语句
            if found_ids:
                placeholders = ','.join(['?'] * len(found_ids))
                query_parts.append(f"custom_id IN ({placeholders})")
                params.extend(found_ids)
            else:
                # 如果没查到任何人，注入一个绝对为假的条件
                query_parts.append("1=0")

            # 销毁该虚拟条件，防止被后续的常规逻辑再次解析报错
            filters = [cond for cond in filters if cond.get('field') != 'log_query']

        for i, f in enumerate(filters):
            field = f.get('field')
            op = f.get('operator')
            val = str(f.get('value', '')).strip()
            # 提取逻辑连接词，无论它是第几个条件，先备用
            logic = f.get('logic', 'AND').upper()

            if field in allowed_fields and op in allowed_ops:
                if op == 'IS_EMPTY':
                    condition_str = f"({field} IS NULL OR {field} = '')"
                elif op == 'IS_NOT_EMPTY':
                    condition_str = f"({field} IS NOT NULL AND {field} != '')"
                elif val:
                    params.append(f"%{val}%" if op == 'LIKE' else val)
                    condition_str = f"{field} {op} ?"
                else:
                    continue
                # 判断前面是否已经有其他条件（比如被拦截器塞入的跨库条件）
                if not query_parts:
                    # 如果前面没有任何条件，这是真正的第一条，不需要逻辑词
                    query_parts.append(f"({condition_str})")
                else:
                    # 如果前面已经有条件了，必须用逻辑词连接
                    valid_logic = logic if logic in ('AND', 'OR') else 'AND'
                    query_parts.append(f"{valid_logic} ({condition_str})")
        where_clause = " WHERE " + " ".join(query_parts) if query_parts else ""
    else:
        where_clause = ""

    try:
        with closing(sqlite3.connect(MAIN_DB_FILE)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            print(f"SELECT * FROM students {where_clause} ORDER BY id DESC LIMIT ? OFFSET ?", params + [limit, offset])
            cursor.execute(f"SELECT COUNT(*) FROM students {where_clause}", params)
            total_count = cursor.fetchone()[0]
            cursor.execute(f"SELECT * FROM students {where_clause} ORDER BY id DESC LIMIT ? OFFSET ?",
                           params + [limit, offset])
            rows = cursor.fetchall()
        total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
        return jsonify({"status": "success", "data": [dict(r) for r in rows], "total": total_count, "page": page,
                        "total_pages": total_pages})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500



@students_bp.route('/api', methods=['POST'])
@login_required
def add_student():
    """新增单条学生记录"""
    data = request.json or {}
    current_editor = session.get('real_name', '未知操作者')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    enter_year = data.get('enter_year', '')
    campus = data.get('campus', '')
    year_part = enter_year[-2:] if len(enter_year) == 4 else '00'

    if campus == '校本部':
        campus_part = '91'
    elif campus == '礼贤校区':
        campus_part = '81'
    else:
        campus_part = '00'

    try:
        with closing(sqlite3.connect(MAIN_DB_FILE, isolation_level=None)) as conn:
            # 使用 EXCLUSIVE 锁解决并发竞态条件
            conn.execute("BEGIN EXCLUSIVE")
            try:
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
                        current_class, subject, language_type, category, major, at_school, remarks, boarding_status, 
                        apartment, dormitory, last_edit_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    custom_id, data.get('school_id'), data.get('national_id'), data.get('name'),
                    data.get('former_name'), data.get('sex'), enter_year, campus,
                    data.get('current_class'), data.get('subject'), data.get('language_type'), data.get('category'),
                    data.get('major'), data.get('at_school'), data.get('remarks'),
                    data.get('boarding_status'), data.get('apartment'), data.get('dormitory'), current_time
                ))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

        write_log(custom_id, '新增档案', f"创建了学生档案: {data.get('name')}", current_editor)
        return jsonify({"status": "success", "message": "添加成功", "custom_id": custom_id})

    except sqlite3.IntegrityError:
        return jsonify({"status": "error", "message": "学号或身份证号已存在，请检查"}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": f"系统错误: {str(e)}"}), 500


@students_bp.route('/api/<int:student_id>', methods=['PUT'])
@login_required
def update_student(student_id):
    """更新学生记录"""
    data = request.json or {}
    current_editor = session.get('real_name', '未知操作者')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        with closing(sqlite3.connect(MAIN_DB_FILE)) as conn:
            conn.row_factory = sqlite3.Row
            with conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM students WHERE id=?", (student_id,))
                old_record = dict(cursor.fetchone() or {})

                if not old_record:
                    return jsonify({"status": "error", "message": "学生记录不存在"}), 404

                cursor.execute('''
                    UPDATE students SET 
                        school_id=?, national_id=?, name=?, former_name=?, sex=?, enter_year=?, campus=?, 
                        current_class=?, subject=?, language_type=?, category=?, major=?, at_school=?, remarks=?, 
                        boarding_status=?, apartment=?, dormitory=?, last_edit_at=?
                    WHERE id=?
                ''', (
                    data.get('school_id'), data.get('national_id'), data.get('name'), data.get('former_name'),
                    data.get('sex'), data.get('enter_year'), data.get('campus'), data.get('current_class'),
                    data.get('subject'), data.get('language_type'), data.get('category'), data.get('major'),
                    data.get('at_school'),
                    data.get('remarks'), data.get('boarding_status'), data.get('apartment'),
                    data.get('dormitory'), current_time, student_id
                ))

                cursor.execute("SELECT * FROM students WHERE id=?", (student_id,))
                new_record = dict(cursor.fetchone())

        diff_str = get_diff(old_record, new_record)
        if diff_str:
            write_log(old_record['custom_id'], '修改档案', diff_str, current_editor)

        return jsonify({"status": "success", "message": "更新成功"})
    except sqlite3.IntegrityError:
        return jsonify({"status": "error", "message": "唯一标识（如学号/身份证）冲突"}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": f"系统错误: {str(e)}"}), 500


@students_bp.route('/api/<int:student_id>', methods=['DELETE'])
@login_required
def delete_student(student_id):
    """删除学生记录"""
    current_editor = session.get('real_name', '未知操作者')
    try:
        with closing(sqlite3.connect(MAIN_DB_FILE)) as conn:
            conn.row_factory = sqlite3.Row
            with conn:
                cursor = conn.cursor()
                cursor.execute("SELECT custom_id, name FROM students WHERE id=?", (student_id,))
                record = cursor.fetchone()

                if record:
                    custom_id = record['custom_id']
                    name = record['name']
                    cursor.execute("DELETE FROM students WHERE id=?", (student_id,))

        if record:
            write_log(custom_id, '删除档案', f"删除了学生: {name}", current_editor)

        return jsonify({"status": "success", "message": "删除成功"})
    except Exception as e:
        return jsonify({"status": "error", "message": f"系统错误: {str(e)}"}), 500


@students_bp.route('/api/import', methods=['POST'])
@login_required
def import_students():
    """
    【全量更新版】批量导入学生档案数据
    支持新增字段：language_type (外语种类)
    具备三级降维匹配逻辑与事务防撞车机制
    """
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "未上传文件"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"status": "error", "message": "未选择文件"}), 400

    current_editor = session.get('real_name', '未知操作者')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
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

        insert_count, update_count, error_count = 0, 0, 0

        # 双数据库安全事务上下文
        with closing(sqlite3.connect(MAIN_DB_FILE, timeout=20.0)) as conn, \
                closing(sqlite3.connect(LOG_DB_FILE, timeout=20.0)) as log_conn:

            cursor = conn.cursor()
            log_cursor = log_conn.cursor()

            try:
                cursor.execute("BEGIN TRANSACTION")
                log_cursor.execute("BEGIN TRANSACTION")

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
                            prefix_counters[prefix] = max(prefix_counters.get(prefix, 0), seq)
                        except ValueError:
                            pass

                update_data, insert_data, log_data = [], [], []

                for row in records:
                    name = str(row.get('姓名', '')).strip()
                    if not name or name.lower() == 'nan':
                        continue

                    internal_id = str(row.get('内部编号', '')).strip()
                    national_id = str(row.get('国家身份证号', '')).strip()
                    school_id = str(row.get('省学籍辅号', '')).strip()
                    language_type = str(row.get('外语种类', '英语')).strip()

                    target_custom_id = None
                    if internal_id and internal_id.lower() not in ('nan', 'none'):
                        target_custom_id = internal_id
                    elif national_id and national_id in nid_to_cid:
                        target_custom_id = nid_to_cid[national_id]
                    elif school_id and school_id in sid_to_cid:
                        target_custom_id = sid_to_cid[school_id]

                    db_national_id = national_id if national_id and national_id.lower() not in ('nan', 'none') else None
                    db_school_id = school_id if school_id and school_id.lower() not in ('nan', 'none') else None
                    enter_year = str(row.get('所在年级', '')).strip()
                    campus_name = str(row.get('校区', '')).strip()

                    common_values = (
                        name, str(row.get('曾用名', '')).strip(), str(row.get('性别', '')).strip(),
                        enter_year, campus_name, str(row.get('当前班级', '')).strip(),
                        str(row.get('选科', '')).strip(), str(row.get('类别', '')).strip(),
                        str(row.get('专业', '')).strip(), str(row.get('在校情况', '')).strip(),
                        str(row.get('特殊情况备注', '')).strip(), str(row.get('住宿情况', '')).strip(),
                        str(row.get('公寓', '')).strip(), str(row.get('宿舍及床位', '')).strip(),
                        language_type, current_time
                    )

                    if target_custom_id:
                        update_data.append(common_values + (target_custom_id,))
                        log_data.append(
                            (target_custom_id, '批量导入更新', f'通过Excel更新资料，语种设定为:{language_type}',
                             current_time, current_editor))
                        update_count += 1
                    else:
                        year_part = enter_year[-2:] if len(enter_year) >= 2 else '00'
                        campus_part = '91' if campus_name == '校本部' else '81' if campus_name == '礼贤校区' else '00'
                        prefix = f"{year_part}{campus_part}"

                        prefix_counters[prefix] = prefix_counters.get(prefix, 0) + 1
                        final_custom_id = f"{prefix}{prefix_counters[prefix]:04d}"

                        insert_data.append((final_custom_id, db_school_id, db_national_id) + common_values)
                        log_data.append((final_custom_id, '批量导入新增', f'通过Excel导入新学生:{name}',
                                         current_time, current_editor))
                        insert_count += 1

                if update_data:
                    cursor.executemany('''
                        UPDATE students SET 
                            name=?, former_name=?, sex=?, enter_year=?, campus=?, current_class=?, subject=?, 
                            category=?, major=?, at_school=?, remarks=?, boarding_status=?, apartment=?, 
                            dormitory=?, language_type=?, last_edit_at=?
                        WHERE custom_id=?
                    ''', update_data)

                if insert_data:
                    cursor.executemany('''
                        INSERT INTO students (
                            custom_id, school_id, national_id, name, former_name, sex, enter_year, campus, 
                            current_class, subject, category, major, at_school, remarks, boarding_status, 
                            apartment, dormitory, language_type, last_edit_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', insert_data)

                if log_data:
                    log_cursor.executemany('''
                        INSERT INTO change_logs (custom_id, action_type, details, timestamp, editor)
                        VALUES (?, ?, ?, ?, ?)
                    ''', log_data)

                conn.commit()
                log_conn.commit()

            except Exception as inner_e:
                # 捕获所有异常类型（包括 IntegrityError 等），统一执行双回滚
                conn.rollback()
                log_conn.rollback()
                error_count = len(insert_data) + len(update_data)
                insert_count = update_count = 0
                raise inner_e  # 抛出让外层捕获返回 500

        return jsonify({
            "status": "success",
            "insert_count": insert_count,
            "update_count": update_count,
            "error_count": error_count
        })

    except Exception as e:
        return jsonify({"status": "error", "message": f"处理失败: {str(e)}"}), 500


@students_bp.route('/api/export_query', methods=['GET'])
@login_required
def export_query_students():
    """
    【修复版】根据前端查询条件导出 Excel
    支持普通模式下的：关键词、校区、年级、班级过滤
    """
    is_advanced = request.args.get('is_advanced', 'false').lower() == 'true'
    query = "SELECT * FROM students WHERE 1=1"
    params = []

    if is_advanced:
        adv_conditions_str = request.args.get('adv_conditions', '[]')
        try:
            conditions = json.loads(adv_conditions_str)
        except Exception:
            conditions = []

        valid_fields = ['custom_id', 'school_id', 'national_id', 'name', 'former_name', 'sex',
                        'enter_year', 'campus', 'current_class', 'subject', 'category',
                        'major', 'at_school', 'boarding_status', 'apartment', 'dormitory',
                        'remarks', 'language_type']
        valid_ops = ['=', '!=', 'LIKE', 'IS_EMPTY', 'IS_NOT_EMPTY']

        if conditions:
            parts = []
            for cond in conditions:
                field = cond.get('field')
                op = cond.get('op')
                val = str(cond.get('value', '')).strip()

                if field in valid_fields and op in valid_ops:
                    condition_str = ""
                    if op == 'IS_EMPTY':
                        condition_str = f"({field} IS NULL OR {field} = '')"
                    elif op == 'IS_NOT_EMPTY':
                        condition_str = f"({field} IS NOT NULL AND {field} != '')"
                    elif val:
                        condition_str = f"{field} {op} ?"
                        params.append(f"%{val}%" if op == 'LIKE' else val)
                    else:
                        continue

                    logic = cond.get('logic', 'AND') if len(parts) > 0 else ""
                    if logic:
                        parts.append(logic)
                    parts.append(condition_str)
            if parts:
                query += " AND (" + " ".join(parts) + ")"
    else:
        search = request.args.get('search', '').strip()
        campus = request.args.get('campus', '').strip()
        enter_year = request.args.get('enter_year', '').strip()
        current_class = request.args.get('current_class', '').strip()

        if search:
            query += " AND (name LIKE ? OR custom_id LIKE ? OR national_id LIKE ?)"
            params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

        if campus and campus not in ('全部', ''):
            query += " AND campus = ?"
            params.append(campus)

        if enter_year and enter_year not in ('全部', ''):
            query += " AND enter_year = ?"
            params.append(enter_year)

        if current_class and current_class not in ('全部', ''):
            query += " AND current_class = ?"
            params.append(current_class)

    query += " ORDER BY enter_year DESC, campus, current_class, custom_id"

    try:
        with closing(sqlite3.connect(MAIN_DB_FILE)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()

        students_data = [dict(row) for row in rows]
        template_path = os.path.join(BASE_DIR, 'static', 'files', '学生信息批量修改模板.xlsx')

        excel_io = generate_students_query_excel(students_data, template_path)
        return send_file(
            excel_io,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='查询结果批量下载.xlsx'
        )
    except Exception as e:
        return jsonify({"status": "error", "message": f"导出失败: {str(e)}"}), 500


@students_bp.route('/api/<custom_id>/logs', methods=['GET'])
@login_required
def get_student_logs(custom_id):
    """拉取指定学生的修改日志 (时间倒序)"""
    try:
        with closing(sqlite3.connect(LOG_DB_FILE)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute('''
                SELECT action_type, details, timestamp, editor 
                FROM change_logs 
                WHERE custom_id = ? 
                ORDER BY timestamp DESC
            ''', (custom_id,))
            rows = cursor.fetchall()

        return jsonify({"status": "success", "data": [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({"status": "error", "message": f"拉取日志失败: {str(e)}"}), 500


@students_bp.route('/api/logs/recent', methods=['GET'])
@login_required
def get_recent_logs():
    """获取全校最近的 20 条学生档案修改记录 (附带跨库查询姓名)"""
    try:
        # 1. 从日志库提取最新的20条记录
        with closing(sqlite3.connect(LOG_DB_FILE)) as conn_log:
            conn_log.row_factory = sqlite3.Row
            cursor_log = conn_log.cursor()
            cursor_log.execute('''
                SELECT timestamp, editor, custom_id, action_type, details 
                FROM change_logs 
                ORDER BY timestamp DESC 
                LIMIT 20
            ''')
            log_rows = [dict(r) for r in cursor_log.fetchall()]

        # 2. 收集需要查询的学生编号去重列表
        custom_ids = list(set([r['custom_id'] for r in log_rows if r.get('custom_id')]))
        name_map = {}

        # 3. 连接主业务库，批量查询并构建编号到姓名的映射字典
        if custom_ids:
            with closing(sqlite3.connect(MAIN_DB_FILE)) as conn_main:
                cursor_main = conn_main.cursor()
                # 动态生成占位符
                placeholders = ','.join(['?'] * len(custom_ids))
                cursor_main.execute(f'''
                    SELECT custom_id, name FROM students WHERE custom_id IN ({placeholders})
                ''', custom_ids)

                for cid, name in cursor_main.fetchall():
                    name_map[cid] = name

        # 4. 将查到的姓名拼装入返回的数据中 (若学生已被彻底删除则兜底显示)
        for log in log_rows:
            log['student_name'] = name_map.get(log['custom_id'], '未知/已删除')

        return jsonify({"status": "success", "data": log_rows})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500