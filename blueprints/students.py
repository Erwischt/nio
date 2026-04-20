import sqlite3
import os
import math
import json
import pandas as pd
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, session, send_file
from utils.decorators import login_required
from utils.excel_exporter import generate_students_query_excel

# 定义蓝图
students_bp = Blueprint('students', __name__)

# ================= 动态路径解析 =================
# 定位到项目根目录下的数据库文件
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN_DB_FILE = os.path.join(BASE_DIR, 'db', 'core', 'students_info.db')
LOG_DB_FILE = os.path.join(BASE_DIR, 'db', 'core', 'students_info_change_log.db')

# ================= 辅助函数 =================

def write_log(custom_id, action_type, details, editor):
    """将操作记录安全写入独立的日志数据库"""
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
                          'boarding_status', 'remarks'}
        # 1. 扩展白名单，加入 'IS_EMPTY' 和 'IS_NOT_EMPTY'
        allowed_ops = {'=', '!=', '>', '<', '>=', '<=', 'LIKE', 'IS_EMPTY', 'IS_NOT_EMPTY'}
        for i, f in enumerate(filters):
            field = f.get('field')
            op = f.get('operator')
            val = str(f.get('value', '')).strip()
            logic = f.get('logic', 'AND').upper() if i > 0 else ''
            # 2. 移除对 val 必须存在的统一硬性校验，将校验下放到具体操作符中
            if field in allowed_fields and op in allowed_ops:
                # 3. 针对“为空”和“不为空”做特殊 SQL 拼接，不需要追加参数
                if op == 'IS_EMPTY':
                    condition_str = f"({field} IS NULL OR {field} = '')"
                elif op == 'IS_NOT_EMPTY':
                    condition_str = f"({field} IS NOT NULL AND {field} != '')"
                # 4. 对于其他常规操作符，仍然需要校验 val 是否存在
                elif val:
                    params.append(f"%{val}%" if op == 'LIKE' else val)
                    condition_str = f"{field} {op} ?"
                else:
                    continue  # 如果是常规操作符但没有填值，则跳过此条件
                # 组装 SQL 语句
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
    return jsonify({"status": "success", "data": [dict(r) for r in rows], "total": total_count, "page": page, "total_pages": total_pages})

@students_bp.route('/api', methods=['POST'])
@login_required
def add_student():
    """新增单条学生记录"""
    data = request.json
    current_editor = session.get('real_name', '未知操作者')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    enter_year = data.get('enter_year', '')
    campus = data.get('campus', '')
    year_part = enter_year[-2:] if len(enter_year) == 4 else '00'
    campus_part = '0'
    if campus == '校本部':
        campus_part = '91'
    elif campus == '礼贤校区':
        campus_part = '81'
    else:
        campus_part = '00'

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
        conn.commit()
        conn.close()

        write_log(custom_id, '新增档案', f"创建了学生档案: {data.get('name')}", current_editor)
        return jsonify({"status": "success", "message": "添加成功", "custom_id": custom_id})

    except sqlite3.IntegrityError:
        return jsonify({"status": "error", "message": "学号或身份证号已存在，请检查"}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@students_bp.route('/api/<int:student_id>', methods=['PUT'])
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
                current_class=?, subject=?, language_type=?, category=?, major=?, at_school=?, remarks=?, 
                boarding_status=?, apartment=?, dormitory=?, last_edit_at=?
            WHERE id=?
        ''', (
            data.get('school_id'), data.get('national_id'), data.get('name'), data.get('former_name'),
            data.get('sex'), data.get('enter_year'), data.get('campus'), data.get('current_class'),
            data.get('subject'), data.get('language_type'), data.get('category'), data.get('major'), data.get('at_school'),
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

@students_bp.route('/api/<int:student_id>', methods=['DELETE'])
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
        # 1. 读取工作表2并清洗表头
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file, dtype=str)
        else:
            try:
                df = pd.read_excel(file, sheet_name=1, dtype=str)
            except ValueError:
                return jsonify({"status": "error", "message": "读取失败：未找到工作表2，请严格使用模板！"}), 400

        # 清洗表头：去除空格和必填星号
        df.columns = [str(c).replace('*', '').strip() for c in df.columns]
        df.dropna(how='all', inplace=True)
        df = df.fillna('')
        records = df.to_dict('records')

        conn = sqlite3.connect(MAIN_DB_FILE, timeout=20.0)
        cursor = conn.cursor()
        log_conn = sqlite3.connect(LOG_DB_FILE, timeout=20.0)
        log_cursor = log_conn.cursor()

        # 2. 预加载现有识别码用于高速比对
        cursor.execute("SELECT custom_id, national_id, school_id FROM students")
        all_existing = cursor.fetchall()
        nid_to_cid = {row[1]: row[0] for row in all_existing if row[1]}
        sid_to_cid = {row[2]: row[0] for row in all_existing if row[2]}

        # ID 生成计数器逻辑
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
        insert_count, update_count, error_count = 0, 0, 0

        # 3. 遍历记录进行逻辑处理
        for row in records:
            name = str(row.get('姓名', '')).strip()
            if not name or name.lower() == 'nan':
                continue

            # 提取识别字段
            internal_id = str(row.get('内部编号', '')).strip()
            national_id = str(row.get('国家身份证号', '')).strip()
            school_id = str(row.get('省学籍辅号', '')).strip()

            # 提取新字段：外语种类 (language_type)
            language_type = str(row.get('外语种类', '英语')).strip()

            # 三级降维匹配逻辑
            target_custom_id = None
            if internal_id and internal_id.lower() not in ('nan', 'none'):
                target_custom_id = internal_id
            elif national_id and national_id in nid_to_cid:
                target_custom_id = nid_to_cid[national_id]
            elif school_id and school_id in sid_to_cid:
                target_custom_id = sid_to_cid[school_id]

            # 字段清洗
            db_national_id = national_id if national_id and national_id.lower() not in ('nan', 'none') else None
            db_school_id = school_id if school_id and school_id.lower() not in ('nan', 'none') else None
            enter_year = str(row.get('入学年份', '')).strip()
            campus_name = str(row.get('校区', '')).strip()

            # 公共数据元组（含 language_type）
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
                # 更新模式：注意 SQL 语句中的字段顺序需与元组对应
                update_data.append(common_values + (target_custom_id,))
                log_data.append((target_custom_id, '批量导入更新', f'通过Excel更新资料，语种设定为:{language_type}',
                                 current_time, current_editor))
                update_count += 1
            else:
                # 新增模式：自动生成内部编号
                year_part = enter_year[-2:] if len(enter_year) >= 2 else '00'

                campus_part = '0'
                if campus_name == '校本部':
                    campus_part = '91'
                elif campus_name == '礼贤校区':
                    campus_part = '81'
                else:
                    campus_part = '00'

                prefix = f"{year_part}{campus_part}"

                prefix_counters[prefix] = prefix_counters.get(prefix, 0) + 1
                final_custom_id = f"{prefix}{prefix_counters[prefix]:04d}"

                insert_data.append((final_custom_id, db_school_id, db_national_id) + common_values)
                log_data.append((final_custom_id, '批量导入新增', f'通过Excel导入新学生:{name}',
                                 current_time, current_editor))
                insert_count += 1

        # 4. 执行数据库事务
        try:
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
        except sqlite3.IntegrityError as e:
            conn.rollback()
            log_conn.rollback()
            error_count = len(insert_data) + len(update_data)
            insert_count = update_count = 0
            print(f"数据库冲突引发回滚: {e}")

        finally:
            conn.close()
            log_conn.close()

        return jsonify({
            "status": "success",
            "insert_count": insert_count,
            "update_count": update_count,
            "error_count": error_count
        })

    except Exception as e:
        import traceback
        print(f"导入失败堆栈: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": f"程序执行异常: {str(e)}"}), 500


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
        # ================= 高级模式逻辑 (添加为空/不为空支持) =================
        adv_conditions_str = request.args.get('adv_conditions', '[]')
        try:
            conditions = json.loads(adv_conditions_str)
        except Exception:
            conditions = []

        valid_fields = ['custom_id', 'school_id', 'national_id', 'name', 'former_name', 'sex',
                        'enter_year', 'campus', 'current_class', 'subject', 'category',
                        'major', 'at_school', 'boarding_status', 'apartment', 'dormitory',
                        'remarks', 'language_type']
        # 1. 扩展白名单，加入 'IS_EMPTY' 和 'IS_NOT_EMPTY'
        valid_ops = ['=', '!=', 'LIKE', 'IS_EMPTY', 'IS_NOT_EMPTY']

        if conditions:
            parts = []
            for cond in conditions:
                field = cond.get('field')
                op = cond.get('op')
                val = str(cond.get('value', '')).strip()

                # 2. 移除对 val 的统一硬性限制
                if field in valid_fields and op in valid_ops:
                    condition_str = ""
                    # 3. 针对不同操作符生成对应的 SQL 片段
                    if op == 'IS_EMPTY':
                        condition_str = f"({field} IS NULL OR {field} = '')"
                    elif op == 'IS_NOT_EMPTY':
                        condition_str = f"({field} IS NOT NULL AND {field} != '')"
                    elif val:  # 常规操作符必须有值
                        condition_str = f"{field} {op} ?"
                        params.append(f"%{val}%" if op == 'LIKE' else val)
                    else:
                        continue  # 无效条件直接跳过
                    # 动态判定是否需要逻辑连接词
                    logic = cond.get('logic', 'AND') if len(parts) > 0 else ""
                    if logic:
                        parts.append(logic)
                    parts.append(condition_str)
            # 只有当成功解析到有效参数时才追加 AND 块
            if parts:
                query += " AND (" + " ".join(parts) + ")"
    else:
        # ================= 普通模式逻辑 (此处为修复核心) =================
        # 1. 补全参数获取：必须与前端 params.append 的 Key 严格一致
        search = request.args.get('search', '').strip()
        campus = request.args.get('campus', '').strip()
        enter_year = request.args.get('enter_year', '').strip()  # ➕ 修复：获取年级
        current_class = request.args.get('current_class', '').strip()  # ➕ 修复：获取班级

        # 2. 补全 SQL 拼接
        if search:
            query += " AND (name LIKE ? OR custom_id LIKE ? OR national_id LIKE ?)"
            params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])

        # 需排除“全部”或空值，防止误触发过滤
        if campus and campus not in ('全部', ''):
            query += " AND campus = ?"
            params.append(campus)

        if enter_year and enter_year not in ('全部', ''):
            query += " AND enter_year = ?"
            params.append(enter_year)

        if current_class and current_class not in ('全部', ''):
            query += " AND current_class = ?"
            params.append(current_class)

    # 3. 执行导出
    query += " ORDER BY enter_year DESC, campus, current_class, custom_id"
    conn = sqlite3.connect(MAIN_DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    students_data = [dict(row) for row in rows]
    template_path = os.path.join(BASE_DIR, 'static', 'files', '学生信息批量修改模板.xlsx')

    try:
        excel_io = generate_students_query_excel(students_data, template_path)
        return send_file(
            excel_io,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='查询结果批量下载.xlsx'
        )
    except Exception as e:
        return jsonify({"status": "error", "message": f"导出失败: {str(e)}"}), 500