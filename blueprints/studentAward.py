import sqlite3
import os
import math
import pandas as pd
from datetime import datetime
from contextlib import closing
from flask import Blueprint, render_template, request, jsonify, session
from utils.decorators import login_required

# 定义蓝图
studentAward_bp = Blueprint('studentAward', __name__)

# ================= 动态路径解析 =================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN_DB_FILE = os.path.join(BASE_DIR, 'db', 'core', 'students_info.db')
LOG_DB_FILE = os.path.join(BASE_DIR, 'db', 'core', 'students_info_change_log.db')


# ================= 辅助函数 =================
def write_award_log(award_id, custom_id, action_type, details, editor):
    """将奖惩操作记录写入专属审计表"""
    if not details: return
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    with closing(sqlite3.connect(LOG_DB_FILE)) as conn:
        with conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO award_change_log (award_id, custom_id, action_type, details, timestamp, editor)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (award_id, custom_id, action_type, details, timestamp, editor))


# ================= 路由接口 =================

@studentAward_bp.route('/')
@login_required
def index():
    """渲染学生奖惩管理主页 (预留角色分流控制)"""
    role = session.get('role', 'teacher')

    # 预留的权限控制分支骨架
    if role == 'admin':
        # 管理员可以看全校数据
        pass
    elif role == 'teacher':
        # 普通教师可能只能看特定数据（视业务拓展而定）
        pass

    return render_template('studentAwardManagement/index.html')


@studentAward_bp.route('/api/query', methods=['POST'])
@login_required
def query_awards():
    """拉取奖惩列表 (执行与 students 底表的 LEFT JOIN 以获取真实姓名)"""
    data = request.json or {}
    page = int(data.get('page', 1))
    limit = int(data.get('limit', 20))
    offset = (page - 1) * limit

    keyword = data.get('keyword', '').strip()
    category = data.get('category', '').strip()
    level = data.get('level', '').strip()

    query_parts = []
    params = []

    if keyword:
        query_parts.append("(s.name LIKE ? OR a.custom_id LIKE ?)")
        params.extend([f"%{keyword}%", f"%{keyword}%"])
    if category:
        query_parts.append("a.award_category = ?")
        params.append(category)
    if level:
        query_parts.append("a.award_level = ?")
        params.append(level)

    where_clause = " WHERE " + " AND ".join(query_parts) if query_parts else ""

    base_sql = f"""
        FROM student_award a 
        LEFT JOIN students s ON a.custom_id = s.custom_id 
        {where_clause}
    """

    try:
        with closing(sqlite3.connect(MAIN_DB_FILE)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(f"SELECT COUNT(*) {base_sql}", params)
            total_count = cursor.fetchone()[0]

            cursor.execute(f"""
                SELECT a.*, s.name as student_name 
                {base_sql} 
                ORDER BY a.award_date DESC, a.id DESC 
                LIMIT ? OFFSET ?
            """, params + [limit, offset])

            rows = cursor.fetchall()

        total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
        return jsonify({
            "status": "success",
            "data": [dict(r) for r in rows],
            "total": total_count,
            "page": page,
            "total_pages": total_pages
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@studentAward_bp.route('/api/<int:award_id>', methods=['GET'])
@login_required
def get_single_award(award_id):
    """获取单条奖惩详情"""
    try:
        with closing(sqlite3.connect(MAIN_DB_FILE)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT a.*, s.name as student_name 
                FROM student_award a 
                LEFT JOIN students s ON a.custom_id = s.custom_id 
                WHERE a.id = ?
            """, (award_id,))
            record = cursor.fetchone()

            if not record:
                return jsonify({"status": "error", "message": "记录不存在"}), 404

        return jsonify({"status": "success", "data": dict(record)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@studentAward_bp.route('/api', methods=['POST'])
@login_required
def add_award():
    """新增单条奖惩记录"""
    data = request.json or {}
    current_editor = session.get('real_name', '未知操作者')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    custom_id = str(data.get('custom_id', '')).strip()

    try:
        with closing(sqlite3.connect(MAIN_DB_FILE)) as conn:
            with conn:
                cursor = conn.cursor()
                # 安全校验：防止强行给不存在的学生写入奖惩
                cursor.execute("SELECT id FROM students WHERE custom_id = ?", (custom_id,))
                if not cursor.fetchone():
                    return jsonify({"status": "error", "message": "该内部编号对应的学生不存在"}), 400

                cursor.execute('''
                    INSERT INTO student_award (
                        custom_id, award_date, award_category, award_level, 
                        award_name, description, issuing_authority, created_at, editor
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    custom_id, data.get('award_date'), data.get('award_category'),
                    data.get('award_level'), data.get('award_name'), data.get('description'),
                    data.get('issuing_authority'), current_time, current_editor
                ))
                new_id = cursor.lastrowid

        write_award_log(new_id, custom_id, '新增记录',
                        f"添加了[{data.get('award_category')}]: {data.get('award_name')}", current_editor)
        return jsonify({"status": "success", "message": "添加成功"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@studentAward_bp.route('/api/<int:award_id>', methods=['PUT'])
@login_required
def update_award(award_id):
    """更新奖惩记录"""
    data = request.json or {}
    current_editor = session.get('real_name', '未知操作者')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        with closing(sqlite3.connect(MAIN_DB_FILE)) as conn:
            with conn:
                cursor = conn.cursor()
                cursor.execute("SELECT custom_id FROM student_award WHERE id=?", (award_id,))
                old_record = cursor.fetchone()
                if not old_record:
                    return jsonify({"status": "error", "message": "记录不存在"}), 404

                custom_id = old_record[0]

                cursor.execute('''
                    UPDATE student_award SET 
                        award_date=?, award_category=?, award_level=?, award_name=?, 
                        description=?, issuing_authority=?, editor=?
                    WHERE id=?
                ''', (
                    data.get('award_date'), data.get('award_category'), data.get('award_level'),
                    data.get('award_name'), data.get('description'), data.get('issuing_authority'),
                    current_editor, award_id
                ))

        write_award_log(award_id, custom_id, '修改记录', f"更新了奖惩细节", current_editor)
        return jsonify({"status": "success", "message": "更新成功"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@studentAward_bp.route('/api/<int:award_id>', methods=['DELETE'])
@login_required
def delete_award(award_id):
    """删除奖惩记录"""
    current_editor = session.get('real_name', '未知操作者')
    try:
        with closing(sqlite3.connect(MAIN_DB_FILE)) as conn:
            with conn:
                cursor = conn.cursor()
                cursor.execute("SELECT custom_id, award_name FROM student_award WHERE id=?", (award_id,))
                record = cursor.fetchone()

                if record:
                    custom_id, award_name = record
                    cursor.execute("DELETE FROM student_award WHERE id=?", (award_id,))

        if record:
            write_award_log(award_id, custom_id, '删除记录', f"删除了: {award_name}", current_editor)

        return jsonify({"status": "success", "message": "删除成功"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@studentAward_bp.route('/api/import', methods=['POST'])
@login_required
def import_awards():
    """批量导入奖惩记录（搭载四维联合匹配与降级回退机制）"""
    if 'file' not in request.files:
        return jsonify({"status": "error", "message": "未上传文件"}), 400

    file = request.files['file']
    current_editor = session.get('real_name', '未知操作者')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        # 严格约束：尝试读取 Sheet2 (模板数据区)，若不存在则直接中止并报错
        try:
            df = pd.read_excel(file, sheet_name=1, dtype=str)
        except ValueError:
            return jsonify(
                {"status": "error", "message": "读取失败：未找到数据工作表(Sheet2)，请严格使用规定模板导入！"}), 400

        # 清洗表头：移除模板中强制必填的 '*' 号标识
        df.columns = [str(c).replace('*', '').strip() for c in df.columns]
        df.dropna(how='all', inplace=True)
        df = df.fillna('')

        insert_count, error_count = 0, 0
        log_data = []

        # 双库事务开启
        with closing(sqlite3.connect(MAIN_DB_FILE)) as conn, closing(sqlite3.connect(LOG_DB_FILE)) as log_conn:
            with conn, log_conn:
                cursor = conn.cursor()
                log_cursor = log_conn.cursor()

                # 1. 内存预加载全校学生基础信息，消除循环查询
                cursor.execute("SELECT custom_id, name, campus, enter_year, current_class FROM students")
                students_data = cursor.fetchall()

                valid_custom_ids = set()
                info_map = {}

                for row in students_data:
                    cid, s_name, s_campus, s_year, s_class = row
                    valid_custom_ids.add(cid)

                    # 构建四维联合主键字典
                    key = (str(s_name).strip(), str(s_campus).strip(), str(s_year).strip(), str(s_class).strip())
                    if key not in info_map:
                        info_map[key] = []
                    info_map[key].append(cid)

                # 2. 遍历数据并执行智能降维匹配
                for _, row in df.iterrows():
                    internal_id = str(row.get('内部编号', '')).strip()
                    name = str(row.get('姓名', '')).strip()
                    campus = str(row.get('校区', '')).strip()
                    enter_year = str(row.get('年级', '')).strip()
                    current_class = str(row.get('当前班级', '')).strip()

                    award_date = str(row.get('奖惩日期', '')).strip()
                    award_name = str(row.get('奖惩名称', '')).strip()
                    award_category = str(row.get('奖惩类别', '奖励')).strip()

                    if not name and not internal_id and not award_name:
                        continue  # 忽略彻头彻尾的空行

                    # ================= 核心：降级匹配逻辑 =================
                    target_cid = None
                    match_key = (name, campus, enter_year, current_class)

                    # 优先级 1：尝试通过 (姓名, 校区, 年级, 班级) 定位唯一学生
                    if match_key in info_map and len(info_map[match_key]) == 1:
                        target_cid = info_map[match_key][0]
                    else:
                        # 优先级 2：若存在重名或四维匹配失败，降级信任用户填写的“内部编号”
                        if internal_id and internal_id in valid_custom_ids:
                            target_cid = internal_id
                    # ======================================================

                    # 若依然无法确定学生归属，或业务核心字段缺失，拦截并计入失败
                    if not target_cid or not award_date or not award_name:
                        error_count += 1
                        continue

                    # 3. 单条执行插入以捕获自增 ID（在同一事务内极速完成）
                    cursor.execute('''
                        INSERT INTO student_award (
                            custom_id, award_date, award_category, award_level, 
                            award_name, description, issuing_authority, created_at, editor
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        target_cid, award_date, award_category,
                        str(row.get('奖惩级别', '')).strip(), award_name,
                        str(row.get('详细说明', '')).strip(), str(row.get('颁发单位', '')).strip(),
                        current_time, current_editor
                    ))

                    new_award_id = cursor.lastrowid

                    # 装载审计日志数组
                    log_data.append((
                        new_award_id, target_cid, '批量导入记录',
                        f"通过Excel导入[{award_category}]: {award_name}",
                        current_time, current_editor
                    ))
                    insert_count += 1

                # 4. 批量写入审计追踪记录
                if log_data:
                    log_cursor.executemany('''
                        INSERT INTO award_change_log (award_id, custom_id, action_type, details, timestamp, editor)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', log_data)

        return jsonify({
            "status": "success",
            "insert_count": insert_count,
            "error_count": error_count
        })

    except Exception as e:
        return jsonify({"status": "error", "message": f"处理失败: {str(e)}"}), 500