import sqlite3
import os

# ================= 动态路径解析 =================
# 假设该脚本运行在项目根目录的 scripts/ 文件夹下
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCORES_DB_DIR = os.path.join(BASE_DIR, 'db', 'scores')

def init_scores_db(grade_year):
    """为指定年级初始化独立的成绩分库 (包含 6 张核心表)"""
    print(f"🚀 开始初始化 {grade_year} 级成绩数据库 (6表终极架构)...")
    
    # 确保目录存在
    os.makedirs(SCORES_DB_DIR, exist_ok=True)
    db_path = os.path.join(SCORES_DB_DIR, f'scores_{grade_year}.db')
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # ================= 1. 考试元数据表 =================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS exam_metadata (
            exam_id VARCHAR(50) PRIMARY KEY,
            exam_name VARCHAR(100) NOT NULL,
            exam_date VARCHAR(20) NOT NULL,
            semester VARCHAR(20),
            exam_type VARCHAR(20),
            creator VARCHAR(50),
            is_analyzed INTEGER DEFAULT 0 
        )
    ''')

    # ================= 2. 师资映射双子表：底座表 =================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stable_class_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            semester VARCHAR(20) NOT NULL,
            campus VARCHAR(20),
            class_type VARCHAR(20) NOT NULL,
            class_name VARCHAR(50) NOT NULL,
            subject VARCHAR(20) NOT NULL,
            teacher_name VARCHAR(50) NOT NULL,
            teacher_uid INTEGER NOT NULL
        )
    ''')

    # ================= 3. 师资映射双子表：快照表 =================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dynamic_class_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exam_id VARCHAR(50) NOT NULL,
            campus VARCHAR(20),
            class_type VARCHAR(20) NOT NULL,
            class_name VARCHAR(50) NOT NULL,
            subject VARCHAR(20) NOT NULL,
            teacher_name VARCHAR(50) NOT NULL,
            teacher_uid INTEGER NOT NULL,
            FOREIGN KEY (exam_id) REFERENCES exam_metadata(exam_id)
        )
    ''')

    # ================= 4. 学生走班关系底表 =================
    # 作用：记录学生在特定学期的行政班和各科教学班归属
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS student_class_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            semester VARCHAR(20) NOT NULL,
            custom_id CHAR(20) NOT NULL,
            student_name VARCHAR(50),
            admin_class VARCHAR(50),
            physics_class VARCHAR(50),
            chemistry_class VARCHAR(50),
            biology_class VARCHAR(50),
            politics_class VARCHAR(50),
            history_class VARCHAR(50),
            geography_class VARCHAR(50)
        )
    ''')

    # ================= 5. 原始分底表 =================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS students_raw_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exam_id VARCHAR(50) NOT NULL,
            custom_id CHAR(20) NOT NULL,
            chinese REAL,
            math REAL,
            english REAL,
            physics REAL,
            chemistry REAL,
            biology REAL,
            politics REAL,
            history REAL,
            geography REAL,
            total_score REAL,
            FOREIGN KEY (exam_id) REFERENCES exam_metadata(exam_id)
        )
    ''')

    # ================= 6. 进阶分析大宽表 =================
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS students_analyze_score (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            exam_id VARCHAR(50) NOT NULL,
            custom_id CHAR(20) NOT NULL,
            name VARCHAR(50),
            
            -- 双轨制班级归属 (从走班底表 Join 过来)
            admin_class VARCHAR(50),
            physics_class VARCHAR(50),
            chemistry_class VARCHAR(50),
            biology_class VARCHAR(50),
            politics_class VARCHAR(50),
            history_class VARCHAR(50),
            geography_class VARCHAR(50),
            
            -- 总分维度与目标划线
            total_score REAL,
            total_rank_school INTEGER,
            total_rank_class INTEGER,
            total_score_goal VARCHAR(50),
            
            -- 九大学科四维进阶数据
            chinese_raw REAL, chinese_assigned REAL, chinese_grade VARCHAR(10), chinese_rank_class INTEGER,
            math_raw REAL, math_assigned REAL, math_grade VARCHAR(10), math_rank_class INTEGER,
            english_raw REAL, english_assigned REAL, english_grade VARCHAR(10), english_rank_class INTEGER,
            
            physics_raw REAL, physics_assigned REAL, physics_grade VARCHAR(10), physics_rank_class INTEGER,
            chemistry_raw REAL, chemistry_assigned REAL, chemistry_grade VARCHAR(10), chemistry_rank_class INTEGER,
            biology_raw REAL, biology_assigned REAL, biology_grade VARCHAR(10), biology_rank_class INTEGER,
            
            politics_raw REAL, politics_assigned REAL, politics_grade VARCHAR(10), politics_rank_class INTEGER,
            history_raw REAL, history_assigned REAL, history_grade VARCHAR(10), history_rank_class INTEGER,
            geography_raw REAL, geography_assigned REAL, geography_grade VARCHAR(10), geography_rank_class INTEGER,
            
            FOREIGN KEY (exam_id) REFERENCES exam_metadata(exam_id)
        )
    ''')

    # ================= 7. 学生教学班九科全景矩阵大宽表 =================
    # 作用：用于绑定每个学生的定制编号与具体的九科教学班信息 (联合主键: custom_id, semester)
    cursor.execute('''
            CREATE TABLE IF NOT EXISTS stable_student_course_mapping (
                custom_id CHAR(20) NOT NULL,
                name VARCHAR(20),
                subject_track TEXT,
                chinese_class TEXT,
                math_class TEXT,
                english_class TEXT,
                physics_class TEXT,
                chemistry_class TEXT,
                biology_class TEXT,
                politics_class TEXT,
                history_class TEXT,
                geography_class TEXT,
                semester TEXT,
                PRIMARY KEY (custom_id)
            )
        ''')


    conn.commit()
    conn.close()
    print(f"✅ {grade_year} 级成绩分库已就绪: [{db_path}]\n")


if __name__ == '__main__':
    print("=== 校园数据平台：年级成绩分库初始化向导 ===")
    year = input("👉 请输入要初始化的年级 (例如 2024，直接回车取消): ").strip()
    if year:
        init_scores_db(year)
    else:
        print("操作已取消。")