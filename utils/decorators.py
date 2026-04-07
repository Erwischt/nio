from functools import wraps
from flask import session, request, jsonify, redirect, url_for

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'uid' not in session:
            if request.path.startswith('/api/'):
                return jsonify({"status": "error", "message": "未登录或会话已过期，请重新登录"}), 401
            return redirect(url_for('auth.login_page', next=request.path))
        return f(*args, **kwargs)
    return decorated_function