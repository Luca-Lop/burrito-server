#!/usr/bin/env python3
"""
BurritoLauncher - Backend Server v2.0
Real-time chat + game platform API
Deploy on Render/Railway/PythonAnywhere
"""

import os, json, time, hashlib, secrets, sqlite3, threading
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, request, jsonify, g
from flask_cors import CORS
from flask_socketio import SocketIO, emit, join_room, leave_room

# ============================================================
# APP SETUP
# ============================================================
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "burrito-secret-change-me")

socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading",
                    ping_timeout=60, ping_interval=25)

DATABASE = os.environ.get("DB_PATH", "burrito.db")

# Track connected users for presence
connected_users = {}  # socket_id -> {"user_id", "username", "rooms"}

# ============================================================
# DATABASE
# ============================================================
def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA foreign_keys=ON")
    return g.db

@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db: db.close()

def get_db_direct():
    """For use outside request context (socketio events)."""
    db = sqlite3.connect(DATABASE)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db

def init_db():
    db = sqlite3.connect(DATABASE)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            display_name TEXT,
            avatar_color TEXT DEFAULT '#d4841a',
            bio TEXT DEFAULT '',
            xp INTEGER DEFAULT 0,
            level INTEGER DEFAULT 1,
            badges TEXT DEFAULT '["newcomer"]',
            settings TEXT DEFAULT '{}',
            created_at TEXT DEFAULT (datetime('now')),
            last_seen TEXT DEFAULT (datetime('now')),
            auth_token TEXT UNIQUE,
            status TEXT DEFAULT 'offline'
        );
        
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT UNIQUE NOT NULL,
            title TEXT NOT NULL,
            description TEXT DEFAULT '',
            version TEXT DEFAULT '1.0.0',
            author TEXT DEFAULT '',
            author_id INTEGER,
            tags TEXT DEFAULT '[]',
            category TEXT DEFAULT 'general',
            multiplayer INTEGER DEFAULT 0,
            controls INTEGER DEFAULT 0,
            controller_support INTEGER DEFAULT 0,
            micropython INTEGER DEFAULT 0,
            file_data TEXT DEFAULT '',
            download_url TEXT DEFAULT '',
            thumbnail_url TEXT DEFAULT '',
            total_plays INTEGER DEFAULT 0,
            rating REAL DEFAULT 0,
            added_at TEXT DEFAULT (datetime('now')),
            is_active INTEGER DEFAULT 1,
            FOREIGN KEY (author_id) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS user_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            game_id TEXT NOT NULL,
            total_plays INTEGER DEFAULT 0,
            total_playtime_seconds REAL DEFAULT 0,
            last_played TEXT,
            high_score REAL DEFAULT 0,
            custom_data TEXT DEFAULT '{}',
            updated_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id),
            UNIQUE(user_id, game_id)
        );
        
        CREATE TABLE IF NOT EXISTS play_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            game_id TEXT NOT NULL,
            started_at TEXT NOT NULL,
            duration_seconds REAL DEFAULT 0,
            score REAL DEFAULT 0,
            synced_from_offline INTEGER DEFAULT 0,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS play_log_weekly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            played_at TEXT DEFAULT (datetime('now')),
            user_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS friends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            friend_id INTEGER NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (friend_id) REFERENCES users(id),
            UNIQUE(user_id, friend_id)
        );
        
        CREATE TABLE IF NOT EXISTS chat_rooms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id TEXT UNIQUE NOT NULL,
            room_type TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT DEFAULT '',
            created_by INTEGER,
            game_id TEXT DEFAULT NULL,
            max_members INTEGER DEFAULT 50,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (created_by) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS chat_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            role TEXT DEFAULT 'member',
            joined_at TEXT DEFAULT (datetime('now')),
            UNIQUE(room_id, user_id)
        );
        
        CREATE TABLE IF NOT EXISTS chat_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            display_name TEXT DEFAULT '',
            avatar_color TEXT DEFAULT '#d4841a',
            message TEXT NOT NULL,
            message_type TEXT DEFAULT 'text',
            sent_at TEXT DEFAULT (datetime('now')),
            edited INTEGER DEFAULT 0,
            deleted INTEGER DEFAULT 0
        );
        
        CREATE TABLE IF NOT EXISTS user_library (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            game_id TEXT NOT NULL,
            installed INTEGER DEFAULT 0,
            added_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id),
            UNIQUE(user_id, game_id)
        );
        
        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            synced_at TEXT DEFAULT (datetime('now')),
            items_synced INTEGER DEFAULT 0,
            sync_type TEXT DEFAULT 'normal',
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        CREATE TABLE IF NOT EXISTS game_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT NOT NULL,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            file_content TEXT,
            file_size INTEGER DEFAULT 0,
            is_main INTEGER DEFAULT 0,
            uploaded_at TEXT DEFAULT (datetime('now'))
        );
    """)
    
    # Create default rooms
    default_rooms = [
        ("global_support", "support", "Support", "Get help with BurritoLauncher"),
        ("global_feedback", "feedback", "Feedback", "Share your ideas and feedback"),
        ("global_tricks", "community", "Tips & Tricks", "Share game tips and tricks"),
        ("global_records", "community", "Records & Completions", "Show off your achievements"),
    ]
    for room_id, rtype, name, desc in default_rooms:
        db.execute("""INSERT OR IGNORE INTO chat_rooms 
                      (room_id, room_type, name, description, created_by)
                      VALUES (?, ?, ?, ?, NULL)""",
                   (room_id, rtype, name, desc))
    
    db.commit()
    db.close()
    print("[DB] Database initialized with chat rooms")


# ============================================================
# AUTH HELPERS
# ============================================================
def hash_pw(pw):
    salt = app.config["SECRET_KEY"].encode()
    return hashlib.pbkdf2_hmac("sha256", pw.encode(), salt, 100000).hex()

def gen_token():
    return secrets.token_hex(32)

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            return jsonify({"error": "No auth token"}), 401
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE auth_token = ?", (token,)).fetchone()
        if not user:
            return jsonify({"error": "Invalid token"}), 401
        db.execute("UPDATE users SET last_seen = datetime('now') WHERE id = ?", (user["id"],))
        db.commit()
        g.current_user = dict(user)
        return f(*args, **kwargs)
    return decorated

def get_user_from_token(token):
    """For socketio auth."""
    if not token: return None
    db = get_db_direct()
    user = db.execute("SELECT * FROM users WHERE auth_token = ?", (token,)).fetchone()
    db.close()
    return dict(user) if user else None


# ============================================================
# ROUTES - HEALTH
# ============================================================
@app.route("/")
def index():
    return jsonify({"name": "BurritoLauncher API", "version": "2.0.0",
                    "status": "online", "chat": "socketio",
                    "time": datetime.utcnow().isoformat()})

@app.route("/api/ping")
def ping():
    return jsonify({"pong": True, "time": datetime.utcnow().isoformat()})


# ============================================================
# ROUTES - AUTH
# ============================================================
@app.route("/api/auth/register", methods=["POST"])
def register():
    data = request.get_json() or {}
    username = data.get("username", "").strip().lower()
    password = data.get("password", "")
    display_name = data.get("display_name", username)
    avatar_color = data.get("avatar_color", "#d4841a")
    
    if len(username) < 3: return jsonify({"error": "Username must be 3+ characters"}), 400
    if len(username) > 20: return jsonify({"error": "Username too long"}), 400
    if not username.replace("_","").isalnum(): return jsonify({"error": "Username: letters, numbers, underscores only"}), 400
    if len(password) < 4: return jsonify({"error": "Password must be 4+ characters"}), 400
    
    db = get_db()
    if db.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone():
        return jsonify({"error": "Username taken"}), 409
    
    token = gen_token()
    db.execute("""INSERT INTO users (username, password_hash, display_name, avatar_color, auth_token)
                  VALUES (?, ?, ?, ?, ?)""",
               (username, hash_pw(password), display_name, avatar_color, token))
    db.commit()
    
    user = db.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    user_dict = dict(user)
    
    # Auto-join global rooms
    for room_id in ["global_support", "global_feedback", "global_tricks", "global_records"]:
        db.execute("INSERT OR IGNORE INTO chat_members (room_id, user_id) VALUES (?, ?)",
                   (room_id, user_dict["id"]))
    db.commit()
    
    return jsonify({
        "success": True,
        "user": {
            "id": user_dict["id"], "username": user_dict["username"],
            "display_name": user_dict["display_name"],
            "avatar_color": user_dict["avatar_color"],
            "token": token, "xp": 0, "level": 1,
            "badges": json.loads(user_dict["badges"]),
            "bio": "", "created_at": user_dict["created_at"]
        }
    }), 201

@app.route("/api/auth/login", methods=["POST"])
def login():
    data = request.get_json() or {}
    username = data.get("username", "").strip().lower()
    password = data.get("password", "")
    
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    if not user or user["password_hash"] != hash_pw(password):
        return jsonify({"error": "Invalid username or password"}), 401
    
    token = gen_token()
    db.execute("UPDATE users SET auth_token = ?, last_seen = datetime('now'), status = 'online' WHERE id = ?",
               (token, user["id"]))
    db.commit()
    
    user_dict = dict(user)
    return jsonify({
        "success": True,
        "user": {
            "id": user_dict["id"], "username": user_dict["username"],
            "display_name": user_dict["display_name"],
            "avatar_color": user_dict["avatar_color"],
            "bio": user_dict["bio"], "xp": user_dict["xp"],
            "level": user_dict["level"],
            "badges": json.loads(user_dict["badges"]),
            "token": token, "created_at": user_dict["created_at"]
        }
    })

@app.route("/api/auth/me")
@require_auth
def get_me():
    u = g.current_user
    return jsonify({
        "id": u["id"], "username": u["username"],
        "display_name": u["display_name"], "avatar_color": u["avatar_color"],
        "bio": u["bio"], "xp": u["xp"], "level": u["level"],
        "badges": json.loads(u["badges"]), "status": u["status"],
        "created_at": u["created_at"], "last_seen": u["last_seen"]
    })

@app.route("/api/auth/profile", methods=["PUT"])
@require_auth
def update_profile():
    data = request.get_json() or {}
    db = get_db()
    uid = g.current_user["id"]
    
    allowed = ["display_name", "avatar_color", "bio"]
    updates = []
    values = []
    for key in allowed:
        if key in data:
            updates.append(f"{key} = ?")
            values.append(data[key])
    
    if updates:
        values.append(uid)
        db.execute(f"UPDATE users SET {', '.join(updates)} WHERE id = ?", values)
        db.commit()
    
    return jsonify({"success": True})

@app.route("/api/auth/logout", methods=["POST"])
@require_auth
def logout():
    db = get_db()
    db.execute("UPDATE users SET auth_token = NULL, status = 'offline' WHERE id = ?",
               (g.current_user["id"],))
    db.commit()
    return jsonify({"success": True})


# ============================================================
# ROUTES - FRIENDS
# ============================================================
@app.route("/api/friends")
@require_auth
def get_friends():
    db = get_db()
    uid = g.current_user["id"]
    
    friends = db.execute("""
        SELECT u.id, u.username, u.display_name, u.avatar_color, 
               u.level, u.status, u.last_seen, f.status as friend_status
        FROM friends f
        JOIN users u ON (CASE WHEN f.user_id = ? THEN f.friend_id ELSE f.user_id END) = u.id
        WHERE (f.user_id = ? OR f.friend_id = ?) AND f.status = 'accepted'
    """, (uid, uid, uid)).fetchall()
    
    return jsonify({"friends": [dict(f) for f in friends]})

@app.route("/api/friends/requests")
@require_auth
def get_friend_requests():
    db = get_db()
    uid = g.current_user["id"]
    
    incoming = db.execute("""
        SELECT u.id, u.username, u.display_name, u.avatar_color, u.level, f.created_at
        FROM friends f JOIN users u ON f.user_id = u.id
        WHERE f.friend_id = ? AND f.status = 'pending'
    """, (uid,)).fetchall()
    
    outgoing = db.execute("""
        SELECT u.id, u.username, u.display_name, u.avatar_color, u.level, f.created_at
        FROM friends f JOIN users u ON f.friend_id = u.id
        WHERE f.user_id = ? AND f.status = 'pending'
    """, (uid,)).fetchall()
    
    return jsonify({
        "incoming": [dict(r) for r in incoming],
        "outgoing": [dict(r) for r in outgoing]
    })

@app.route("/api/friends/add", methods=["POST"])
@require_auth
def add_friend():
    data = request.get_json() or {}
    username = data.get("username", "").strip().lower()
    
    db = get_db()
    uid = g.current_user["id"]
    
    other = db.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
    if not other: return jsonify({"error": "User not found"}), 404
    if other["id"] == uid: return jsonify({"error": "Cannot add yourself"}), 400
    
    existing = db.execute(
        "SELECT * FROM friends WHERE (user_id=? AND friend_id=?) OR (user_id=? AND friend_id=?)",
        (uid, other["id"], other["id"], uid)).fetchone()
    
    if existing:
        if existing["status"] == "accepted":
            return jsonify({"error": "Already friends"}), 409
        if existing["status"] == "pending":
            # If they sent us a request, accept it
            if existing["friend_id"] == uid:
                db.execute("UPDATE friends SET status = 'accepted' WHERE id = ?", (existing["id"],))
                db.commit()
                # Notify via socketio
                socketio.emit("friend_accepted", {
                    "username": g.current_user["username"],
                    "display_name": g.current_user["display_name"]
                }, room=f"user_{other['id']}")
                return jsonify({"success": True, "message": "Friend request accepted!"})
            return jsonify({"error": "Request already sent"}), 409
    
    db.execute("INSERT INTO friends (user_id, friend_id, status) VALUES (?, ?, 'pending')",
               (uid, other["id"]))
    db.commit()
    
    # Notify via socketio
    socketio.emit("friend_request", {
        "from_username": g.current_user["username"],
        "from_display_name": g.current_user["display_name"],
        "from_avatar_color": g.current_user["avatar_color"]
    }, room=f"user_{other['id']}")
    
    return jsonify({"success": True, "message": "Friend request sent!"})

@app.route("/api/friends/remove", methods=["POST"])
@require_auth
def remove_friend():
    data = request.get_json() or {}
    username = data.get("username", "").strip().lower()
    
    db = get_db()
    uid = g.current_user["id"]
    other = db.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
    if not other: return jsonify({"error": "User not found"}), 404
    
    db.execute("DELETE FROM friends WHERE (user_id=? AND friend_id=?) OR (user_id=? AND friend_id=?)",
               (uid, other["id"], other["id"], uid))
    db.commit()
    return jsonify({"success": True})

@app.route("/api/users/search")
@require_auth
def search_users():
    query = request.args.get("q", "").strip().lower()
    if len(query) < 2: return jsonify({"users": []})
    
    db = get_db()
    users = db.execute("""
        SELECT id, username, display_name, avatar_color, level, status
        FROM users WHERE username LIKE ? OR display_name LIKE ? LIMIT 20
    """, (f"%{query}%", f"%{query}%")).fetchall()
    
    return jsonify({"users": [dict(u) for u in users]})


# ============================================================
# ROUTES - GAMES
# ============================================================
@app.route("/api/games")
def list_games():
    db = get_db()
    games = db.execute(
        "SELECT * FROM games WHERE is_active = 1 ORDER BY total_plays DESC"
    ).fetchall()
    result = []
    for g_row in games:
        gd = dict(g_row)
        gd["tags"] = json.loads(gd.get("tags", "[]"))
        result.append(gd)
    return jsonify({"games": result})

@app.route("/api/games/trending")
def trending_games():
    """Games with most plays in last 7 days."""
    db = get_db()
    week_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()
    
    trending = db.execute("""
        SELECT g.*, COUNT(p.id) as weekly_plays
        FROM games g
        LEFT JOIN play_log_weekly p ON g.game_id = p.game_id AND p.played_at > ?
        WHERE g.is_active = 1
        GROUP BY g.game_id
        ORDER BY weekly_plays DESC
        LIMIT 50
    """, (week_ago,)).fetchall()
    
    result = []
    for row in trending:
        d = dict(row)
        d["tags"] = json.loads(d.get("tags", "[]"))
        result.append(d)
    
    return jsonify({"trending": result, "period": "7_days"})

@app.route("/api/games/<game_id>")
def get_game(game_id):
    db = get_db()
    game = db.execute("SELECT * FROM games WHERE game_id = ?", (game_id,)).fetchone()
    if not game: return jsonify({"error": "Game not found"}), 404
    gd = dict(game)
    gd["tags"] = json.loads(gd.get("tags", "[]"))
    
    # Get game files
    files = db.execute("SELECT file_name, is_main FROM game_files WHERE game_id = ?",
                       (game_id,)).fetchall()
    gd["files"] = [dict(f) for f in files]
    
    return jsonify(gd)

@app.route("/api/games", methods=["POST"])
@require_auth
def post_game():
    """Post a new game with its file contents."""
    data = request.get_json() or {}
    
    game_id = data.get("game_id", "").strip().lower().replace(" ", "_")
    title = data.get("title", "").strip()
    if not game_id or not title:
        return jsonify({"error": "game_id and title required"}), 400
    
    db = get_db()
    uid = g.current_user["id"]
    
    # Check if game exists
    existing = db.execute("SELECT id FROM games WHERE game_id = ?", (game_id,)).fetchone()
    if existing:
        return jsonify({"error": "A game with this ID already exists"}), 409
    
    tags = json.dumps(data.get("tags", []))
    
    db.execute("""INSERT INTO games 
        (game_id, title, description, version, author, author_id, tags, category,
         multiplayer, controls, controller_support, micropython)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (game_id, title, data.get("description", ""), data.get("version", "1.0"),
         g.current_user["display_name"], uid, tags,
         data.get("category", "general"),
         1 if data.get("multiplayer") else 0,
         1 if data.get("controls") else 0,
         1 if data.get("controller_support") else 0,
         1 if data.get("micropython") else 0))
    
    # Store game files if provided
    files = data.get("files", {})
    main_file = data.get("main_file", "")
    
    for fname, content in files.items():
        is_main = 1 if fname == main_file else 0
        db.execute("""INSERT INTO game_files 
            (game_id, file_name, file_path, file_content, file_size, is_main)
            VALUES (?, ?, ?, ?, ?, ?)""",
            (game_id, fname, fname, content, len(content), is_main))
    
    # Update user badges and XP
    badges = json.loads(g.current_user["badges"])
    if "creator" not in badges:
        badges.append("creator")
        db.execute("UPDATE users SET badges = ? WHERE id = ?", (json.dumps(badges), uid))
    db.execute("UPDATE users SET xp = xp + 50 WHERE id = ?", (uid,))
    
    db.commit()
    
    return jsonify({"success": True, "game_id": game_id}), 201

@app.route("/api/games/<game_id>/files")
def get_game_files(game_id):
    """Get all files for a game (for streaming play)."""
    db = get_db()
    files = db.execute(
        "SELECT file_name, file_content, is_main FROM game_files WHERE game_id = ?",
        (game_id,)).fetchall()
    
    if not files:
        return jsonify({"error": "No files found for this game"}), 404
    
    result = {}
    main_file = ""
    for f in files:
        result[f["file_name"]] = f["file_content"]
        if f["is_main"]:
            main_file = f["file_name"]
    
    return jsonify({"game_id": game_id, "files": result, "main_file": main_file})


# ============================================================
# ROUTES - STATS
# ============================================================
@app.route("/api/stats")
@require_auth
def get_stats():
    db = get_db()
    stats = db.execute(
        "SELECT * FROM user_stats WHERE user_id = ? ORDER BY last_played DESC",
        (g.current_user["id"],)).fetchall()
    result = {}
    for s in stats:
        sd = dict(s)
        sd["custom_data"] = json.loads(sd.get("custom_data", "{}"))
        result[sd["game_id"]] = sd
    return jsonify({"stats": result})

@app.route("/api/stats/<game_id>/play", methods=["POST"])
@require_auth
def record_play(game_id):
    data = request.get_json() or {}
    db = get_db()
    uid = g.current_user["id"]
    duration = data.get("duration_seconds", 0)
    score = data.get("score", 0)
    
    # Log session
    db.execute("""INSERT INTO play_sessions 
        (user_id, game_id, started_at, duration_seconds, score, synced_from_offline)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (uid, game_id, data.get("started_at", datetime.utcnow().isoformat()),
         duration, score, 1 if data.get("from_offline") else 0))
    
    # Weekly log for trending
    db.execute("INSERT INTO play_log_weekly (game_id, user_id) VALUES (?, ?)",
               (game_id, uid))
    
    # Update user stats
    existing = db.execute(
        "SELECT * FROM user_stats WHERE user_id = ? AND game_id = ?",
        (uid, game_id)).fetchone()
    
    if existing:
        db.execute("""UPDATE user_stats SET 
            total_plays = total_plays + 1,
            total_playtime_seconds = total_playtime_seconds + ?,
            last_played = datetime('now'),
            high_score = MAX(high_score, ?),
            updated_at = datetime('now')
            WHERE user_id = ? AND game_id = ?""",
            (duration, score, uid, game_id))
    else:
        db.execute("""INSERT INTO user_stats 
            (user_id, game_id, total_plays, total_playtime_seconds, last_played, high_score)
            VALUES (?, ?, 1, ?, datetime('now'), ?)""",
            (uid, game_id, duration, score))
    
    # Update global play count
    db.execute("UPDATE games SET total_plays = total_plays + 1 WHERE game_id = ?",
               (game_id,))
    
    # XP
    db.execute("UPDATE users SET xp = xp + 5, level = 1 + (xp + 5) / 100 WHERE id = ?", (uid,))
    
    db.commit()
    return jsonify({"success": True})

@app.route("/api/sync", methods=["POST"])
@require_auth
def sync_offline():
    data = request.get_json() or {}
    db = get_db()
    uid = g.current_user["id"]
    synced = 0
    
    for session in data.get("play_sessions", []):
        game_id = session.get("game_id", "")
        plays = session.get("plays", 1)
        playtime = session.get("playtime_seconds", 0)
        score = session.get("score", 0)
        
        for _ in range(plays):
            db.execute("""INSERT INTO play_sessions 
                (user_id, game_id, started_at, duration_seconds, score, synced_from_offline)
                VALUES (?, ?, ?, ?, ?, 1)""",
                (uid, game_id, session.get("started_at", datetime.utcnow().isoformat()),
                 playtime / max(plays, 1), score))
            db.execute("INSERT INTO play_log_weekly (game_id, user_id) VALUES (?, ?)",
                       (game_id, uid))
        
        existing = db.execute(
            "SELECT * FROM user_stats WHERE user_id = ? AND game_id = ?",
            (uid, game_id)).fetchone()
        
        if existing:
            db.execute("""UPDATE user_stats SET 
                total_plays = total_plays + ?, total_playtime_seconds = total_playtime_seconds + ?,
                last_played = datetime('now'), high_score = MAX(high_score, ?),
                updated_at = datetime('now')
                WHERE user_id = ? AND game_id = ?""",
                (plays, playtime, score, uid, game_id))
        else:
            db.execute("""INSERT INTO user_stats 
                (user_id, game_id, total_plays, total_playtime_seconds, last_played, high_score)
                VALUES (?, ?, ?, ?, datetime('now'), ?)""",
                (uid, game_id, plays, playtime, score))
        
        db.execute("UPDATE games SET total_plays = total_plays + ? WHERE game_id = ?",
                   (plays, game_id))
        synced += plays
    
    db.execute("INSERT INTO sync_log (user_id, items_synced, sync_type) VALUES (?, ?, 'offline')",
               (uid, synced))
    db.commit()
    return jsonify({"success": True, "items_synced": synced})

@app.route("/api/leaderboard/<game_id>")
def leaderboard(game_id):
    db = get_db()
    rows = db.execute("""
        SELECT u.username, u.display_name, u.avatar_color, 
               us.total_plays, us.high_score, us.total_playtime_seconds
        FROM user_stats us JOIN users u ON us.user_id = u.id
        WHERE us.game_id = ? ORDER BY us.high_score DESC LIMIT 50
    """, (game_id,)).fetchall()
    return jsonify({"game_id": game_id, "leaderboard": [dict(r) for r in rows]})


# ============================================================
# ROUTES - CHAT (REST endpoints for history)
# ============================================================
@app.route("/api/chat/rooms")
@require_auth
def get_chat_rooms():
    db = get_db()
    uid = g.current_user["id"]
    
    # Rooms user is a member of
    rooms = db.execute("""
        SELECT cr.*, cm.role,
            (SELECT COUNT(*) FROM chat_members WHERE room_id = cr.room_id) as member_count,
            (SELECT message FROM chat_messages WHERE room_id = cr.room_id 
             ORDER BY id DESC LIMIT 1) as last_message,
            (SELECT sent_at FROM chat_messages WHERE room_id = cr.room_id 
             ORDER BY id DESC LIMIT 1) as last_message_time
        FROM chat_rooms cr
        JOIN chat_members cm ON cr.room_id = cm.room_id AND cm.user_id = ?
        WHERE cr.is_active = 1
        ORDER BY last_message_time DESC
    """, (uid,)).fetchall()
    
    return jsonify({"rooms": [dict(r) for r in rooms]})

@app.route("/api/chat/rooms", methods=["POST"])
@require_auth
def create_chat_room():
    data = request.get_json() or {}
    name = data.get("name", "").strip()
    room_type = data.get("type", "party")
    game_id = data.get("game_id")
    
    if not name: return jsonify({"error": "Room name required"}), 400
    
    db = get_db()
    uid = g.current_user["id"]
    
    room_id = f"{room_type}_{uid}_{int(time.time())}"
    if game_id:
        room_id = f"game_{game_id}"
    
    # Check if game room already exists
    if game_id:
        existing = db.execute("SELECT room_id FROM chat_rooms WHERE game_id = ?",
                             (game_id,)).fetchone()
        if existing:
            # Just join it
            db.execute("INSERT OR IGNORE INTO chat_members (room_id, user_id, role) VALUES (?, ?, 'member')",
                       (existing["room_id"], uid))
            db.commit()
            return jsonify({"success": True, "room_id": existing["room_id"]})
    
    db.execute("""INSERT INTO chat_rooms 
        (room_id, room_type, name, description, created_by, game_id)
        VALUES (?, ?, ?, ?, ?, ?)""",
        (room_id, room_type, name, data.get("description", ""), uid, game_id))
    
    # Creator is admin
    db.execute("INSERT INTO chat_members (room_id, user_id, role) VALUES (?, ?, 'admin')",
               (room_id, uid))
    
    # Add invited members
    for invite_username in data.get("invite", []):
        invited = db.execute("SELECT id FROM users WHERE username = ?",
                            (invite_username.strip().lower(),)).fetchone()
        if invited:
            db.execute("INSERT OR IGNORE INTO chat_members (room_id, user_id) VALUES (?, ?)",
                       (room_id, invited["id"]))
            # Notify
            socketio.emit("room_invite", {
                "room_id": room_id, "room_name": name,
                "invited_by": g.current_user["display_name"]
            }, room=f"user_{invited['id']}")
    
    db.commit()
    return jsonify({"success": True, "room_id": room_id}), 201

@app.route("/api/chat/<room_id>/messages")
@require_auth
def get_messages(room_id):
    db = get_db()
    uid = g.current_user["id"]
    
    # Check membership
    member = db.execute(
        "SELECT * FROM chat_members WHERE room_id = ? AND user_id = ?",
        (room_id, uid)).fetchone()
    
    # Allow access to global rooms
    if not member and not room_id.startswith("global_"):
        return jsonify({"error": "Not a member of this room"}), 403
    
    limit = request.args.get("limit", 100, type=int)
    before_id = request.args.get("before", None, type=int)
    
    if before_id:
        messages = db.execute("""
            SELECT * FROM chat_messages 
            WHERE room_id = ? AND id < ? AND deleted = 0
            ORDER BY id DESC LIMIT ?
        """, (room_id, before_id, limit)).fetchall()
    else:
        messages = db.execute("""
            SELECT * FROM chat_messages 
            WHERE room_id = ? AND deleted = 0
            ORDER BY id DESC LIMIT ?
        """, (room_id, limit)).fetchall()
    
    return jsonify({"messages": [dict(m) for m in reversed(messages)]})

@app.route("/api/chat/<room_id>/invite", methods=["POST"])
@require_auth
def invite_to_room(room_id):
    data = request.get_json() or {}
    username = data.get("username", "").strip().lower()
    
    db = get_db()
    uid = g.current_user["id"]
    
    # Check if inviter is member
    member = db.execute(
        "SELECT role FROM chat_members WHERE room_id = ? AND user_id = ?",
        (room_id, uid)).fetchone()
    if not member:
        return jsonify({"error": "You are not in this room"}), 403
    
    invited = db.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
    if not invited:
        return jsonify({"error": "User not found"}), 404
    
    db.execute("INSERT OR IGNORE INTO chat_members (room_id, user_id) VALUES (?, ?)",
               (room_id, invited["id"]))
    db.commit()
    
    room = db.execute("SELECT name FROM chat_rooms WHERE room_id = ?", (room_id,)).fetchone()
    socketio.emit("room_invite", {
        "room_id": room_id,
        "room_name": room["name"] if room else room_id,
        "invited_by": g.current_user["display_name"]
    }, room=f"user_{invited['id']}")
    
    return jsonify({"success": True})

@app.route("/api/chat/game/<game_id>")
@require_auth
def get_or_create_game_chat(game_id):
    """Get or create a chat room for a specific game."""
    db = get_db()
    uid = g.current_user["id"]
    
    room = db.execute("SELECT * FROM chat_rooms WHERE game_id = ?", (game_id,)).fetchone()
    
    if not room:
        game = db.execute("SELECT title FROM games WHERE game_id = ?", (game_id,)).fetchone()
        game_name = game["title"] if game else game_id
        
        room_id = f"game_{game_id}"
        db.execute("""INSERT INTO chat_rooms 
            (room_id, room_type, name, description, game_id)
            VALUES (?, 'game', ?, ?, ?)""",
            (room_id, f"{game_name} Chat", f"Discussion for {game_name}", game_id))
        db.commit()
        room = db.execute("SELECT * FROM chat_rooms WHERE room_id = ?", (room_id,)).fetchone()
    
    # Auto-join
    db.execute("INSERT OR IGNORE INTO chat_members (room_id, user_id) VALUES (?, ?)",
               (room["room_id"], uid))
    db.commit()
    
    return jsonify(dict(room))


# ============================================================
# SOCKETIO - REAL-TIME CHAT
# ============================================================
@socketio.on("connect")
def handle_connect():
    token = request.args.get("token", "")
    user = get_user_from_token(token)
    
    if not user:
        return False  # Reject connection
    
    connected_users[request.sid] = {
        "user_id": user["id"],
        "username": user["username"],
        "display_name": user["display_name"],
        "avatar_color": user["avatar_color"],
        "rooms": set()
    }
    
    # Join personal room for notifications
    join_room(f"user_{user['id']}")
    
    # Update status
    db = get_db_direct()
    db.execute("UPDATE users SET status = 'online' WHERE id = ?", (user["id"],))
    db.commit()
    db.close()
    
    print(f"[Chat] {user['username']} connected")

@socketio.on("disconnect")
def handle_disconnect():
    user_data = connected_users.pop(request.sid, None)
    if user_data:
        db = get_db_direct()
        db.execute("UPDATE users SET status = 'offline', last_seen = datetime('now') WHERE id = ?",
                   (user_data["user_id"],))
        db.commit()
        db.close()
        
        # Notify rooms
        for room_id in user_data.get("rooms", set()):
            emit("user_left", {
                "username": user_data["username"],
                "display_name": user_data["display_name"]
            }, room=room_id)
        
        print(f"[Chat] {user_data['username']} disconnected")

@socketio.on("join_room")
def handle_join_room(data):
    room_id = data.get("room_id", "")
    user_data = connected_users.get(request.sid)
    if not user_data: return
    
    join_room(room_id)
    user_data["rooms"].add(room_id)
    
    emit("user_joined", {
        "username": user_data["username"],
        "display_name": user_data["display_name"],
        "avatar_color": user_data["avatar_color"]
    }, room=room_id)
    
    # Send current online users in room
    online_in_room = []
    for sid, ud in connected_users.items():
        if room_id in ud.get("rooms", set()):
            online_in_room.append({
                "username": ud["username"],
                "display_name": ud["display_name"],
                "avatar_color": ud["avatar_color"]
            })
    emit("room_users", {"room_id": room_id, "users": online_in_room})

@socketio.on("leave_room")
def handle_leave_room(data):
    room_id = data.get("room_id", "")
    user_data = connected_users.get(request.sid)
    if not user_data: return
    
    leave_room(room_id)
    user_data["rooms"].discard(room_id)
    
    emit("user_left", {
        "username": user_data["username"],
        "display_name": user_data["display_name"]
    }, room=room_id)

@socketio.on("send_message")
def handle_send_message(data):
    room_id = data.get("room_id", "")
    message = data.get("message", "").strip()
    msg_type = data.get("type", "text")
    
    user_data = connected_users.get(request.sid)
    if not user_data or not message: return
    
    # Rate limit: max 5 messages per second
    # (simple check, production would use Redis)
    
    # Save to database
    db = get_db_direct()
    cursor = db.execute("""INSERT INTO chat_messages 
        (room_id, user_id, username, display_name, avatar_color, message, message_type)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (room_id, user_data["user_id"], user_data["username"],
         user_data["display_name"], user_data["avatar_color"],
         message, msg_type))
    msg_id = cursor.lastrowid
    db.commit()
    db.close()
    
    # Broadcast to room
    emit("new_message", {
        "id": msg_id,
        "room_id": room_id,
        "user_id": user_data["user_id"],
        "username": user_data["username"],
        "display_name": user_data["display_name"],
        "avatar_color": user_data["avatar_color"],
        "message": message,
        "type": msg_type,
        "sent_at": datetime.utcnow().isoformat()
    }, room=room_id)

@socketio.on("typing")
def handle_typing(data):
    room_id = data.get("room_id", "")
    user_data = connected_users.get(request.sid)
    if not user_data: return
    
    emit("user_typing", {
        "username": user_data["username"],
        "display_name": user_data["display_name"]
    }, room=room_id, include_self=False)

@socketio.on("delete_message")
def handle_delete_message(data):
    msg_id = data.get("message_id")
    user_data = connected_users.get(request.sid)
    if not user_data or not msg_id: return
    
    db = get_db_direct()
    msg = db.execute("SELECT * FROM chat_messages WHERE id = ?", (msg_id,)).fetchone()
    if msg and msg["user_id"] == user_data["user_id"]:
        db.execute("UPDATE chat_messages SET deleted = 1 WHERE id = ?", (msg_id,))
        db.commit()
        emit("message_deleted", {"message_id": msg_id, "room_id": msg["room_id"]},
             room=msg["room_id"])
    db.close()


# ============================================================
# CLEANUP - Delete old weekly play logs (older than 8 days)
# ============================================================
def cleanup_old_data():
    """Run periodically to clean old trending data."""
    while True:
        try:
            db = get_db_direct()
            cutoff = (datetime.utcnow() - timedelta(days=8)).isoformat()
            db.execute("DELETE FROM play_log_weekly WHERE played_at < ?", (cutoff,))
            db.commit()
            db.close()
        except:
            pass
        time.sleep(3600)  # Every hour

# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    init_db()
    
    # Start cleanup thread
    cleanup_thread = threading.Thread(target=cleanup_old_data, daemon=True)
    cleanup_thread.start()
    
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"
    
    print(f"[Server] BurritoLauncher API v2.0 starting on port {port}")
    print(f"[Server] Chat: SocketIO enabled")
    
    socketio.run(app, host="0.0.0.0", port=port, debug=debug,
                 allow_unsafe_werkzeug=True)
