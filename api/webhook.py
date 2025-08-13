# api/webhook.py - Refined to match simple DB and handle new users
from http.server import BaseHTTPRequestHandler
import json
from datetime import datetime, timezone, timedelta
import os
import requests
import psycopg
import urllib.parse

# --- Configuration ---
POSTGRES_URL = os.environ.get('POSTGRES_URL')
LINE_TOKEN = os.environ.get('LINE_TOKEN', '')
THAILAND_TZ = timezone(timedelta(hours=7))

# --- Database Functions ---
def get_db_connection():
    if not POSTGRES_URL: return None
    try:
        return psycopg.connect(POSTGRES_URL)
    except psycopg.OperationalError:
        return None

def get_user_name(user_id):
    """Gets the registered name (e.g., ward name) for a given LINE user_id."""
    conn = get_db_connection()
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM ward_id WHERE user_id = %s", (user_id,))
            result = cur.fetchone()
            return result[0] if result else None
    finally:
        if conn: conn.close()

# --- Main HTTP Handler ---
class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            webhook_data = json.loads(post_data.decode('utf-8'))
            print(f"🔥 Webhook received: {datetime.now(THAILAND_TZ).isoformat()}")

            for event in webhook_data.get('events', []):
                process_event(event)
        except Exception as e:
            print(f"❌ Unhandled Error in do_POST: {e}")
        finally:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'OK')

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        db_status = "✅ Connected" if get_db_connection() else "❌ Connection Failed"
        self.wfile.write(json.dumps({"status": "Webhook is running", "database_status": db_status}).encode('utf-8'))

# --- Event Processing Logic ---
def process_event(event):
    user_id = event.get('source', {}).get('userId')
    if not user_id: return

    event_type = event.get('type')
    print(f"Processing event '{event_type}' for user {user_id}")
    
    # Update user activity record
    handle_user_event(user_id, event_type)
    
    if event_type == 'message':
        # For any message from a user, show the main menu.
        send_blood_usage_menu(user_id)
    elif event_type == 'postback':
        handle_postback_event(event, user_id)
    elif event_type == 'follow':
        send_welcome_message(user_id)

def handle_postback_event(event, user_id):
    postback_data = event.get('postback', {}).get('data', '')
    print(f"📞 Postback from {user_id}: {postback_data}")
    
    # Check if the user wants to start a blood request
    if postback_data == 'START_BLOOD_REQUEST':
        # Create a preliminary request record in the database
        request_id = f"BR{datetime.now(THAILAND_TZ).strftime('%y%m%d%H%M%S')}{os.urandom(2).hex().upper()}"
        
        conn = get_db_connection()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO blood_requests (request_id, user_id, status) VALUES (%s, %s, %s)",
                        (request_id, user_id, 'initiated')
                    )
                conn.commit()
                # Send the user a link to the form, passing the new request_id
                send_blood_form_link(user_id, request_id)
            finally:
                if conn: conn.close()
        else:
            send_line_message(user_id, {"type": "text", "text": "ขออภัย, ไม่สามารถเชื่อมต่อฐานข้อมูลได้ในขณะนี้"})

def handle_user_event(user_id, event_type):
    """Inserts or updates a user's record in the 'users' table."""
    conn = get_db_connection()
    if not conn: return
    
    # --- REFINEMENT: This SQL now matches the simplified schema (no last_event) ---
    sql = """
        INSERT INTO users (user_id, status, last_activity)
        VALUES (%s, %s, %s)
        ON CONFLICT (user_id) DO UPDATE SET
            status = EXCLUDED.status,
            last_activity = EXCLUDED.last_activity;
    """
    status = 'inactive' if event_type == 'unfollow' else 'active'
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, status, datetime.now(THAILAND_TZ)))
        conn.commit()
        print(f"👤 User {user_id[:8]}... status updated to '{status}'.")
    finally:
        if conn: conn.close()

# --- LINE Messaging Functions ---
def send_line_message(user_id, messages):
    if not LINE_TOKEN: return False
    try:
        requests.post(
            'https://api.line.me/v2/bot/message/push',
            headers={'Authorization': f'Bearer {LINE_TOKEN}'},
            json={"to": user_id, "messages": messages if isinstance(messages, list) else [messages]},
            timeout=5
        ).raise_for_status()
        print(f"✅ Message sent to {user_id}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ LINE API Error: {e.response.status_code if e.response else 'N/A'} - {e.response.text if e.response else e}")
        return False

def send_blood_usage_menu(user_id):
    """Sends the main menu to start a blood request."""
    # REFINEMENT: Handles unregistered users gracefully.
    user_name = get_user_name(user_id)
    greeting = f"สวัสดีคุณ {user_name}" if user_name else "สวัสดี"
    
    message = {
        "type": "template", "altText": "แจ้งเตือนการใช้เลือด",
        "template": {
            "type": "buttons", "title": "🩸 ระบบแจ้งใช้เลือด",
            "text": f"{greeting}, กรุณาเลือกเมนูเพื่อดำเนินการต่อ",
            "actions": [{
                "type": "postback", "label": "🚨 แจ้งใช้เลือด", "data": "START_BLOOD_REQUEST"
            }]
        }
    }
    send_line_message(user_id, message)

def send_blood_form_link(user_id, request_id):
    """Sends a message with a button linking to the web form."""
    form_url = f"https://test-bb-six.vercel.app/confirm_usage.html?request_id={urllib.parse.quote(request_id)}"
    
    message = {
        "type": "template", "altText": "กรุณากรอกข้อมูลการใช้เลือด",
        "template": {
            "type": "buttons", "title": "📝 กรอกข้อมูล",
            "text": f"กรุณากดปุ่มด้านล่างเพื่อกรอกรายละเอียดสำหรับคำขอ: {request_id}",
            "actions": [{
                "type": "uri", "label": "📋 เปิดฟอร์ม", "uri": form_url
            }]
        }
    }
    send_line_message(user_id, message)

def send_welcome_message(user_id):
    """Sends a welcome message to new followers."""
    text = "ยินดีต้อนรับสู่ระบบแจ้งใช้เลือด! 🩸\n\nพิมพ์ข้อความใดๆ เพื่อเริ่มต้นใช้งาน"
    send_line_message(user_id, {"type": "text", "text": text})
