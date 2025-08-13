# api/webhook.py - High-Performance Webhook with Redis Caching and Decoupled Submission
from http.server import BaseHTTPRequestHandler
import json
from datetime import datetime, timezone, timedelta
import os
import requests
import psycopg
from redis import Redis
import urllib.parse
import threading
import queue
import time

# --- Configuration ---
POSTGRES_URL = os.environ.get('POSTGRES_URL')
LINE_TOKEN = os.environ.get('LINE_TOKEN', '')
# Vercel KV (Redis) Credentials
KV_URL = os.environ.get('KV_URL')
# Your LIFF/Form URL
FORM_BASE_URL = os.environ.get('FORM_BASE_URL', 'https://test-bb-six.vercel.app')


THAILAND_TZ = timezone(timedelta(hours=7))

# --- Global Connections & Queues ---
# Use a global Redis client for caching
redis_client = Redis.from_url(KV_URL, decode_responses=True) if KV_URL else None

sse_clients = []
sse_queue = queue.Queue()

# --- Database & Cache Functions ---
def get_db_connection():
    if not POSTGRES_URL: return None
    try:
        # Using a context manager is better for serverless functions
        return psycopg.connect(POSTGRES_URL, autocommit=True)
    except psycopg.OperationalError as e:
        print(f"❌ Database connection failed: {e}")
        return None

def get_user_profile(line_user_id):
    """
    Gets user profile (internal UUID, ward info) first from Redis cache,
    then falls back to PostgreSQL.
    """
    cache_key = f"user_profile:{line_user_id}"

    # 1. Try to get from Redis cache
    if redis_client:
        try:
            cached_profile = redis_client.get(cache_key)
            if cached_profile:
                print(f"CACHE HIT for user {line_user_id}")
                return json.loads(cached_profile)
        except Exception as e:
            print(f"⚠️ Redis GET error: {e}")

    print(f"CACHE MISS for user {line_user_id}. Querying PostgreSQL.")
    
    # 2. If not in cache, query PostgreSQL
    with get_db_connection() as conn:
        if not conn: return None
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    u.user_id, -- The internal UUID
                    wd.ward_id,
                    wd.ward_name,
                    up.reporter_name
                FROM users u
                LEFT JOIN user_profiles up ON u.user_id = up.user_id
                LEFT JOIN ward_directory wd ON up.primary_ward_id = wd.ward_id
                WHERE u.line_user_id = %s
            """, (line_user_id,))
            result = cur.fetchone()

    if not result:
        print(f"❌ User with line_user_id {line_user_id} not found in DB.")
        return None

    profile_data = {
        "user_uuid": str(result[0]),
        "ward_id": result[1],
        "ward_name": result[2],
        "reporter_name": result[3]
    }

    # 3. Store the result back in Redis with an expiration (e.g., 24 hours)
    if redis_client:
        try:
            redis_client.set(cache_key, json.dumps(profile_data), ex=86400) # Cache for 1 day
            print(f"CACHE SET for user {line_user_id}")
        except Exception as e:
            print(f"⚠️ Redis SET error: {e}")
            
    return profile_data

# (notify_dashboard_update and other SSE functions remain the same)
def notify_dashboard_update(event_type, data):
    event_data = {'type': event_type, 'timestamp': datetime.now(THAILAND_TZ).isoformat(), **data}
    try:
        sse_queue.put_nowait(json.dumps(event_data))
        print(f"📡 Dashboard notification sent: {event_type}")
    except queue.Full:
        print("⚠️ SSE queue is full, dropping notification")

# --- Main HTTP Handler ---
class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        """Routes POST requests to the correct handler based on the path."""
        if self.path == '/api/webhook':
            self.handle_line_webhook()
        elif self.path == '/api/submit_request':
            self.handle_form_submission()
        else:
            self.send_error(404, "Endpoint not found")

    def handle_line_webhook(self):
        """
        Handles incoming events from LINE. This is optimized for speed.
        It does NOT connect to the database.
        """
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            webhook_data = json.loads(post_data.decode('utf-8'))
            
            for event in webhook_data.get('events', []):
                self.process_line_event(event)
        except Exception as e:
            print(f"❌ Unhandled Error in Webhook: {e}")
        finally:
            # Always respond quickly to the LINE Platform
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'OK')
            
    def process_line_event(self, event):
        """Processes a single event from LINE without blocking."""
        user_id = event.get('source', {}).get('userId')
        if not user_id: return

        event_type = event.get('type')
        print(f"⚡ Processing event '{event_type}' for user {user_id}")
        
        # Note: We are no longer calling handle_user_event here to keep the webhook fast.
        # User activity can be updated during form submission instead.
        
        if event_type == 'message' or event_type == 'follow':
            send_blood_usage_menu_fast(user_id)
        elif event_type == 'postback':
            postback_data = event.get('postback', {}).get('data', '')
            if postback_data == 'START_BLOOD_REQUEST':
                send_blood_form_link_fast(user_id)
    
    def handle_form_submission(self):
        """
        Handles form data submitted from the web front-end.
        This is where database operations now occur.
        """
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            form_data = json.loads(self.rfile.read(content_length))
            line_user_id = form_data.get('line_user_id')

            if not line_user_id:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": "line_user_id is required"}).encode())
                return

            # Get user profile from cache or DB
            user_profile = get_user_profile(line_user_id)
            if not user_profile:
                # This could happen if a user is not registered. We can create them on-the-fly.
                # For now, we'll error out.
                raise ValueError("User profile not found.")

            # Insert into database
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # The database will generate the UUID for request_id
                    cur.execute("""
                        INSERT INTO blood_requests 
                        (user_id, ward_id, schedule_id, blood_type, patient_name, hospital_number, blood_details, delivery_location, reporter_name, status, request_data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING request_id, created_at;
                    """, (
                        user_profile['user_uuid'],
                        user_profile['ward_id'],
                        form_data.get('schedule_id'), # Assuming form sends this ID
                        form_data.get('blood_type'),
                        form_data.get('patient_name'),
                        form_data.get('hospital_number'),
                        json.dumps(form_data.get('blood_components')), # Storing components here for simplicity
                        form_data.get('delivery_location'),
                        user_profile.get('reporter_name'),
                        'pending', # New status, changed from 'submitted'
                        json.dumps(form_data)
                    ))
                    result = cur.fetchone()
                    new_request_id = str(result[0])
                    created_at_ts = result[1]

            print(f"✅ New request {new_request_id} saved to database.")
            
            # Notify dashboard via SSE
            notify_dashboard_update('new_request', {
                'request_id': new_request_id,
                'status': 'pending',
                'ward_name': user_profile.get('ward_name', 'Unknown'),
                'patient_name': form_data.get('patient_name'),
                'created_at': created_at_ts.isoformat()
            })

            # Send confirmation message back to the user
            send_confirmation_message(line_user_id, new_request_id)
            
            # Respond to the form submission
            self.send_response(200)
            self.send_header('Content-type','application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "success", "request_id": new_request_id}).encode())

        except Exception as e:
            print(f"❌ Form submission error: {e}")
            self.send_error(500, f"Internal Server Error: {e}")

    # (do_GET, handle_sse, sse_worker, and other SSE functions remain the same)
    def do_GET(self):
        # ... (keep existing GET handler logic)
        pass
    def handle_sse(self):
        # ... (keep existing SSE handler logic)
        pass
    def send_sse_message(self, data):
        # ... (keep existing SSE send logic)
        pass
# (sse_worker thread also remains the same)

# --- NEW High-Speed LINE Messaging Functions ---
def send_blood_usage_menu_fast(user_id):
    """Sends the main menu INSTANTLY without a database query."""
    message = {
        "type": "template", "altText": "แจ้งเตือนการใช้เลือด",
        "template": {
            "type": "buttons", "title": "🩸 ระบบแจ้งใช้เลือด",
            "text": "สวัสดี, กรุณาเลือกเมนูเพื่อดำเนินการต่อ", # Generic greeting
            "actions": [{"type": "postback", "label": "🚨 แจ้งใช้เลือด", "data": "START_BLOOD_REQUEST"}]
        }
    }
    send_line_message(user_id, message)

def send_blood_form_link_fast(user_id):
    """
    Sends a link to the web form INSTANTLY.
    The form does not have a request_id yet, but passes the user's ID.
    """
    # The form will get the line_user_id and include it in the submission
    form_url = f"{FORM_BASE_URL}/confirm_usage.html?line_user_id={urllib.parse.quote(user_id)}"
    
    message = {
        "type": "template", "altText": "กรุณากรอกข้อมูลการใช้เลือด",
        "template": {
            "type": "buttons", "title": "📝 กรอกข้อมูล",
            "text": "กรุณากดปุ่มด้านล่างเพื่อกรอกรายละเอียดการใช้เลือด",
            "actions": [{"type": "uri", "label": "📋 เปิดฟอร์ม", "uri": form_url}]
        }
    }
    send_line_message(user_id, message)

def send_confirmation_message(user_id, request_id):
    """Sends a confirmation to the user after a successful form submission."""
    text = f"✅ ได้รับข้อมูลของท่านเรียบร้อยแล้ว\n\nหมายเลขคำขอของท่านคือ:\n{request_id}\n\nเจ้าหน้าที่จะดำเนินการตรวจสอบโดยเร็วที่สุด"
    send_line_message(user_id, {"type": "text", "text": text})

# --- LINE Messaging Functions ---
def send_line_message(user_id, messages):
    if not LINE_TOKEN: return False
    try:
        response = requests.post(
            'https://api.line.me/v2/bot/message/push',
            headers={'Authorization': f'Bearer {LINE_TOKEN}'},
            json={"to": user_id, "messages": messages if isinstance(messages, list) else [messages]},
            timeout=10
        )
        response.raise_for_status()
        print(f"✅ Message sent to {user_id}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ LINE API Error: {e.response.status_code if e.response else 'N/A'} - {e.response.text if e.response else e}")
        return False

def send_blood_usage_menu(user_id):
    """Sends the main menu to start a blood request."""
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

# --- Utility Functions for Dashboard Integration ---
def send_status_update_notification(request_id, old_status, new_status):
    """Send notification when request status changes."""
    request_details = get_request_details(request_id)
    if request_details:
        notify_dashboard_update('status_update', {
            'request_id': request_id,
            'old_status': old_status,
            'new_status': new_status,
            'patient_name': request_details.get('patient_name'),
            'ward_name': request_details.get('ward_name'),
            'message': f'Status changed: {old_status} → {new_status}'
        })

def send_bulk_update_notification(message):
    """Send bulk update notification (e.g., system maintenance)."""
    notify_dashboard_update('system_update', {
        'message': message,
        'severity': 'info'
    })

# Health check function
def get_system_health():
    """Get system health status for monitoring."""
    db_conn = get_db_connection()
    return {
        'database': 'connected' if db_conn else 'disconnected',
        'line_token': 'configured' if LINE_TOKEN else 'missing',
        'sse_clients': len(sse_clients),
        'timestamp': datetime.now(THAILAND_TZ).isoformat()
    }
