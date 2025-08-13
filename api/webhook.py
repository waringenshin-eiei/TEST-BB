# api/webhook.py - Final Production-Ready Webhook
# Architecture: High-speed, decoupled, with Redis caching and SSE for real-time updates.

# --- Required Libraries ---
# You will need to install: requests, psycopg[binary], redis
# pip install requests psycopg[binary] redis

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
# Load all credentials and settings from environment variables for security.
POSTGRES_URL = os.environ.get('POSTGRES_URL')
LINE_TOKEN = os.environ.get('LINE_TOKEN')
FORM_BASE_URL = os.environ.get('FORM_BASE_URL', 'https://test-bb-six.vercel.app') # IMPORTANT: Change this to your frontend URL

# Vercel KV (Redis) Credentials
KV_URL = os.environ.get('KV_URL')

# --- Constants & Globals ---
THAILAND_TZ = timezone(timedelta(hours=7))

# Initialize Redis client if configured. The app will work without it, but slower.
redis_client = None
if KV_URL:
    try:
        redis_client = Redis.from_url(KV_URL, decode_responses=True)
        print("✅ Redis client connected.")
    except Exception as e:
        print(f"⚠️ Could not connect to Redis: {e}. Caching will be disabled.")
else:
    print("⚠️ KV_URL not set. Caching will be disabled.")


# In-memory queue for Server-Sent Events (SSE).
# A more robust solution for multi-server setups would use Redis Pub/Sub.
sse_clients = []
sse_queue = queue.Queue()

# --- Core Logic: Database & Cache ---

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    if not POSTGRES_URL:
        print("❌ POSTGRES_URL is not configured.")
        return None
    try:
        return psycopg.connect(POSTGRES_URL, autocommit=True)
    except psycopg.OperationalError as e:
        print(f"❌ Database connection failed: {e}")
        return None

def get_user_profile(line_user_id):
    """
    Efficiently retrieves a user's profile.
    1. Tries to fetch from Redis cache.
    2. If not in cache, queries PostgreSQL.
    3. Stores the result back into the cache for future requests.
    """
    cache_key = f"user_profile:{line_user_id}"

    # 1. Try Redis Cache first
    if redis_client:
        try:
            cached_profile = redis_client.get(cache_key)
            if cached_profile:
                print(f"CACHE HIT for user {line_user_id}")
                return json.loads(cached_profile)
        except Exception as e:
            print(f"⚠️ Redis GET error: {e}")

    # 2. Cache Miss or Redis Error: Fallback to PostgreSQL
    print(f"CACHE MISS for user {line_user_id}. Querying PostgreSQL.")
    with get_db_connection() as conn:
        if not conn: return None
        with conn.cursor() as cur:
            # This query joins all necessary tables to build a complete profile
            cur.execute("""
                SELECT
                    u.user_id,         -- The internal UUID
                    wd.ward_id,        -- The ward's integer ID
                    wd.ward_name,      -- The ward's display name
                    up.reporter_name   -- The user's registered name
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

    # 3. Store result in Redis for next time (expires in 24 hours)
    if redis_client:
        try:
            redis_client.set(cache_key, json.dumps(profile_data), ex=86400)
            print(f"CACHE SET for user {line_user_id}")
        except Exception as e:
            print(f"⚠️ Redis SET error: {e}")
            
    return profile_data


# --- Core Logic: LINE Messaging ---

def send_line_message(user_id, messages):
    """Generic function to push a message to a user via the LINE API."""
    if not LINE_TOKEN: return
    try:
        requests.post(
            'https://api.line.me/v2/bot/message/push',
            headers={'Authorization': f'Bearer {LINE_TOKEN}', 'Content-Type': 'application/json'},
            json={"to": user_id, "messages": messages if isinstance(messages, list) else [messages]},
            timeout=5
        )
        print(f"✅ LINE message sent to {user_id}")
    except requests.exceptions.RequestException as e:
        print(f"❌ LINE API Error: {e}")

def send_combined_form_link(user_id):
    """Sends a single, direct-to-form message. This is the primary instant response."""
    form_url = f"{FORM_BASE_URL}/confirm_usage.html?line_user_id={urllib.parse.quote(user_id)}"
    
    message = {
        "type": "template",
        "altText": "แจ้งเตือนการใช้เลือด",
        "template": {
            "type": "buttons",
            "thumbnailImageUrl": "https://storage.googleapis.com/line-flex-images-logriz/blood-request-banner.png",
            "imageAspectRatio": "rectangle",
            "imageSize": "cover",
            "title": "🩸 ระบบแจ้งใช้เลือด",
            "text": "สวัสดี, กดปุ่มด้านล่างเพื่อเปิดฟอร์มแจ้งใช้เลือดได้ทันที",
            "actions": [{"type": "uri", "label": "📋 เปิดฟอร์ม", "uri": form_url}]
        }
    }
    send_line_message(user_id, message)

def send_confirmation_message(user_id, request_id):
    """Sends a confirmation to the user AFTER a successful form submission."""
    text = f"✅ ได้รับข้อมูลของท่านเรียบร้อยแล้ว\n\nหมายเลขคำขอของท่านคือ:\n{request_id}\n\nเจ้าหน้าที่จะดำเนินการตรวจสอบโดยเร็วที่สุด"
    send_line_message(user_id, {"type": "text", "text": text})


# --- Core Logic: Server-Sent Events (SSE) for Dashboard ---

def notify_dashboard_update(event_type, data):
    """Puts a new event onto the queue for the SSE worker to send."""
    event_data = {'type': event_type, 'timestamp': datetime.now(THAILAND_TZ).isoformat(), **data}
    try:
        sse_queue.put_nowait(json.dumps(event_data))
        print(f"📡 SSE event queued: {event_type}")
    except queue.Full:
        print("⚠️ SSE queue is full, dropping event")

def sse_worker():
    """
    A background thread that continuously sends queued messages
    to all connected dashboard clients.
    """
    while True:
        try:
            message = sse_queue.get() # This blocks until a message is available
            for client_queue in sse_clients[:]:
                try:
                    client_queue.put_nowait(message)
                except queue.Full:
                    # This client is not processing messages, assume disconnected
                    sse_clients.remove(client_queue)
        except Exception as e:
            print(f"SSE worker error: {e}")
            time.sleep(1) # Prevent rapid-fire errors

# Start the single SSE worker thread when the serverless function initializes.
sse_thread = threading.Thread(target=sse_worker, daemon=True)
if not sse_thread.is_alive():
    sse_thread.start()


# --- Main HTTP Request Handler ---

class handler(BaseHTTPRequestHandler):

    # --- POST Request Router ---
    def do_POST(self):
        if self.path == '/api/webhook':
            self.handle_line_webhook()
        elif self.path == '/api/submit_request':
            self.handle_form_submission()
        else:
            self.send_error(404, "Endpoint not found")

    # --- GET Request Router ---
    def do_GET(self):
        if self.path == '/api/sse-updates':
            self.handle_sse_connection()
        elif self.path == '/api/webhook':
            # A simple health check for the webhook URL
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            response = {"status": "ok", "sse_clients": len(sse_clients)}
            self.wfile.write(json.dumps(response).encode('utf-8'))
        else:
            self.send_error(404, "Endpoint not found")

    # --- Endpoint Handler 1: The Fast LINE Webhook ---
    def handle_line_webhook(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(content_length))
            
            for event in body.get('events', []):
                user_id = event.get('source', {}).get('userId')
                event_type = event.get('type')
                if not user_id: continue

                print(f"⚡ Webhook received '{event_type}' from {user_id}")
                if event_type in ['message', 'follow']:
                    send_combined_form_link(user_id)
        except Exception as e:
            print(f"❌ Unhandled Error in Webhook: {e}")
        finally:
            # CRITICAL: Always respond 200 OK immediately to the LINE Platform.
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'OK')

    # --- Endpoint Handler 2: The Form Submission Worker ---
    def handle_form_submission(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            form_data = json.loads(self.rfile.read(content_length))
            line_user_id = form_data.get('line_user_id')

            if not line_user_id:
                raise ValueError("line_user_id is required in form submission")

            # Get user profile (from cache or DB)
            user_profile = get_user_profile(line_user_id)
            if not user_profile:
                raise ValueError(f"User profile not found for line_user_id {line_user_id}")

            # Write the new request to the database
            with get_db_connection() as conn:
                if not conn: raise ConnectionError("Database connection failed during submission")
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO blood_requests 
                        (user_id, ward_id, schedule_id, status, request_data)
                        VALUES (%s, %s, %s, 'pending', %s)
                        RETURNING request_id, created_at;
                    """, (
                        user_profile['user_uuid'],
                        user_profile['ward_id'],
                        form_data.get('schedule_id'), # Assuming form sends the integer ID
                        json.dumps(form_data)
                    ))
                    result = cur.fetchone()
                    new_request_id, created_at_ts = str(result[0]), result[1]

            print(f"✅ Request {new_request_id} saved to database.")
            
            # Trigger notifications (these are non-blocking)
            notify_dashboard_update('new_request', {
                'request_id': new_request_id,
                'status': 'pending',
                'ward_name': user_profile.get('ward_name', 'Unknown'),
                'created_at': created_at_ts.isoformat()
            })
            send_confirmation_message(line_user_id, new_request_id)
            
            # Send success response back to the web form
            self.send_response(200)
            self.send_header('Content-type','application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "success", "request_id": new_request_id}).encode())

        except Exception as e:
            print(f"❌ Form submission error: {e}")
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": str(e)}).encode())

    # --- Endpoint Handler 3: SSE Connection for Dashboard ---
    def handle_sse_connection(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Connection', 'keep-alive')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        client_queue = queue.Queue()
        sse_clients.append(client_queue)
        print(f"➕ SSE client connected. Total clients: {len(sse_clients)}")

        try:
            # Send an initial connection confirmation
            self.wfile.write(f"data: {json.dumps({'type': 'connected'})}\n\n".encode())
            self.wfile.flush()

            while True:
                try:
                    # Wait for a message from the worker, with a timeout for heartbeats
                    message = client_queue.get(timeout=25)
                    self.wfile.write(f"data: {message}\n\n".encode())
                    self.wfile.flush()
                except queue.Empty:
                    # Send a heartbeat to keep the connection open
                    self.wfile.write(f"data: {json.dumps({'type': 'heartbeat'})}\n\n".encode())
                    self.wfile.flush()
        except (IOError, BrokenPipeError, ConnectionResetError) as e:
             # This is expected when a client closes their browser
            print(f"SSE client disconnected: {e}")
        finally:
            if client_queue in sse_clients:
                sse_clients.remove(client_queue)
            print(f"➖ SSE client removed. Total clients: {len(sse_clients)}")
