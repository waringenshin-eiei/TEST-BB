# api/webhook.py - Final, Complete, and Integrated Production-Ready Handler
#
# This single file provides a complete backend solution managing:
# 1. POST /api/webhook:         Instantaneous replies to the LINE Messaging API.
# 2. POST /api/submit_request:   Handles form submissions, creates new users on-the-fly, and sends rich reports.
# 3. GET  /api/sse-updates:      Provides a real-time Server-Sent Events stream for dashboards.
#
# --- Required Libraries ---
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
import traceback

# --- Configuration ---
POSTGRES_URL = os.environ.get('POSTGRES_URL')
LINE_TOKEN = os.environ.get('LINE_TOKEN')
FORM_BASE_URL = os.environ.get('FORM_BASE_URL', 'https://your-domain.com') # IMPORTANT: Set this in your environment
KV_URL = os.environ.get('KV_URL')

# --- Constants & Globals ---
THAILAND_TZ = timezone(timedelta(hours=7))
redis_client = Redis.from_url(KV_URL, decode_responses=True) if KV_URL else None
sse_clients = []
sse_queue = queue.Queue()

# --- Core Logic: Database & Cache ---
def get_db_connection():
    if not POSTGRES_URL: return None
    try:
        return psycopg.connect(POSTGRES_URL)
    except psycopg.OperationalError as e:
        print(f"❌ Database connection failed: {e}")
        return None

def get_user_profile(line_user_id):
    cache_key = f"user_profile:{line_user_id}"
    if redis_client:
        try:
            cached_profile = redis_client.get(cache_key)
            if cached_profile:
                print(f"CACHE HIT for user {line_user_id}")
                return json.loads(cached_profile)
        except Exception as e:
            print(f"⚠️ Redis GET error: {e}")

    print(f"CACHE MISS for user {line_user_id}. Querying PostgreSQL.")
    with get_db_connection() as conn:
        if not conn: return None
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users WHERE line_user_id = %s", (line_user_id,))
            result = cur.fetchone()

    if not result:
        return None

    profile_data = {"user_uuid": str(result[0])}
    if redis_client:
        try:
            redis_client.set(cache_key, json.dumps(profile_data), ex=86400)
            print(f"CACHE SET for user {line_user_id}")
        except Exception as e:
            print(f"⚠️ Redis SET error: {e}")
    return profile_data

# --- Core Logic: LINE Messaging & Flex Report ---
def send_line_message(user_id, messages):
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

def create_flex_report(report_data):
    """
    REFINED: Creates a LINE Flex Message with bilingual labels and fixes the subtype bug.
    """
    blood_type = report_data.get('bloodType', 'unknown').lower()
    color_themes = {
        'redcell': {'main': '#B91C1C', 'light': '#FEF2F2'},
        'ffp': {'main': '#B45309', 'light': '#FFFBEB'},
        'cryoprecipitate': {'main': '#0369A1', 'light': '#F0F9FF'},
        'unknown': {'main': '#4B5563', 'light': '#F3F4F6'}
    }
    theme = color_themes.get(blood_type, color_themes['unknown'])

    def create_bilingual_row(label_th, label_en, value, is_bold=False):
        return {
            "type": "box", "layout": "horizontal", "margin": "md", "contents": [
                { "type": "box", "layout": "vertical", "flex": 1, "contents": [
                    { "type": "text", "text": label_th, "size": "sm", "color": "#555555" },
                    { "type": "text", "text": label_en, "size": "xxs", "color": "#999999" }
                ]},
                { "type": "text", "text": str(value) if value else "-", "size": "sm", "color": "#111111", "align": "end", "weight": "bold" if is_bold else "regular", "flex": 2, "wrap": True }
            ]
        }
        
    return {
        "type": "flex", "altText": f"ยืนยันคำขอใช้เลือด: {report_data.get('request_id')}",
        "contents": { "type": "bubble",
            "header": {"type": "box", "layout": "vertical", "paddingAll": "20px", "backgroundColor": theme['main'], "contents": [
                {"type": "box", "layout": "vertical", "contents": [
                    {"type": "text", "text": "✅ ยืนยันข้อมูลสำเร็จ", "color": "#FFFFFF", "size": "lg", "weight": "bold"},
                    {"type": "text", "text": "Request Confirmed", "color": "#FFFFFFDD", "size": "xs"}
                ]}
            ]},
            "body": {"type": "box", "layout": "vertical", "spacing": "md", "backgroundColor": theme['light'], "paddingAll": "20px", "contents": [
                {"type": "box", "layout": "vertical", "margin": "md", "contents": [
                    {"type": "text", "text": "ข้อมูลคำขอใช้เลือด", "weight": "bold", "size": "xl", "color": theme['main']},
                    {"type": "text", "text": "Blood Request Information", "size": "xs", "color": theme['main'] + "B3"}
                ]},
                {"type": "separator", "margin": "lg"},
                create_bilingual_row("Request ID", "Request ID", report_data.get('request_id'), is_bold=True),
                create_bilingual_row("ผู้ป่วย", "Patient", f"{report_data.get('patientName')} (HN: {report_data.get('hn')})"),
                create_bilingual_row("หอผู้ป่วย", "Ward", report_data.get('wardName')),
                {"type": "separator", "margin": "lg"},
                create_bilingual_row("ผลิตภัณฑ์", "Product", report_data.get('bloodType', '').upper(), is_bold=True),
                create_bilingual_row("ชนิดย่อย", "Subtype", report_data.get('subtype', '-')), # FIX: Use .get() with a default to prevent errors
                create_bilingual_row("จำนวน", "Quantity", f"{report_data.get('quantity')} Units"),
                {"type": "separator", "margin": "lg"},
                create_bilingual_row("รอบส่ง", "Schedule", report_data.get('deliveryTime')),
                create_bilingual_row("สถานที่", "Location", report_data.get('deliveryLocation')),
                create_bilingual_row("ผู้แจ้ง", "Reporter", report_data.get('reporterName')),
                {"type": "separator", "margin": "lg"},
                {"type": "text", "text": f"บันทึกเมื่อ: {datetime.now(THAILAND_TZ).strftime('%d %b %Y, %H:%M')}", "wrap": True, "size": "xxs", "color": "#AAAAAA", "align": "center"}
            ]}
        }
    }

def send_form_link(user_id):
    form_url = f"{FORM_BASE_URL}/confirm_usage.html?line_user_id={urllib.parse.quote(user_id)}"
    message = {"type": "template", "altText": "แจ้งเตือนการใช้เลือด", "template": {
            "type": "buttons", "thumbnailImageUrl": "https://storage.googleapis.com/line-flex-images-logriz/blood-request-banner.png",
            "imageAspectRatio": "rectangle", "imageSize": "cover", "title": "🩸 ระบบแจ้งใช้เลือด",
            "text": "สวัสดี, กดปุ่มด้านล่างเพื่อเปิดฟอร์มแจ้งใช้เลือดได้ทันที",
            "actions": [{"type": "uri", "label": "📋 เปิดฟอร์ม", "uri": form_url}]
        }}
    send_line_message(user_id, message)

# --- SSE Logic ---
def notify_dashboard_update(event_type, data):
    event_data = {'type': event_type, 'timestamp': datetime.now(THAILAND_TZ).isoformat(), **data}
    try:
        sse_queue.put_nowait(json.dumps(event_data))
    except queue.Full:
        print("⚠️ SSE queue is full, dropping event")

def sse_worker():
    while True:
        try:
            message = sse_queue.get()
            for client_queue in sse_clients[:]:
                try:
                    client_queue.put_nowait(message)
                except queue.Full:
                    sse_clients.remove(client_queue)
        except Exception as e:
            print(f"SSE worker error: {e}")
            time.sleep(1)

sse_thread = threading.Thread(target=sse_worker, daemon=True)
if not sse_thread.is_alive():
    sse_thread.start()

# --- Main HTTP Handler ---
class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/api/webhook': self.handle_line_webhook()
        elif self.path == '/api/submit_request': self.handle_form_submission()
        else: self._send_response(404, {"error": "Endpoint not found"})

    def do_GET(self):
        if self.path == '/api/sse-updates': self.handle_sse_connection()
        elif self.path == '/api/webhook': self._send_response(200, {"status": "ok"})
        else: self._send_response(404, {"error": "Endpoint not found"})

    def handle_line_webhook(self):
        try:
            body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))))
            for event in body.get('events', []):
                user_id = event.get('source', {}).get('userId')
                if user_id and event.get('type') in ['message', 'follow']:
                    send_form_link(user_id)
        except Exception as e: print(f"❌ Webhook error: {e}")
        finally: self._send_response(200, {})

    def handle_form_submission(self):
        try:
            form_data = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))))
            line_user_id = form_data.get('line_user_id')
            if not line_user_id: raise ValueError("line_user_id is required")

            user_profile = get_user_profile(line_user_id)
            
            with get_db_connection() as conn:
                if not conn: raise ConnectionError("Database connection failed")
                with conn.cursor() as cur:
                    if not user_profile:
                        print(f"INFO: User {line_user_id} not found. Creating new user record.")
                        cur.execute(
                            "INSERT INTO users (line_user_id) VALUES (%s) RETURNING user_id",
                            (line_user_id,)
                        )
                        new_user_uuid = str(cur.fetchone()[0])
                        user_profile = {'user_uuid': new_user_uuid}
                        if redis_client:
                             redis_client.set(f"user_profile:{line_user_id}", json.dumps(user_profile), ex=86400)
                    
                    selected_ward_name = form_data.get('wardName')
                    if not selected_ward_name: raise ValueError("Ward Name from form is required")
                    
                    cur.execute("SELECT ward_id FROM ward_directory WHERE ward_name = %s", (selected_ward_name,))
                    ward_result = cur.fetchone()
                    if not ward_result: raise ValueError(f"Ward '{selected_ward_name}' not found in directory.")
                    ward_id_for_request = ward_result[0]
                    
                    cur.execute("""
                        INSERT INTO blood_requests (user_id, ward_id, schedule_id, status, request_data)
                        VALUES (%s, %s, %s, 'pending', %s) RETURNING request_id, created_at;
                    """, (user_profile['user_uuid'], ward_id_for_request, form_data.get('schedule_id'), json.dumps(form_data)))
                    result = cur.fetchone()
                    new_request_id, created_at = str(result[0]), result[1]
                    
                    cur.execute("""
                        INSERT INTO blood_components (request_id, component_type, quantity, component_subtype)
                        VALUES (%s, %s, %s, %s)
                    """, (new_request_id, form_data.get('bloodType'), form_data.get('quantity'), form_data.get('subtype')))
                    conn.commit()
            
            print(f"✅ DB insert successful for new request {new_request_id}")

            report_data = {**form_data, "request_id": new_request_id}
            flex_message = create_flex_report(report_data)
            send_line_message(line_user_id, flex_message)
            
            notify_dashboard_update('new_request', {
                'request_id': new_request_id, 'status': 'pending', 'ward_name': form_data.get('wardName'),
                'created_at': created_at.isoformat(), 'patient_name': form_data.get('patientName')
            })

            self._send_response(200, {"status": "success", "request_id": new_request_id})

        except Exception as e:
            print(f"❌ Form submission error: {e}\n{traceback.format_exc()}")
            self._send_response(500, {"status": "error", "message": "An internal error occurred."})

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
            self.wfile.write(f"data: {json.dumps({'type': 'connected'})}\n\n".encode('utf-8'))
            self.wfile.flush()
            while True:
                try:
                    message = client_queue.get(timeout=25)
                    self.wfile.write(f"data: {message}\n\n".encode('utf-8'))
                    self.wfile.flush()
                except queue.Empty:
                    heartbeat = json.dumps({'type': 'heartbeat', 'timestamp': datetime.now(THAILAND_TZ).isoformat()})
                    self.wfile.write(f"data: {heartbeat}\n\n".encode('utf-8'))
                    self.wfile.flush()
        except (IOError, BrokenPipeError, ConnectionResetError) as e:
            print(f"🔌 SSE client disconnected gracefully: {e}")
        finally:
            if client_queue in sse_clients: sse_clients.remove(client_queue)
            print(f"➖ SSE client removed. Total clients: {len(sse_clients)}")

    def do_OPTIONS(self):
        self._send_response(204, None)

    def _send_response(self, status_code, body):
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        if body is not None:
            self.wfile.write(json.dumps(body, ensure_ascii=False).encode('utf-8'))
