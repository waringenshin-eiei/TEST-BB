# api/webhook.py - Final, Complete, and Integrated Production-Ready Handler
#
# This single file provides a complete backend solution managing:
# 1. POST /api/webhook:         Instantaneous replies to the LINE Messaging API.
# 2. POST /api/submit_request:  Handles submissions, creates new users, splits JSON to DB columns, and sends rich reports.
# 3. GET  /api/get_requests:      Fetches initial data for the dashboard.
# 4. POST /api/update_status:   Handles status changes from the dashboard.
# 5. GET  /api/sse-updates:       Provides a real-time Server-Sent Events stream for dashboards.
#
# --- Required Libraries ---
# pip install requests psycopg[binary] redis

from http.server import BaseHTTPRequestHandler
import json
from datetime import datetime, timezone, timedelta
import os
import requests
import psycopg
from psycopg.rows import dict_row
from redis import Redis
import urllib.parse
import threading
import queue
import time
import traceback

# --- Configuration ---
POSTGRES_URL = os.environ.get('POSTGRES_URL')
LINE_TOKEN = os.environ.get('LINE_TOKEN')
FORM_BASE_URL = os.environ.get('FORM_BASE_URL', 'https://test-bb-six.vercel.app') # IMPORTANT: Set this in your environment
KV_URL = os.environ.get('KV_URL')

# --- Constants & Globals ---
THAILAND_TZ = timezone(timedelta(hours=7))
redis_client = Redis.from_url(KV_URL, decode_responses=True) if KV_URL else None
sse_clients = []
sse_queue = queue.Queue()

# --- Core Logic: Database & Cache ---
def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    if not POSTGRES_URL: return None
    try:
        return psycopg.connect(POSTGRES_URL)
    except psycopg.OperationalError as e:
        print(f"❌ Database connection failed: {e}")
        return None

def get_user_profile(line_user_id):
    """Efficiently retrieves a user's internal UUID from cache or DB."""
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
    
    try:
        conn = get_db_connection()
        if not conn: 
            print("❌ Database connection failed")
            return None
            
        try:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT user_id FROM users WHERE line_user_id = %s", (line_user_id,))
                result = cur.fetchone()
                
                if not result:
                    print(f"No user found for line_user_id: {line_user_id}")
                    return None
                    
                # Access the result properly
                user_uuid = result['user_id'] if isinstance(result, dict) else result[0]
                profile_data = {"user_uuid": str(user_uuid)}
                
                if redis_client:
                    try:
                        redis_client.set(cache_key, json.dumps(profile_data), ex=86400)
                        print(f"CACHE SET for user {line_user_id}")
                    except Exception as e:
                        print(f"⚠️ Redis SET error: {e}")
                        
                return profile_data
                
        finally:
            conn.close()
            
    except Exception as e:
        print(f"❌ Error in get_user_profile: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        return None

# --- Core Logic: LINE Messaging & Flex Report ---
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

def create_flex_report(report_data):
    """Creates a beautiful, color-coded LINE Flex Message JSON object with bilingual labels."""
    blood_type = report_data.get('bloodType', 'unknown').lower()
    color_themes = {
        'redcell': {'main': '#B91C1C', 'light': '#FEF2F2'},
        'ffp': {'main': '#B45309', 'light': '#FFFBEB'},
        'cryoprecipitate': {'main': '#0369A1', 'light': '#F0F9FF'},
        'unknown': {'main': '#4B5563', 'light': '#F3F4F6'}
    }
    theme = color_themes.get(blood_type, color_themes['unknown'])
    
    delivery_time = report_data.get('deliveryTime', '-')
    todays_date = datetime.now(THAILAND_TZ).strftime('%d %b %Y')
    full_delivery_schedule = f"{todays_date}, {delivery_time}" if delivery_time != '-' else '-'

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
                create_bilingual_row("ผู้ป่วย", "Patient", f"{report_data.get('patientName')} (HN: {report_data.get('hn')})", is_bold=True),
                create_bilingual_row("หอผู้ป่วย", "Ward", report_data.get('wardName')),
                {"type": "separator", "margin": "lg"},
                create_bilingual_row("ผลิตภัณฑ์", "Product", report_data.get('bloodType', '').upper(), is_bold=True),
                create_bilingual_row("ชนิดย่อย", "Subtype", report_data.get('subtype', '-')),
                create_bilingual_row("จำนวน", "Quantity", f"{report_data.get('quantity')} Units"),
                {"type": "separator", "margin": "lg"},
                create_bilingual_row("รอบส่ง", "Schedule", full_delivery_schedule),
                create_bilingual_row("สถานที่", "Location", report_data.get('deliveryLocation')),
                create_bilingual_row("ผู้แจ้ง", "Reporter", report_data.get('reporterName')),
                {"type": "separator", "margin": "lg"},
                {"type": "text", "text": f"บันทึกเมื่อ (Date): {datetime.now(THAILAND_TZ).strftime('%d %b %Y, %H:%M')}", "wrap": True, "size": "xxs", "color": "#AAAAAA", "align": "center"}
            ]}
        }
    }

def send_form_link(user_id):
    """Sends the initial message with a button to open the form."""
    form_url = f"{FORM_BASE_URL}/confirm_usage.html?line_user_id={urllib.parse.quote(user_id)}"
    message = {"type": "template", "altText": "แจ้งเตือนการใช้เลือด", "template": {
            "type": "buttons", "thumbnailImageUrl": "https://storage.googleapis.com/line-flex-images-logriz/blood-request-banner.png",
            "imageAspectRatio": "rectangle", "imageSize": "cover", "title": "🩸 ระบบแจ้งใช้เลือด",
            "text": "สวัสดี, กดปุ่มด้านล่างเพื่อเปิดฟอร์มแจ้งใช้เลือดได้ทันที",
            "actions": [{"type": "uri", "label": "📋 เปิดฟอร์ม", "uri": form_url}]
        }}
    send_line_message(user_id, message)

# --- Core Logic: Server-Sent Events (SSE) ---
def notify_dashboard_update(event_type, data):
    event_data = {'type': event_type, 'timestamp': datetime.now(THAILAND_TZ).isoformat(), **data}
    try:
        # Use default=str to handle non-serializable types like datetime
        sse_queue.put_nowait(json.dumps(event_data, default=str)) 
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
        elif self.path == '/api/update_status': self.handle_update_status()
        else: self._send_response(404, {"error": "Endpoint not found"})

    def do_GET(self):
        if self.path == '/api/sse-updates': self.handle_sse_connection()
        elif self.path == '/api/get_requests': self.handle_get_requests()
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
                with conn.cursor(row_factory=dict_row) as cur:
                    if not user_profile:
                        print(f"INFO: User {line_user_id} not found. Creating new user record.")
                        cur.execute("INSERT INTO users (line_user_id) VALUES (%s) RETURNING user_id", (line_user_id,))
                        new_user_uuid = str(cur.fetchone()['user_id'])
                        user_profile = {'user_uuid': new_user_uuid}
                        if redis_client: redis_client.set(f"user_profile:{line_user_id}", json.dumps(user_profile), ex=86400)
                    
                    selected_ward_name = form_data.get('wardName')
                    if not selected_ward_name: raise ValueError("Ward Name from form is required")
                    
                    cur.execute("SELECT ward_id FROM ward_directory WHERE ward_name = %s", (selected_ward_name,))
                    ward_result = cur.fetchone()
                    if not ward_result: raise ValueError(f"Ward '{selected_ward_name}' not found in directory.")
                    ward_id_for_request = ward_result['ward_id']
                    
                    sql_insert_request = """
                        INSERT INTO blood_requests (user_id, ward_id, schedule_id, status, blood_type, patient_name, hospital_number, delivery_location, reporter_name, blood_details, request_data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *;
                    """
                    blood_details_str = f"{form_data.get('subtype')} ({form_data.get('quantity')} Units)"
                    insert_values = (
                        user_profile['user_uuid'], ward_id_for_request, form_data.get('schedule_id'), 'pending',
                        form_data.get('bloodType'), form_data.get('patientName'), form_data.get('hn'),
                        form_data.get('deliveryLocation'), form_data.get('reporterName'), blood_details_str,
                        json.dumps(form_data))
                    
                    cur.execute(sql_insert_request, insert_values)
                    new_request_record = cur.fetchone()
                    conn.commit()
            
            new_request_id = str(new_request_record['request_id'])
            print(f"✅ DB insert successful for new request {new_request_id}")

            report_data = {**form_data, "request_id": new_request_id}
            flex_message = create_flex_report(report_data)
            send_line_message(line_user_id, flex_message)
            
            notify_dashboard_update('new_request', new_request_record)
            self._send_response(200, {"status": "success", "request_id": new_request_id})

        except Exception as e:
            print(f"❌ Form submission error: {e}\n{traceback.format_exc()}")
            self._send_response(500, {"status": "error", "message": "An internal error occurred."})

    def handle_get_requests(self):
        """Fetches initial requests for the dashboard."""
        try:
            conn = get_db_connection()
            if not conn: 
                raise ConnectionError("Database connection failed")
            
            try:
                with conn.cursor(row_factory=dict_row) as cur:
                    # First, let's check what tables exist
                    cur.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name IN ('blood_requests', 'ward_directory');
                    """)
                    tables = [row['table_name'] for row in cur.fetchall()]
                    print(f"📊 Available tables: {tables}")
                    
                    # Check if blood_requests table exists and what columns it has
                    if 'blood_requests' in tables:
                        cur.execute("""
                            SELECT column_name, data_type 
                            FROM information_schema.columns 
                            WHERE table_name = 'blood_requests';
                        """)
                        columns = cur.fetchall()
                        print(f"📊 blood_requests columns: {[col['column_name'] for col in columns]}")
                    else:
                        raise Exception("blood_requests table not found")
                    
                    # Start with a simple query to test
                    cur.execute("SELECT COUNT(*) as count FROM blood_requests;")
                    count_result = cur.fetchone()
                    print(f"📊 Total records in blood_requests: {count_result['count']}")
                    
                    # Try a basic query first
                    cur.execute("""
                        SELECT 
                            request_id, 
                            status, 
                            blood_type, 
                            created_at, 
                            updated_at
                        FROM blood_requests
                        ORDER BY created_at DESC 
                        LIMIT 10;
                    """)
                    basic_requests = cur.fetchall()
                    print(f"📊 Retrieved {len(basic_requests)} basic records")
                    
                    # Now try the full query
                    if 'ward_directory' in tables:
                        cur.execute("""
                            SELECT 
                                br.request_id, 
                                br.status, 
                                br.blood_type, 
                                br.patient_name,
                                br.hospital_number,
                                br.delivery_location,
                                br.blood_details,
                                br.request_data, 
                                br.created_at, 
                                br.updated_at,
                                wd.ward_name
                            FROM blood_requests br
                            LEFT JOIN ward_directory wd ON br.ward_id = wd.ward_id
                            ORDER BY br.created_at DESC 
                            LIMIT 100;
                        """)
                    else:
                        # Query without ward join if ward_directory doesn't exist
                        cur.execute("""
                            SELECT 
                                request_id, 
                                status, 
                                blood_type, 
                                patient_name,
                                hospital_number,
                                delivery_location,
                                blood_details,
                                request_data, 
                                created_at, 
                                updated_at,
                                'N/A' as ward_name
                            FROM blood_requests
                            ORDER BY created_at DESC 
                            LIMIT 100;
                        """)
                    
                    raw_requests = cur.fetchall()
                    print(f"📊 Retrieved {len(raw_requests)} full records")
                    
            finally:
                conn.close()
            
            # Process records for safe JSON serialization
            processed_requests = []
            for i, row in enumerate(raw_requests):
                try:
                    record = dict(row) if hasattr(row, 'keys') else dict(row._asdict()) if hasattr(row, '_asdict') else {}
                    print(f"📊 Processing record {i}: {list(record.keys())}")
                    
                    # Handle datetime conversion
                    for date_field in ['created_at', 'updated_at']:
                        if date_field in record and record[date_field]:
                            if isinstance(record[date_field], datetime):
                                record[date_field] = record[date_field].isoformat()
                            elif isinstance(record[date_field], str):
                                try:
                                    dt = datetime.fromisoformat(record[date_field].replace('Z', '+00:00'))
                                    record[date_field] = dt.isoformat()
                                except ValueError:
                                    pass
                    
                    # Handle request_data JSON parsing
                    if 'request_data' in record and record['request_data']:
                        if isinstance(record['request_data'], str):
                            try:
                                record['request_data'] = json.loads(record['request_data'])
                            except json.JSONDecodeError:
                                print(f"⚠️ Warning: Malformed JSON in request_data for request_id {record.get('request_id')}")
                                record['request_data'] = {}
                        elif record['request_data'] is None:
                            record['request_data'] = {}
                    else:
                        record['request_data'] = {}
                    
                    # Ensure all required fields have default values
                    record.setdefault('patient_name', 'N/A')
                    record.setdefault('hospital_number', 'N/A')
                    record.setdefault('delivery_location', 'N/A')
                    record.setdefault('blood_details', 'N/A')
                    record.setdefault('ward_name', 'N/A')
                    record.setdefault('blood_type', 'unknown')
                    record.setdefault('status', 'pending')
                    record.setdefault('request_id', f'unknown_{i}')
    
                    processed_requests.append(record)
                    
                except Exception as row_error:
                    print(f"❌ Error processing row {i}: {row_error}")
                    print(f"Raw row data: {row}")
                    continue
    
            print(f"✅ Successfully processed {len(processed_requests)} requests")
            self._send_response(200, {"requests": processed_requests})
            
        except Exception as e:
            print(f"❌ Get requests error: {e}")
            print(f"Error type: {type(e).__name__}")
            print(f"Traceback: {traceback.format_exc()}")
            error_details = {
                "error": "Failed to fetch requests",
                "message": str(e),
                "type": type(e).__name__,
                "traceback": traceback.format_exc()
            }
            self._send_response(500, error_details)


    def handle_update_status(self):
        """Updates the status of a request from the dashboard."""
        try:
            form_data = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))))
            request_id = form_data.get('request_id')
            new_status = form_data.get('status')
            if not request_id or not new_status: raise ValueError("request_id and status are required")

            with get_db_connection() as conn:
                if not conn: raise ConnectionError("Database connection failed")
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute("UPDATE blood_requests SET status = %s, updated_at = NOW() WHERE request_id = %s RETURNING *;", (new_status, request_id))
                    updated_request = cur.fetchone()
                    conn.commit()
            
            if updated_request:
                print(f"✅ Status updated for {request_id} to {new_status}")
                notify_dashboard_update('status_update', dict(updated_request))
                self._send_response(200, {"status": "success", "request": dict(updated_request)})
            else:
                self._send_response(404, {"error": "Request not found"})
        except Exception as e:
            print(f"❌ Update status error: {e}\n{traceback.format_exc()}")
            self._send_response(500, {"error": "Failed to update status."})

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
            self.wfile.write(json.dumps(body, ensure_ascii=False, default=str).encode('utf-8'))
