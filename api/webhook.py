# api/webhook.py - Enhanced with SSE support for real-time dashboard updates
from http.server import BaseHTTPRequestHandler
import json
from datetime import datetime, timezone, timedelta
import os
import requests
import psycopg
import urllib.parse
import threading
import queue
import time

# --- Configuration ---
POSTGRES_URL = os.environ.get('POSTGRES_URL')
LINE_TOKEN = os.environ.get('LINE_TOKEN', '')
THAILAND_TZ = timezone(timedelta(hours=7))

# Global queue for SSE events
sse_clients = []
sse_queue = queue.Queue()

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

def notify_dashboard_update(event_type, data):
    """Send real-time updates to connected dashboard clients."""
    event_data = {
        'type': event_type,
        'timestamp': datetime.now(THAILAND_TZ).isoformat(),
        **data
    }
    
    # Add to SSE queue
    try:
        sse_queue.put_nowait(json.dumps(event_data))
        print(f"📡 Dashboard notification sent: {event_type}")
    except queue.Full:
        print("⚠️ SSE queue is full, dropping notification")

def get_request_details(request_id):
    """Get detailed information about a request for notifications."""
    conn = get_db_connection()
    if not conn: return None
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT br.request_id, br.patient_name, br.hospital_number, 
                       br.status, br.request_data, wi.name as ward_name
                FROM blood_requests br
                LEFT JOIN ward_id wi ON br.user_id = wi.user_id
                WHERE br.request_id = %s
            """, (request_id,))
            result = cur.fetchone()
            
            if result:
                return {
                    'request_id': result[0],
                    'patient_name': result[1],
                    'hospital_number': result[2],
                    'status': result[3],
                    'ward_name': result[5]
                }
    except Exception as e:
        print(f"Error getting request details: {e}")
    finally:
        if conn: conn.close()
    
    return None

# --- Main HTTP Handler ---
class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/api/sse-updates':
            self.handle_sse()
        else:
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            db_status = "✅ Connected" if get_db_connection() else "❌ Connection Failed"
            response = {
                "status": "Webhook is running", 
                "database_status": db_status,
                "sse_clients": len(sse_clients),
                "timestamp": datetime.now(THAILAND_TZ).isoformat()
            }
            self.wfile.write(json.dumps(response).encode('utf-8'))

    def handle_sse(self):
        """Handle Server-Sent Events for real-time dashboard updates."""
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Connection', 'keep-alive')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        # Add client to list
        client_queue = queue.Queue()
        sse_clients.append(client_queue)
        
        try:
            # Send initial connection message
            self.send_sse_message({
                'type': 'connected',
                'message': 'Dashboard connected to real-time updates',
                'timestamp': datetime.now(THAILAND_TZ).isoformat()
            })
            
            # Keep connection alive and send updates
            while True:
                try:
                    # Check for new messages in client queue (timeout after 30 seconds)
                    message = client_queue.get(timeout=30)
                    self.wfile.write(f"data: {message}\n\n".encode('utf-8'))
                    self.wfile.flush()
                except queue.Empty:
                    # Send heartbeat to keep connection alive
                    self.send_sse_message({
                        'type': 'heartbeat',
                        'timestamp': datetime.now(THAILAND_TZ).isoformat()
                    })
                except Exception as e:
                    print(f"SSE client disconnected: {e}")
                    break
                    
        except Exception as e:
            print(f"SSE connection error: {e}")
        finally:
            # Remove client from list
            if client_queue in sse_clients:
                sse_clients.remove(client_queue)

    def send_sse_message(self, data):
        """Send a single SSE message."""
        try:
            message = json.dumps(data)
            self.wfile.write(f"data: {message}\n\n".encode('utf-8'))
            self.wfile.flush()
        except Exception as e:
            print(f"Error sending SSE message: {e}")

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

# --- SSE Background Worker ---
def sse_worker():
    """Background worker to distribute SSE messages to all connected clients."""
    while True:
        try:
            # Get message from queue (blocking)
            message = sse_queue.get()
            
            # Send to all connected clients
            disconnected_clients = []
            for client_queue in sse_clients[:]:  # Copy list to avoid modification during iteration
                try:
                    client_queue.put_nowait(message)
                except queue.Full:
                    # Client queue is full, mark for removal
                    disconnected_clients.append(client_queue)
                except Exception as e:
                    print(f"Error sending to SSE client: {e}")
                    disconnected_clients.append(client_queue)
            
            # Remove disconnected clients
            for client in disconnected_clients:
                if client in sse_clients:
                    sse_clients.remove(client)
                    
        except Exception as e:
            print(f"SSE worker error: {e}")
            time.sleep(1)

# Start SSE worker thread
sse_thread = threading.Thread(target=sse_worker, daemon=True)
sse_thread.start()

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
                        "INSERT INTO blood_requests (request_id, user_id, status, created_at) VALUES (%s, %s, %s, %s)",
                        (request_id, user_id, 'initiated', datetime.now(THAILAND_TZ))
                    )
                conn.commit()
                
                # Send real-time notification to dashboard
                request_details = get_request_details(request_id)
                if request_details:
                    notify_dashboard_update('new_request', {
                        'request_id': request_id,
                        'status': 'initiated',
                        'ward_name': request_details.get('ward_name', 'Unknown'),
                        'message': f'New blood request initiated: {request_id}'
                    })
                
                # Send the user a link to the form
                send_blood_form_link(user_id, request_id)
                
            except Exception as e:
                print(f"Database error: {e}")
                send_line_message(user_id, {"type": "text", "text": "ขออภัย, ไม่สามารถสร้างคำขอได้ในขณะนี้"})
            finally:
                if conn: conn.close()
        else:
            send_line_message(user_id, {"type": "text", "text": "ขออภัย, ไม่สามารถเชื่อมต่อฐานข้อมูลได้ในขณะนี้"})

def handle_user_event(user_id, event_type):
    """Inserts or updates a user's record in the 'users' table."""
    conn = get_db_connection()
    if not conn: return
    
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
    except Exception as e:
        print(f"User update error: {e}")
    finally:
        if conn: conn.close()

# --- Form Submission Handler ---
def handle_form_submission(request_id, form_data):
    """Handle form submission and update request status."""
    conn = get_db_connection()
    if not conn: return False
    
    try:
        with conn.cursor() as cur:
            # Update request with form data
            cur.execute("""
                UPDATE blood_requests 
                SET patient_name = %s, hospital_number = %s, request_data = %s, 
                    status = %s, updated_at = %s
                WHERE request_id = %s
            """, (
                form_data.get('patient_name'),
                form_data.get('hospital_number'), 
                json.dumps(form_data),
                'submitted',
                datetime.now(THAILAND_TZ),
                request_id
            ))
        conn.commit()
        
        # Send real-time notification to dashboard
        request_details = get_request_details(request_id)
        if request_details:
            notify_dashboard_update('status_update', {
                'request_id': request_id,
                'status': 'submitted',
                'patient_name': request_details.get('patient_name'),
                'ward_name': request_details.get('ward_name'),
                'message': f'Request {request_id} submitted and ready for review'
            })
        
        return True
        
    except Exception as e:
        print(f"Form submission error: {e}")
        return False
    finally:
        if conn: conn.close()

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
