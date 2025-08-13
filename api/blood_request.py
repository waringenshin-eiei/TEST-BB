# api/blood_request.py - Refined to correctly look up user_id
from http.server import BaseHTTPRequestHandler
import json
import os
import traceback
from datetime import datetime, timezone, timedelta
import psycopg
import requests

# --- Configuration ---
POSTGRES_URL = os.environ.get('POSTGRES_URL')
LINE_TOKEN = os.environ.get('LINE_TOKEN', '')
THAILAND_TZ = timezone(timedelta(hours=7))

# --- Helper Functions (get_db_connection, send_line_notification) ---
def get_db_connection():
    if not POSTGRES_URL: return None
    try:
        return psycopg.connect(POSTGRES_URL)
    except psycopg.OperationalError:
        return None

def send_line_notification(user_id, message_text):
    if not LINE_TOKEN:
        print("WARNING: LINE_TOKEN is not configured. Skipping notification.")
        return False
    api_url = 'https://api.line.me/v2/bot/message/push'
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {LINE_TOKEN}'}
    payload = {'to': user_id, 'messages': [{'type': 'text', 'text': message_text}]}
    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=5)
        response.raise_for_status()
        print(f"✅ LINE notification sent successfully to {user_id}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR: Failed to send LINE notification to {user_id}: {e}")
        if e.response is not None:
            print(f"❌ LINE API Response: {e.response.text}")
        return False

# --- Main Request Handler ---
class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)
            request_data = json.loads(post_data.decode('utf-8'))

            request_id = request_data.get('request_id')
            if not request_id:
                return self._send_response(400, {"error": "Missing required field: request_id"})

            conn = get_db_connection()
            if not conn:
                return self._send_response(503, {"error": "Database service is unavailable."})

            try:
                with conn.cursor() as cur:
                    # --- REFINEMENT: Correctly fetch the user_id from the existing request ---
                    # Instead of looking in ward_id, we find the user who started this request.
                    cur.execute("SELECT user_id FROM blood_requests WHERE request_id = %s", (request_id,))
                    result = cur.fetchone()
                    if not result:
                        return self._send_response(404, {"error": f"Request ID '{request_id}' not found."})
                    
                    user_id_to_notify = result[0]
                    print(f"INFO: Found original user_id '{user_id_to_notify}' for request_id '{request_id}'")
                    # --- END REFINEMENT ---

                    # Now, update the record with all the details from the form
                    sql_update = """
                        UPDATE blood_requests SET
                            blood_type = %s, patient_name = %s, hospital_number = %s, ward_name = %s,
                            blood_details = %s, delivery_time = %s, delivery_location = %s,
                            reporter_name = %s, status = %s, request_data = %s, updated_at = %s
                        WHERE request_id = %s
                    """
                    updated_at = datetime.now(THAILAND_TZ)
                    cur.execute(sql_update, (
                        request_data.get('bloodType'), request_data.get('patientName'), request_data.get('hospitalNumber'),
                        request_data.get('wardName'), request_data.get('bloodDetails'), request_data.get('deliveryTime'),
                        request_data.get('deliveryLocation'), request_data.get('reporterName'), 'submitted',
                        json.dumps(request_data), updated_at, request_id
                    ))

                    # Insert component details (deleting any old ones first to be safe)
                    cur.execute("DELETE FROM blood_components WHERE request_id = %s", (request_id,))
                    sql_components = """
                        INSERT INTO blood_components (request_id, component_type, quantity, component_subtype, properties)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cur.execute(sql_components, (
                        request_id, request_data.get('bloodType'), request_data.get('quantity'),
                        request_data.get('subtype'), json.dumps(request_data.get('properties', {}))
                    ))
                    
                    conn.commit()
                    print(f"✅ DB Update successful for {request_id}")

                    # Send final confirmation notification to the original user
                    line_message = (
                        f"✅ ยืนยันข้อมูลสำเร็จ\n"
                        f"Request ID: {request_id}\n\n"
                        f"ผู้ป่วย: {request_data.get('patientName')} (HN: {request_data.get('hospitalNumber')})\n"
                        f"หอผู้ป่วย: {request_data.get('wardName')}\n"
                        f"ประเภทเลือด: {request_data.get('bloodType', '').upper()} จำนวน {request_data.get('quantity')} Unit\n\n"
                        f"ธนาคารเลือดได้รับข้อมูลของท่านแล้ว"
                    )
                    send_line_notification(user_id_to_notify, line_message)

                    self._send_response(200, {"message": "Request updated successfully.", "request_id": request_id})

            except psycopg.Error as e:
                conn.rollback()
                print(f"ERROR: Database transaction failed: {e}")
                self._send_response(500, {"error": f"Database error: {e}"})
            finally:
                if conn:
                    conn.close()

        except Exception as e:
            print(f"ERROR: Unhandled exception in do_POST: {e}\n{traceback.format_exc()}")
            self._send_response(500, {"error": "An unexpected server error occurred."})
            
    def do_OPTIONS(self):
        self.send_response(204)
        self._send_cors_headers()
        self.end_headers()
        
    def _send_response(self, status_code, body):
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps(body, ensure_ascii=False).encode('utf-8'))

    def _send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
