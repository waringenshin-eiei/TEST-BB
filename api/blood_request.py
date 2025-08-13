# api/blood_request.py - Refined to send a dynamic, color-coded LINE Flex Message report.
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

# --- Database Helper ---
def get_db_connection():
    if not POSTGRES_URL: return None
    try:
        return psycopg.connect(POSTGRES_URL)
    except psycopg.OperationalError as e:
        print(f"Database connection error: {e}")
        return None

# --- NEW: Flex Message Notification Function ---
def create_flex_message(request_data):
    """Creates a LINE Flex Message JSON object from the request data."""
    
    blood_type = request_data.get('bloodType', 'unknown').lower()

    # Define color themes based on the blood component
    color_themes = {
        'redcell': {'main': '#B91C1C', 'light': '#FEF2F2'},
        'ffp': {'main': '#B45309', 'light': '#FFFBEB'},
        'cryoprecipitate': {'main': '#0369A1', 'light': '#F0F9FF'},
        'unknown': {'main': '#4B5563', 'light': '#F3F4F6'}
    }
    theme = color_themes.get(blood_type, color_themes['unknown'])

    # Helper to create a row in the Flex Message
    def create_info_row(label, value, is_bold=False):
        return {
            "type": "box",
            "layout": "horizontal",
            "contents": [
                {
                    "type": "text",
                    "text": label,
                    "size": "sm",
                    "color": "#555555",
                    "flex": 1
                },
                {
                    "type": "text",
                    "text": str(value) if value else "-",
                    "size": "sm",
                    "color": "#111111",
                    "align": "end",
                    "weight": "bold" if is_bold else "regular",
                    "flex": 2,
                    "wrap": True
                }
            ]
        }

    flex_message = {
        "type": "flex",
        "altText": f"ยืนยันคำขอใช้เลือด: {request_data.get('request_id', '')}",
        "contents": {
            "type": "bubble",
            "header": {
                "type": "box",
                "layout": "vertical",
                "paddingAll": "20px",
                "backgroundColor": theme['main'],
                "contents": [
                    {
                        "type": "text",
                        "text": "✅ ยืนยันข้อมูลสำเร็จ",
                        "color": "#FFFFFF",
                        "size": "lg",
                        "weight": "bold"
                    },
                    {
                        "type": "text",
                        "text": "Request Confirmed",
                        "color": "#FFFFFFDD",
                        "size": "xs"
                    }
                ]
            },
            "body": {
                "type": "box",
                "layout": "vertical",
                "spacing": "md",
                "backgroundColor": theme['light'],
                "paddingAll": "20px",
                "contents": [
                    {
                        "type": "text",
                        "text": "ข้อมูลคำขอใช้เลือด",
                        "weight": "bold",
                        "size": "xl",
                        "margin": "md",
                        "color": theme['main']
                    },
                    { "type": "separator", "margin": "lg" },
                    create_info_row("Request ID:", request_data.get('request_id'), is_bold=True),
                    create_info_row("ผู้ป่วย (Patient):", f"{request_data.get('patientName')} (HN: {request_data.get('hospitalNumber')})"),
                    create_info_row("หอผู้ป่วย (Ward):", request_data.get('wardName')),
                    { "type": "separator", "margin": "lg" },
                    create_info_row("ผลิตภัณฑ์ (Product):", request_data.get('bloodType', '').upper(), is_bold=True),
                    create_info_row("ชนิดย่อย (Subtype):", request_data.get('subtype')),
                    create_info_row("จำนวน (Quantity):", f"{request_data.get('quantity')} Units"),
                    { "type": "separator", "margin": "lg" },
                    create_info_row("รอบส่ง (Schedule):", request_data.get('deliveryTime')),
                    create_info_row("สถานที่ (Location):", request_data.get('deliveryLocation')),
                    create_info_row("ผู้แจ้ง (Reporter):", request_data.get('reporterName')),
                    { "type": "separator", "margin": "lg" },
                     {
                        "type": "text",
                        "text": "ธนาคารเลือดได้รับข้อมูลของท่านแล้ว และจะดำเนินการโดยเร็วที่สุด",
                        "wrap": True,
                        "size": "xs",
                        "color": "#555555",
                        "margin": "md"
                    }
                ]
            }
        }
    }
    return flex_message

def send_line_flex_notification(user_id, request_data):
    """Sends a rich Flex Message to the specified user."""
    if not LINE_TOKEN:
        print("WARNING: LINE_TOKEN is not configured. Skipping notification.")
        return False
    
    api_url = 'https://api.line.me/v2/bot/message/push'
    headers = {'Content-Type': 'application/json', 'Authorization': f'Bearer {LINE_TOKEN}'}
    
    flex_payload = create_flex_message(request_data)
    payload = {'to': user_id, 'messages': [flex_payload]}

    try:
        response = requests.post(api_url, headers=headers, json=payload, timeout=5)
        response.raise_for_status()
        print(f"✅ LINE Flex Message sent successfully to {user_id}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR: Failed to send LINE Flex Message to {user_id}: {e}")
        if e.response is not None:
            print(f"❌ LINE API Response: {e.response.status_code} - {e.response.text}")
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

            with get_db_connection() as conn:
                if not conn:
                    return self._send_response(503, {"error": "Database service is unavailable."})

                with conn.cursor() as cur:
                    # Fetch the user_id who originally initiated this request
                    cur.execute("SELECT user_id FROM blood_requests WHERE request_id = %s", (request_id,))
                    result = cur.fetchone()
                    if not result:
                        return self._send_response(404, {"error": f"Request ID '{request_id}' not found."})
                    
                    user_id_to_notify = result[0]
                    print(f"INFO: Found original user_id '{user_id_to_notify}' for request_id '{request_id}'")

                    # Update the request record with all the details from the form
                    sql_update = """
                        UPDATE blood_requests SET
                            blood_type = %s, patient_name = %s, hospital_number = %s, ward_name = %s,
                            blood_details = %s, delivery_time = %s, delivery_location = %s,
                            reporter_name = %s, status = 'submitted', request_data = %s, updated_at = %s
                        WHERE request_id = %s
                    """
                    cur.execute(sql_update, (
                        request_data.get('bloodType'), request_data.get('patientName'), request_data.get('hospitalNumber'),
                        request_data.get('wardName'), request_data.get('bloodDetails'), request_data.get('deliveryTime'),
                        request_data.get('deliveryLocation'), request_data.get('reporterName'),
                        json.dumps(request_data), datetime.now(THAILAND_TZ), request_id
                    ))

                    # Insert component details
                    cur.execute("DELETE FROM blood_components WHERE request_id = %s", (request_id,))
                    sql_components = """
                        INSERT INTO blood_components (request_id, component_type, quantity, component_subtype)
                        VALUES (%s, %s, %s, %s)
                    """
                    cur.execute(sql_components, (
                        request_id, request_data.get('bloodType'), request_data.get('quantity'), request_data.get('subtype')
                    ))
                    
                    conn.commit()
                    print(f"✅ DB Update successful for {request_id}")

                    # Send the rich Flex Message confirmation
                    send_line_flex_notification(user_id_to_notify, request_data)

                    self._send_response(200, {"message": "Request updated successfully.", "request_id": request_id})

        except psycopg.Error as db_error:
            print(f"ERROR: Database transaction failed: {db_error}\n{traceback.format_exc()}")
            self._send_response(500, {"error": "A database error occurred during the transaction."})
        except json.JSONDecodeError:
            self._send_response(400, {"error": "Invalid JSON in request body."})
        except Exception as e:
            print(f"ERROR: Unhandled exception in do_POST: {e}\n{traceback.format_exc()}")
            self._send_response(500, {"error": "An unexpected server error occurred."})
            
    def do_OPTIONS(self):
        """Handle CORS preflight requests."""
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
        self.send_header('Access-Control-Allow-Methods', 'POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
