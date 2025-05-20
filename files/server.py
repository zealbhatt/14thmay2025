import json
import re
import logging
import pymysql
import os
from flask import Flask, request, jsonify, session
from flask_cors import CORS
from datetime import datetime
from dateutil.parser import parse
from autogen import AssistantAgent, UserProxyAgent
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from flask_session import Session  # For server-side session management
import uuid

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}})  # Adjust origin to match your React app
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'your-secret-key')  # Required for sessions
app.config['SESSION_TYPE'] = 'filesystem'  # Use filesystem for session storage
Session(app)

ALLOWED_TIME_SLOTS = {
    "09:00:00": "9:00 AM",
    "11:00:00": "11:00 AM",
    "15:00:00": "3:00 PM",
    "17:00:00": "5:00 PM"
}

config_list = [
    {
        "model": "llama3.2",
        "base_url": "http://localhost:11434/v1",
        "api_key": "ollama",
        "price": [0, 0]
    }
]

SYSTEM_MESSAGE = """
You are a friendly and helpful appointment assistant. Your job is to naturally converse with users to help them book, update, or cancel appointments, and to answer queries about their pre-loaded personal information.

Responsibilities:

1. Detect the user's intent: book, update, cancel, or query personal information.
2. Use the pre-loaded user information from the system (stored in session data), including the combined name (firstName + lastName) for responses and firstName, lastName, custId, patientId for database operations.
3. Handle queries about personal information (e.g., firstName, lastName, custId, patientId, lastVisit, firstVisit, etc.) by retrieving the values from the session data and responding directly.
4. For appointment-related intents, extract and remember all required info throughout the conversation:
   - date (Convert to YYYY-MM-DD, use 2025 for ambiguous dates like "10 April" unless specified otherwise)
   - time (Convert to HH:MM:SS)
   - reason (optional, extract if provided, e.g., "for a headache")

5. Stick to available time slots when offering or accepting time:
   - 9:00 AM - 10:00 AM → 09:00:00
   - 11:00 AM - 12:00 PM → 11:00:00
   - 3:00 PM - 4:00 PM → 15:00:00
   - 5:00 PM - 6:00 PM → 17:00:00

IMPORTANT ABOUT CONTEXT:
- The user's information, including firstName, lastName, custId, patientId, and additional fields like phone, email, gender, practiceId, guarId, specialty, userId, registrationDate, lastVisit, and firstVisit, is pre-loaded from a JSON file and available in the session data.
- The combined name (firstName + lastName) is used in conversational responses and emails.
- For queries about personal information (e.g., "What's my last visit?"), retrieve the value from session data. If the field is empty or missing, respond with "I don't have that information."
- Do not ask for firstName or lastName unless the system indicates they are missing or invalid.
- Always consider the last 10 messages to maintain conversation continuity.
- Remember information the user has already provided across multiple messages.
- If you're in the middle of a booking flow and have partial information, don't ask for it again.
- When a user refers to something mentioned earlier, check the conversation history before asking for clarification.
- If a user switches topics mid-conversation (e.g., from booking to querying lastVisit), acknowledge the change and adapt smoothly.

Output Format (Always use this JSON structure, even for queries):

{
  "extracted": {
    "intent": "book|update|cancel|query|null",
    "name": "pre-loaded combined name or null",
    "date": "YYYY-MM-DD or null",
    "time": "HH:MM:SS or null",
    "reason": "reason or null",
    "old_date": "YYYY-MM-DD or null",
    "old_time": "HH:MM:SS or null"
  },
  "missing_fields": ["list of required fields still missing for appointment intents, exclude firstName, lastName, custId, patientId unless explicitly needed"],
  "response": "Your natural language reply to the user"
}

Conversation Guidelines:
- Maintain a friendly, conversational tone.
- For queries about personal information (e.g., "What's my customer ID?"), respond with the value from session data (e.g., "Your customer ID is 12345.") or "I don't have that information" if the field is empty or missing.
- For appointment intents, ask for only one missing field at a time (e.g., date, time), excluding firstName, lastName, custId, patientId unless they are missing.
- Use the pre-loaded combined name (firstName + lastName) in all responses and email notifications.
- Use firstName, lastName, custId, patientId for database operations.
- Additional fields like phone, email, etc., are available in session data and can be queried or used if relevant (e.g., email for notifications).
- Always convert date/time to the correct format (YYYY-MM-DD, HH:MM:SS).
- For dates like "10 April", assume 2025 unless the user specifies another year.
- If info is unclear or ambiguous, ask for clarification.
- If all required info is collected for an appointment, confirm the details.
- If the user includes a reason (e.g., "for a headache"), include it in the extracted data and do not list it as a missing field.
- In the "response" field: write only natural language — no code, no JSON.
- Never change the detected intent unless the user explicitly says so.
- Once a user says "cancel," do not switch intent even if more info is given.
- When asking for time, offer only the available slots.
- If a time is given that doesn't match, politely ask the user to pick from the available options.
- Always return complete, valid JSON with all braces properly closed.
- Once the user's intent is identified (book, update, or cancel), do not change it later unless the user clearly changes their request. Never assume a new intent just because more information is provided.
- Never change "cancel" to "update" unless the user explicitly says "I'd like to reschedule" or "I'd like to update".

For Query Intent:
1. Identify if the user is asking about a specific field (e.g., "What's my name?", "Tell me my last visit").
2. Retrieve the value from session data (e.g., name, lastVisit).
3. Respond with the value (e.g., "Your name is John Doe.", "Your last visit was on 2024-12-01.") or "I don't have that information" if the field is empty or missing.
4. Set intent to "query" in the JSON output and leave other extracted fields as null unless relevant.

For Update Appointments:
1. Use the pre-loaded patientId and ask for the old_date and old_time to fetch the current appointment.
2. After getting old_date and old_time, fetch the current appointment and show it to the user.
3. If found, ask for new_date and new_time to reschedule.
4. If not found, suggest creating a new appointment.
5. Offer only available time slots:
   - 9:00 AM - 10:00 AM → 09:00:00
   - 11:00 AM - 12:00 PM → 11:00:00
   - 3:00 PM - 4:00 PM → 15:00:00
   - 5:00 PM - 6:00 PM → 17:00:00
6. Update with new_date/new_time while preserving other details.

For Cancel Appointments:
1. Use the pre-loaded firstName, lastName, and patientId to fetch the current appointment.
2. Confirm the appointment details with the user.
3. Then ask for date and time to ensure precise appointment cancellation.
4. Once all required details are confirmed, proceed with cancellation.
5. After successful cancellation, confirm it with the user.

Response Behavior Based on System Feedback:

If the system informs you that:
- The appointment is confirmed: thank the user and confirm it (an email will be sent).
- The slot is already taken: apologize and ask for another time.
- The appointment is canceled: acknowledge politely (an email will be sent).
- The appointment is updated: confirm and thank the user (an email will be sent).
- The appointment wasn't found: explain and ask if they'd like to create a new one.
- There was a database error: apologize and suggest trying again later.

❗ Important: If the system gives you a status like CONFIRMED, SLOT_TAKEN, etc., naturally integrate that info into your "response" instead of treating it as a separate message.
❗ Always ensure the JSON output is complete with all braces closed (e.g., { ... }).
"""

conversation_agent = AssistantAgent(
    name="conversation_agent",
    system_message=SYSTEM_MESSAGE,
    llm_config={
        "config_list": config_list,
        "temperature": 0
    }
)

user_proxy = UserProxyAgent(
    name="user",
    is_termination_msg=lambda x: "}" in x.get("content", ""),
    human_input_mode="NEVER",
    code_execution_config={"use_docker": False}
)

def load_user_info_from_json(file_path="user_data.json"):
    """
    Load user information from a JSON file, including firstName, lastName, and additional fields.
    Args:
        file_path (str): Path to the JSON file.
    Returns:
        dict or None: Dictionary with user info if valid, else None.
    """
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f)
                first_name = data.get('firstName', '')
                last_name = data.get('lastName', '')
                if first_name and last_name and isinstance(first_name, str) and isinstance(last_name, str):
                    user_info = {
                        "name": f"{first_name} {last_name}",
                        "firstName": first_name,
                        "lastName": last_name,
                        "custId": data.get("custId", ""),
                        "phone": data.get("phone", ""),
                        "email": data.get("email", ""),
                        "gender": data.get("gender", ""),
                        "practiceId": data.get("practiceId", ""),
                        "patientId": data.get("patientId", ""),
                        "guarId": data.get("guarId", ""),
                        "specialty": data.get("specialty", ""),
                        "userId": data.get("userId", ""),
                        "registrationDate": data.get("registrationDate", ""),
                        "lastVisit": data.get("lastVisit", ""),
                        "firstVisit": data.get("firstVisit", "")
                    }
                    logging.debug(f"Loaded user info from JSON: {user_info}")
                    return user_info
                else:
                    logging.warning("Missing or invalid 'firstName' or 'lastName' in JSON file")
                    return None
        else:
            logging.warning(f"JSON file not found at {file_path}")
            return None
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON format in {file_path}: {e}")
        return None

def connect_to_mysql():
    try:
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="Zeal@94269",
            database="appointments_db",
            cursorclass=pymysql.cursors.DictCursor
        )
        logging.debug("Successfully connected to MySQL database")
        return connection
    except pymysql.MySQLError as e:
        logging.error(f"Database connection failed: {e}")
        raise

def validate_time_slot(time):
    return time in ALLOWED_TIME_SLOTS

def validate_datetime(date, time, intent=None):
    try:
        dt = datetime.strptime(date, "%Y-%m-%d")
        datetime.strptime(time, "%H:%M:%S")
        if dt.date() < datetime.now().date() and intent != "cancel":
            logging.warning(f"Past date rejected: {date}")
            return False
        return validate_time_slot(time)
    except ValueError:
        return False

def parse_datetime(date_str, time_str):
    try:
        dt = parse(f"{date_str} {time_str}", fuzzy=True, default=datetime(2025, 1, 1))
        formatted_time = dt.strftime("%H:%M:%S")
        if validate_time_slot(formatted_time):
            return dt.strftime("%Y-%m-%d"), formatted_time
        return None, None
    except ValueError:
        return None, None

def send_appointment_email(name, intent, date, time, reason, email=None):
    try:
        fixed_email = "debajitpadhi@gmail.com"  # Fallback email
        sender_email = "debajitpadhiyt@gmail.com"
        app_password = os.getenv("GMAIL_APP_PASSWORD", "spza brvk kqag xerf")
        target_email = email if email else fixed_email
        formatted_time = ALLOWED_TIME_SLOTS.get(time, time)
        subject = f"Appointment {intent.capitalize()}"
        body = f"""
Dear {name},

Your appointment has been successfully {intent}.

Details:
- Date: {date}
- Time: {formatted_time}
- Reason: {reason or 'checkup'}

Thank you for using our appointment system!

Best regards,
Appointment Assistant
        """

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = target_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, app_password)
            server.sendmail(sender_email, target_email, msg.as_string())

        logging.info(f"Email sent to {target_email} for {intent} on {date} at {time}")
        return True
    except Exception as e:
        logging.error(f"Failed to send email to {target_email}: {e}")
        return False

def process_appointment(data):
    logging.debug(f"Entering process_appointment with data: {data}")
    intent = data.get('intent')
    first_name = data.get('firstName')
    last_name = data.get('lastName')
    cust_id = data.get('custId')
    patient_id = data.get('patientId')
    date = data.get('date')
    time = data.get('time')
    reason = data.get('reason', '')
    old_date = data.get('old_date')
    old_time = data.get('old_time')

    # Normalize date format (strip time component if present)
    if date and 'T' in date:
        try:
            date_obj = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
            date = date_obj.strftime("%Y-%m-%d")
            logging.debug(f"Normalized date from {data.get('date')} to {date}")
        except ValueError:
            logging.warning(f"Invalid date format: {date}")
            return "INVALID_DATETIME"

    if old_date and 'T' in old_date:
        try:
            date_obj = datetime.strptime(old_date, "%Y-%m-%dT%H:%M:%S")
            old_date = date_obj.strftime("%Y-%m-%d")
            logging.debug(f"Normalized old_date from {data.get('old_date')} to {old_date}")
        except ValueError:
            logging.warning(f"Invalid old_date format: {old_date}")
            return "INVALID_DATETIME"

    if not all([first_name, last_name, cust_id, patient_id]):
        logging.warning("Missing required fields for appointment processing")
        return "MISSING_INFO"

    connection = None
    cursor = None
    try:
        connection = connect_to_mysql()
        cursor = connection.cursor()
        logging.info(f"Processing appointment: {data}")

        if intent == "book":
            if not all([first_name, last_name, cust_id, patient_id, date, time]):
                logging.warning("Missing info for booking")
                return "MISSING_INFO"
            if not validate_datetime(date, time):
                logging.warning(f"Invalid datetime: {date} {time}")
                return "INVALID_DATETIME"
            cursor.execute("SELECT COUNT(*) AS count FROM appointments WHERE date = %s AND time = %s", (date, time))
            if cursor.fetchone()["count"] > 0:
                logging.info(f"Slot taken: {date} {time}")
                return f"SLOT_TAKEN:{date}:{time}"
            cursor.execute(
                "INSERT INTO appointments (firstName, lastName, custId, patientId, date, time, reason) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (first_name, last_name, cust_id, patient_id, date, time, reason)
            )
            connection.commit()
            logging.info(f"Appointment booked: {date} {time}")
            send_appointment_email(data.get('name'), "booked", date, time, reason, data.get('email'))
            return f"CONFIRMED:{date}:{time}"

        elif intent == "cancel":
            if not all([first_name, last_name, patient_id, date, time]):
                logging.warning("Missing info for cancellation")
                return "MISSING_INFO"
            if not validate_time_slot(time):
                logging.warning(f"Invalid time: {time}")
                return "INVALID_DATETIME"
            cursor.execute(
                "SELECT id FROM appointments WHERE firstName = %s AND lastName = %s AND patientId = %s AND date = %s AND time = %s",
                (first_name, last_name, patient_id, date, time)
            )
            appt = cursor.fetchone()
            if appt:
                cursor.execute("DELETE FROM appointments WHERE id = %s", (appt["id"],))
                connection.commit()
                logging.info(f"Appointment canceled: {date} {time}")
                send_appointment_email(data.get('name'), "canceled", date, time, reason, data.get('email'))
                return f"CANCELED:{date}:{time}"
            else:
                logging.info(f"Appointment not found: {first_name} {last_name} {patient_id} {date} {time}")
                return f"NOT_FOUND:{date}:{time}"

        elif intent == "update":
            if not all([patient_id, old_date, old_time]):
                logging.warning("Missing required fields for update (old_date, old_time, patient_id)")
                return "MISSING_INFO"

            # Fetch the existing appointment
            cursor.execute(
                "SELECT id, reason FROM appointments WHERE date = %s AND time = %s AND patientId = %s",
                (old_date, old_time, patient_id)
            )
            appt = cursor.fetchone()
            if not appt:
                logging.info(f"No appointment found for {patient_id} on {old_date} at {old_time}")
                return f"NOT_FOUND:{old_date}:{old_time}"

            if not date or not time:
                # Return fetched appointment details
                logging.debug(f"Fetched appointment: {appt}")
                return f"FETCHED:{old_date}:{old_time}:{appt['reason'] or ''}"

            # Validate new date and time
            if not validate_datetime(date, time):
                logging.warning(f"Invalid new datetime: {date} {time}")
                return "INVALID_DATETIME"

            # Check if the new slot is available
            cursor.execute(
                "SELECT COUNT(*) AS count FROM appointments WHERE date = %s AND time = %s AND id != %s",
                (date, time, appt["id"])
            )
            if cursor.fetchone()["count"] > 0:
                logging.info(f"Slot taken: {date} {time}")
                return f"SLOT_TAKEN:{date}:{time}"

            # Update the appointment
            cursor.execute(
                "UPDATE appointments SET date = %s, time = %s, reason = %s WHERE id = %s",
                (date, time, reason or appt['reason'], appt["id"])
            )
            connection.commit()
            logging.info(f"Appointment updated: {date} {time}")
            send_appointment_email(data.get('name'), "updated", date, time, reason or appt['reason'], data.get('email'))
            return f"UPDATED:{date}:{time}"

        else:
            logging.warning(f"Invalid intent: {intent}")
            return "INVALID_INTENT"

    except pymysql.MySQLError as e:
        logging.error(f"Database error: {e}")
        return f"DB_ERROR:{str(e)}"
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
            logging.debug("Database connection closed")

def extract_response_from_json(text):
    try:
        json_match = re.search(r'{[\s\S]*}', text)
        if json_match:
            json_str = json_match.group(0)
            json_str = re.sub(r'//.*', '', json_str)
            if not json_str.endswith('}'):
                json_str = json_str.rstrip() + '}'
            result = json.loads(json_str)
            logging.debug(f"Extracted JSON: {result}")
            return result.get("response", text)
        logging.warning("No JSON found in response")
        return text
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}")
        response_match = re.search(r'"response"\s*:\s*"([^"]+)"', text)
        if response_match:
            return response_match.group(1)
        return "Sorry, I ran into a technical issue while processing that. Could you try again?"

def get_recent_conversation_context(messages, n=10):
    recent_messages = messages[-n:] if len(messages) > n else messages
    context = []
    for msg in recent_messages:
        role = "User" if msg["role"] == "user" else "Assistant"
        context.append(f"{role}: {msg['content']}")
    return "\n".join(context)

def handle_info_query(user_input):
    """
    Check if the user is querying a specific field and return the response if found.
    Returns: (response, intent) or (None, None) if not a query.
    """
    field_mappings = {
        "name": "name",
        "first name": "firstName",
        "last name": "lastName",
        "customer id": "custId",
        "cust id": "custId",
        "patient id": "patientId",
        "phone": "phone",
        "email": "email",
        "gender": "gender",
        "practice id": "practiceId",
        "guarantor id": "guarId",
        "guar id": "guarId",
        "specialty": "specialty",
        "user id": "userId",
        "registration date": "registrationDate",
        "last visit": "lastVisit",
        "first visit": "firstVisit"
    }

    user_input_lower = user_input.lower().strip()
    for query_key, field in field_mappings.items():
        if re.search(rf"(what's|what is|tell me|give me)\s+(my\s+)?{query_key}", user_input_lower):
            value = session.get('appointment_data', {}).get(field, "")
            if value:
                if field == "name":
                    response = f"Your name is {value}."
                elif field in ["lastVisit", "firstVisit", "registrationDate"]:
                    response = f"Your {query_key} was on {value}."
                else:
                    response = f"Your {query_key} is {value}."
            else:
                response = f"I don’t have that information for your {query_key}."
            logging.info(f"Handled query for {field}: {response}")
            return response, "query"
    return None, None

def initialize_session():
    """
    Initialize the session with default messages and appointment data.
    Load user info from JSON file if not already loaded.
    """
    if 'messages' not in session:
        session['messages'] = [{
            "role": "assistant",
            "content": "👋 Hello! I'm your appointment assistant. How can I help you today?"
        }]
        session['appointment_data'] = {
            "intent": None,
            "name": None,
            "firstName": None,
            "lastName": None,
            "custId": None,
            "phone": None,
            "email": None,
            "gender": None,
            "practiceId": None,
            "patientId": None,
            "guarId": None,
            "specialty": None,
            "userId": None,
            "registrationDate": None,
            "lastVisit": None,
            "firstVisit": None,
            "date": None,
            "time": None,
            "reason": None,
            "old_date": None,
            "old_time": None
        }
        # Load user info from JSON file
        user_info = load_user_info_from_json("user_data.json")
        if user_info:
            for key, value in user_info.items():
                session['appointment_data'][key] = value
            logging.info(f"Initialized session with user info from JSON: {user_info['name']}")
        else:
            logging.info("No user info loaded from JSON; will prompt for name")
            session['messages'].append({
                "role": "assistant",
                "content": "I couldn't find your name. Could you please tell me your name?"
            })
        session.modified = True

@app.route('/api/message', methods=['POST'])
def handle_message():
    """
    Handle incoming user messages from the React frontend.
    Process the message, interact with the assistant, and handle database operations.
    """
    initialize_session()
    data = request.get_json()
    user_input = data.get('message')
    if not user_input:
        return jsonify({"error": "No message provided"}), 400

    logging.debug(f"User input: {user_input}")
    session['messages'].append({"role": "user", "content": user_input})
    session.modified = True

    logging.info(f"Session state before processing: {session['appointment_data']}")
    recent_context = get_recent_conversation_context(session['messages'], 10)

    conversation_context = f"""
Recent Conversation:
{recent_context}

User's latest message: "{user_input}"

Current appointment data:
{json.dumps(session['appointment_data'], indent=2)}
"""

    db_result = None

    # Check for info queries first
    query_response, query_intent = handle_info_query(user_input)
    if query_response:
        human_response = query_response
        session['messages'].append({"role": "assistant", "content": human_response})
        session['appointment_data']['intent'] = query_intent
        session.modified = True
        logging.info(f"Session state after query: {session['appointment_data']}")
        return jsonify({
            "response": human_response,
            "messages": session['messages']
        })
    else:
        # Handle manual name input if JSON failed
        if user_input and not session['appointment_data']['name']:
            user_info = load_user_info_from_json("user_data.json")
            if not user_info:
                extracted_name = re.search(r'\b[A-Za-z\s]+\b', user_input, re.IGNORECASE)
                if extracted_name:
                    full_name = extracted_name.group(0).strip()
                    session['appointment_data']['name'] = full_name
                    session['appointment_data']['firstName'] = full_name.split()[0]
                    session['appointment_data']['lastName'] = full_name.split()[-1] if len(full_name.split()) > 1 else ""
                    session['appointment_data']['custId'] = "UNKNOWN"
                    session['appointment_data']['patientId'] = "UNKNOWN"
                    logging.info(f"Manually set name from user input: {full_name}")
                    session.modified = True

        user_proxy.initiate_chat(conversation_agent, message=conversation_context)
        response_content = user_proxy.chat_messages[conversation_agent][-1]["content"]
        logging.debug(f"Raw LLM response: {response_content}")
        human_response = extract_response_from_json(response_content)

        session['messages'].append({"role": "assistant", "content": human_response})
        session.modified = True

        try:
            json_match = re.search(r'{[\s\S]*}', response_content)
            if json_match:
                json_str = json_match.group(0)
                json_str = re.sub(r'//.*', '', json_str)
                if not json_str.endswith('}'):
                    json_str = json_str.rstrip() + '}'
                result = json.loads(json_str)
                extracted = result.get("extracted", {})
                valid_intents = ["book", "update", "cancel", "query", None]
                if extracted.get("intent") not in valid_intents:
                    logging.warning(f"Invalid intent received: {extracted.get('intent')}. Retaining existing intent.")
                    extracted["intent"] = session['appointment_data'].get("intent")

                # Preserve pre-loaded name
                if session['appointment_data'].get("name"):
                    extracted["name"] = session['appointment_data']['name']
                    logging.info(f"Using pre-loaded name: {extracted['name']}")

                # Normalize date format (strip time component if present)
                for date_field in ["date", "old_date"]:
                    if extracted.get(date_field):
                        try:
                            # Handle date with time (e.g., "2025-05-25T09:00:00")
                            date_obj = datetime.strptime(extracted[date_field], "%Y-%m-%dT%H:%M:%S")
                            extracted[date_field] = date_obj.strftime("%Y-%m-%d")
                        except ValueError:
                            # If date is already in YYYY-MM-DD, keep it
                            if re.match(r"\d{4}-\d{2}-\d{2}", extracted[date_field]):
                                pass
                            else:
                                logging.warning(f"Invalid {date_field} format in extracted data: {extracted[date_field]}")
                                extracted[date_field] = None

                for k, v in extracted.items():
                    if v is not None and k in session['appointment_data']:
                        logging.info(f"Updating session data: {k} = {v}")
                        session['appointment_data'][k] = v
                        session.modified = True

                if (not extracted.get("date") or not extracted.get("time")) and user_input:
                    parsed_date, parsed_time = parse_datetime(user_input, user_input)
                    if parsed_date:
                        if extracted.get("intent") == "update" and not extracted.get("old_date"):
                            extracted["old_date"] = parsed_date
                            session['appointment_data']['old_date'] = parsed_date
                            logging.info(f"Parsed fallback old_date: {parsed_date}")
                        else:
                            extracted["date"] = parsed_date
                            session['appointment_data']['date'] = parsed_date
                            logging.info(f"Parsed fallback date: {parsed_date}")
                    if parsed_time:
                        if extracted.get("intent") == "update" and not extracted.get("old_time"):
                            extracted["old_time"] = parsed_time
                            session['appointment_data']['old_time'] = parsed_time
                            logging.info(f"Parsed fallback old_time: {parsed_time}")
                        else:
                            extracted["time"] = parsed_time
                            session['appointment_data']['time'] = parsed_time
                            logging.info(f"Parsed fallback time: {parsed_time}")
                    session.modified = True

                # Adjust missing_fields to exclude pre-loaded fields
                missing_fields = result.get("missing_fields", [])
                pre_loaded_fields = ["name", "firstName", "lastName", "custId", "phone", "email", "gender",
                                     "practiceId", "patientId", "guarId", "specialty", "userId",
                                     "registrationDate", "lastVisit", "firstVisit"]
                for field in pre_loaded_fields:
                    if session['appointment_data'].get(field) and field in missing_fields:
                        missing_fields.remove(field)
                        logging.debug(f"Removed '{field}' from missing_fields as it is pre-loaded")

                # Trigger database query for update when old_date and old_time are provided
                if session['appointment_data'].get('intent') == "update":
                    if all(session['appointment_data'].get(field) for field in ['patientId', 'old_date', 'old_time']):
                        logging.debug("Triggering update fetch with old_date and old_time")
                        db_result = process_appointment({
                            "intent": "update",
                            "firstName": session['appointment_data'].get('firstName'),
                            "lastName": session['appointment_data'].get('lastName'),
                            "patientId": session['appointment_data'].get('patientId'),
                            "name": session['appointment_data'].get('name'),
                            "email": session['appointment_data'].get('email'),
                            "old_date": session['appointment_data'].get('old_date'),
                            "old_time": session['appointment_data'].get('old_time')
                        })
                        if db_result and "FETCHED:" in db_result:
                            _, date, time, reason = db_result.split(":", 3)
                            session['appointment_data'].update({
                                "current_date": date,
                                "current_time": time,
                                "reason": reason if reason else None
                            })
                            session.modified = True
                            logging.debug(f"Updated session state after fetch: {session['appointment_data']}")

                # Trigger database query for book, cancel, or update when all required fields are present
                if session['appointment_data'].get('intent') in ["book", "cancel", "update"]:
                    required_fields = ["firstName", "lastName", "custId", "patientId", "date", "time"]
                    if session['appointment_data'].get('intent') == "update":
                        required_fields = ["firstName", "lastName", "patientId", "old_date", "old_time", "date", "time"]
                    if all(session['appointment_data'].get(field) for field in required_fields):
                        logging.debug(f"Triggering {session['appointment_data'].get('intent')} query with complete data")
                        db_result = process_appointment(session['appointment_data'])

                if db_result:
                    db_status = db_result.split(":", 1)[0] if ":" in db_result else db_result
                    db_context = ""
                    if db_status == "FETCHED":
                        _, date, time, reason = db_result.split(":", 3)
                        formatted_time = ALLOWED_TIME_SLOTS.get(time, time)
                        db_context = f"I found your appointment on {date} at {formatted_time}. Reason: {reason or 'None'}. Please provide the new date for rescheduling (e.g., 2025-05-20)."
                    elif db_status in ["CONFIRMED", "CANCELED", "UPDATED", "SLOT_TAKEN", "NOT_FOUND"]:
                        if ":" in db_result and db_status != "NOT_FOUND":
                            parts = db_result.split(":")
                            date = parts[1] if len(parts) > 1 else ""
                            time = parts[2] if len(parts) > 2 else ""
                            formatted_time = ALLOWED_TIME_SLOTS.get(time, time)
                            date_time = f"{date} at {formatted_time}" if date and time else ""
                            db_context = {
                                "CONFIRMED": f"The appointment has been successfully confirmed for {date_time}.",
                                "CANCELED": f"The appointment for {date_time} has been successfully canceled.",
                                "UPDATED": f"The appointment has been successfully updated to {date_time}.",
                                "SLOT_TAKEN": f"The requested slot at {date_time} is already booked."
                            }.get(db_status, "")
                        elif db_status == "NOT_FOUND":
                            parts = db_result.split(":")
                            date = parts[1] if len(parts) > 1 else session['appointment_data'].get('old_date', '') if session['appointment_data'].get('intent') == "update" else session['appointment_data'].get('date', '')
                            time = parts[2] if len(parts) > 2 else session['appointment_data'].get('old_time', '') if session['appointment_data'].get('intent') == "update" else session['appointment_data'].get('time', '')
                            formatted_time = ALLOWED_TIME_SLOTS.get(time, time)
                            date_time = f"{date} at {formatted_time}" if date and time else "the specified time"
                            db_context = f"No appointment found for {date_time}. Would you like to book a new one?"
                    conversation_context += f"\nDatabase result: {db_context}"
                    logging.debug(f"Database result context: {db_context}")

                    # Update the assistant's response based on database result
                    name = session['appointment_data'].get('name', 'User')
                    if db_status == "CONFIRMED":
                        human_response = f"Great, {name}! Your appointment is confirmed for {date} at {formatted_time}."
                    elif db_status == "SLOT_TAKEN":
                        human_response = f"Sorry, the slot on {date} at {formatted_time} is already taken. Please choose another time from: 9:00 AM, 11:00 AM, 3:00 PM, or 5:00 PM."
                    elif db_status == "NOT_FOUND" and session['appointment_data'].get('intent') in ["cancel", "update"]:
                        human_response = f"No appointment found for {name} on {date_time}. Would you like to book a new one?"
                    elif db_status == "CANCELED":
                        human_response = f"Your appointment on {date} at {formatted_time} has been canceled."
                    elif db_status == "UPDATED":
                        human_response = f"Your appointment has been updated to {date} at {formatted_time}."
                    elif db_status == "FETCHED":
                        human_response = db_context
                    elif db_status == "DB_ERROR":
                        human_response = "Sorry, there was an issue with the database. Please try again later."

                    session['messages'][-1]["content"] = human_response
                    session.modified = True

                if db_result and db_status in ["CONFIRMED", "CANCELED", "UPDATED"]:
                    logging.info("Resetting session data after successful operation")
                    preserved_data = {
                        "name": session['appointment_data']['name'],
                        "firstName": session['appointment_data']['firstName'],
                        "lastName": session['appointment_data']['lastName'],
                        "custId": session['appointment_data']['custId'],
                        "phone": session['appointment_data']['phone'],
                        "email": session['appointment_data']['email'],
                        "gender": session['appointment_data']['gender'],
                        "practiceId": session['appointment_data']['practiceId'],
                        "patientId": session['appointment_data']['patientId'],
                        "guarId": session['appointment_data']['guarId'],
                        "specialty": session['appointment_data']['specialty'],
                        "userId": session['appointment_data']['userId'],
                        "registrationDate": session['appointment_data']['registrationDate'],
                        "lastVisit": session['appointment_data']['lastVisit'],
                        "firstVisit": session['appointment_data']['firstVisit']
                    }
                    session['appointment_data'] = {
                        "intent": None,
                        "date": None,
                        "time": None,
                        "reason": None,
                        "old_date": None,
                        "old_time": None,
                        **preserved_data
                    }
                    session.modified = True
                elif db_result and db_status == "FETCHED":
                    _, date, time, reason = db_result.split(":", 3)
                    logging.info(f"Updating session with fetched data: date={date}, time={time}, reason={reason}")
                    if session['appointment_data'].get('intent') == "cancel":
                        session['appointment_data'].update({
                            "date": date,
                            "time": time,
                            "reason": reason if reason else None
                        })
                    else:
                        session['appointment_data'].update({
                            "current_date": date,
                            "current_time": time,
                            "reason": reason if reason else None
                        })
                    session.modified = True
        except Exception as e:
                logging.error(f"Error updating session data: {e}")
                human_response = "Sorry, there was an issue processing your request. Please try again."
                session['messages'][-1]["content"] = human_response
                session.modified = True

        logging.info(f"Session state after processing: {session['appointment_data']}")
        return jsonify({
            "response": human_response,
            "messages": session['messages']
        })

@app.route('/api/history', methods=['GET'])
def get_history():
    """
    Return the conversation history.
    """
    initialize_session()
    return jsonify({"messages": session['messages']})

@app.route('/api/reset', methods=['POST'])
def reset_conversation():
    """
    Reset the conversation while preserving user info.
    """
    initialize_session()
    preserved_data = {
        "name": session['appointment_data'].get("name"),
        "firstName": session['appointment_data'].get("firstName"),
        "lastName": session['appointment_data'].get("lastName"),
        "custId": session['appointment_data'].get("custId"),
        "phone": session['appointment_data'].get("phone"),
        "email": session['appointment_data'].get("email"),
        "gender": session['appointment_data'].get("gender"),
        "practiceId": session['appointment_data'].get("practiceId"),
        "patientId": session['appointment_data'].get("patientId"),
        "guarId": session['appointment_data'].get("guarId"),
        "specialty": session['appointment_data'].get("specialty"),
        "userId": session['appointment_data'].get("userId"),
        "registrationDate": session['appointment_data'].get("registrationDate"),
        "lastVisit": session['appointment_data'].get("lastVisit"),
        "firstVisit": session['appointment_data'].get("firstVisit")
    }
    session['messages'] = [{
        "role": "assistant",
        "content": "👋 Hello! I'm your appointment assistant. How can I help you today?"
    }]
    session['appointment_data'] = {
        "intent": None,
        "date": None,
        "time": None,
        "reason": None,
        "old_date": None,
        "old_time": None,
        **preserved_data
    }
    session.modified = True
    return jsonify({"message": "Conversation reset", "messages": session['messages']})

@app.route('/api/debug', methods=['GET'])
def debug_info():
    """
    Return session data for debugging purposes.
    """
    initialize_session()
    recent = session['messages'][-10:] if len(session['messages']) > 10 else session['messages']
    return jsonify({
        "appointment_data": session['appointment_data'],
        "recent_messages": recent,
        "full_messages": session['messages']
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)