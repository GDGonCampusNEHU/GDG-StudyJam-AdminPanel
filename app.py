import os
import re
import pandas as pd
from flask import Flask, request, jsonify, render_template
from supabase import create_client, Client
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

# Load environment variables for local development (Vercel uses its own system)
load_dotenv()

# --- Configuration ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
TABLE_NAME = 'participants'

# --- Flask App Initialization ---
app = Flask(__name__)

# --- Supabase Client Initialization ---
supabase: Client = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Successfully connected to Supabase.")
    except Exception as e:
        print(f"Error initializing Supabase client: {e}")
else:
    print("Supabase URL or Key not found. Make sure to set them in Vercel's environment variables.")

# --- Helper Function ---
def normalize_name(name):
    """Creates a standardized, simplified version of a name for robust matching."""
    s = str(name).lower()
    s = s.replace('[skill badge]', '').strip()
    s = s.replace('gen ai', 'genai')
    s = re.sub(r'[^a-z0-9]+', '_', s)
    s = s.strip('_')
    return s

# --- Flask Routes ---
@app.route('/')
def index():
    """Serves the main HTML page."""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles file upload and processing."""
    if not supabase:
        return jsonify({"error": "Supabase client is not initialized. Check server logs."}), 500

    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No file selected"}), 400

    if file:
        filename = secure_filename(file.filename)
        
        try:
            # --- Get Valid Column Names and Create Normalized Map ---
            response = supabase.table(TABLE_NAME).select("*").limit(1).execute()
            if not response.data:
                return jsonify({"error": f"Could not fetch schema. Table '{TABLE_NAME}' might be empty."}), 500
            
            original_columns = response.data[0].keys()
            column_map = {normalize_name(col): col for col in original_columns}
            print(f"Created a normalized map for {len(column_map)} columns.")

            # --- VERCEL CHANGE: Read file directly into memory ---
            # No need to save the file to disk. Pandas can read the file object.
            if filename.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(file)
            else:
                df = pd.read_csv(file)
            
            required_columns = ['User Name', 'User Email', 'Names of Completed Skill Badges']
            if not all(col in df.columns for col in required_columns):
                return jsonify({"error": "File missing required columns: 'User Name', 'User Email', and 'Names of Completed Skill Badges'."}), 400

            update_log = []
            total_updates = 0
            not_found_log = []
            mismatched_labs_log = set()

            for index, row in df.iterrows():
                user_name_raw = row.get('User Name')
                email_raw = row.get('User Email')
                completed_labs_str = row.get('Names of Completed Skill Badges')
                print(user_name_raw,completed_labs_str,"\n")

                if pd.notna(email_raw):
                    email = str(email_raw).strip()
                    print("email: ",email,"\n")
                    user_name = str(user_name_raw).strip() if pd.notna(user_name_raw) else email

                    if pd.notna(completed_labs_str) and str(completed_labs_str).strip():
                        completed_labs = [lab.strip() for lab in str(completed_labs_str).split('|')]
                        
                        update_data = {}
                        for lab_name in completed_labs:
                            if not lab_name: 
                                continue
                            
                            normalized_lab = normalize_name(lab_name)
                            original_column_name = column_map.get(normalized_lab)
                            

                            if original_column_name:
                                update_data[original_column_name] = "Yes"
                            else:
                                mismatched_labs_log.add(f"'{lab_name.strip()}' from file did not match any database column.")

                        if update_data:
                            try:
                               response = supabase.table(TABLE_NAME).update(update_data).eq('email', email).execute()
                               data = response.data
                               print("update for: ",data[0]["name"])
                               count = response.count

                                # This check now works, comparing int > int
                               if count is not None and count > 0: 
                                    num_labs_updated = len(update_data)
                                    total_updates += num_labs_updated
                                    updated_labs_list = list(update_data.keys())
                                    update_log.append(f"Updated {num_labs_updated} labs for '{user_name}': {', '.join(updated_labs_list)}.")
                               else:
                                    update_log.append(f"No new updates for '{user_name}' (data may already be current).")

                            except Exception as e:
                                update_log.append(f"FAILED to update labs for '{user_name}'. Error: {e}")
            
            final_log = {
                "updates": update_log,
                "users_not_found": not_found_log,
                "mismatched_lab_names": sorted(list(mismatched_labs_log))
            }
            print(final_log["updates"])
            success_message = f"Processing complete. Attempted to apply {total_updates} lab completion updates."
            return jsonify({"message": success_message, "details": final_log})

        except Exception as e:
            return jsonify({"error": f"An error occurred while processing the file: {str(e)}"}), 500
            
    return jsonify({"error": "An unknown error occurred during file upload."}), 500

# This part is for local development only and will not be used by Vercel
if __name__ == '__main__':
    app.run(debug=True, port=5001)