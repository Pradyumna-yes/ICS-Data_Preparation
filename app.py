
import os
import pandas as pd
import msoffcrypto
import io
import pyodbc
import re
import datetime
from flask import (
    Flask, 
    render_template, 
    request, 
    redirect, 
    url_for, 
    send_from_directory
)
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

app = Flask(__name__)

# Configuration
app.config['UPLOAD_FOLDER'] = 'uploads/'
app.config['ALLOWED_EXTENSIONS'] = {'xls', 'xlsx'}
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB limit

server = os.getenv('SQL_SERVER', 'CRMServer3')
database = os.getenv('SQL_DATABASE', 'CRM_MSCRM')
password = os.getenv('EXCEL_PASSWORD', 'D2D2025!')

expected_columns = [
    'Trinity Reference', 'Sign up date', 'Date verified by agency', 'Title', 'Forenames', 'Surname', 
    'Address_1', 'Address_2', 'Address_3', 'Town', 'District', 'County', 'Eircode', 'Landline Tel', 
    'Mobile Tel', 'Email', 'DOB', 'Amount', 'Frequency', 'Debtor Name', 'BIC', 'IBAN', 'Tax Status', 
    'Post Opt-In', 'Newsletter Opt-In', 'Email Opt-In', 'Phone Opt-In', 'Field Representative Name', 
    'Source', 'Start Date', 'Welcome Call', 'Channel'
]

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return redirect(url_for('error', message="No file part"))
    
    file = request.files['file']
    if file.filename == '':
        return redirect(url_for('error', message="No selected file"))
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        return process_file(file_path, filename)
    else:
        return redirect(url_for('error', message="Invalid file type"))

def process_file(file_path, filename):
    try:
        # Decrypt and load the Excel file
        with open(file_path, "rb") as file:
            file_decrypted = io.BytesIO()
            office_file = msoffcrypto.OfficeFile(file)
            office_file.load_key(password=password)
            office_file.decrypt(file_decrypted)
        
        df = pd.read_excel(file_decrypted, engine='xlrd', dtype={'Start Date': str})
        
        # Clean the data (remove leading/trailing spaces)
        df.columns = df.columns.str.strip()
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        
        # Save cleaned data to a temporary XLSX file
        cleaned_file_path = os.path.join(app.config['UPLOAD_FOLDER'], f"cleaned_{filename}.xlsx")
        df.to_excel(cleaned_file_path, index=False)
        
        # Load cleaned data into a new DataFrame
        df_new = pd.read_excel(cleaned_file_path, engine='openpyxl')
        
        # Perform validations
        errors = []
        errors += check_columns(df_new)
        errors += check_IBAN_BIC(df_new)
        errors += check_districts(df_new)
        errors += check_date_format(df_new)
        errors += check_opt_in_values(df_new)
        errors += check_welcome_call_values(df_new)
        errors += check_trinity_reference_values(df_new)
        errors += check_special_characters(df_new)
        errors += check_eircode_format(df_new)
        
        # Export cleaned data
        today_date = datetime.datetime.now().strftime('%Y-%m-%d')
        output_file = os.path.join(app.config['UPLOAD_FOLDER'], f"Pledge_Import_{today_date}.csv")
        df_new.to_csv(output_file, index=False)
        
        return render_template('results.html', 
                              filename=filename,
                              errors=errors,
                              output_file=output_file)
    
    except Exception as e:
        return redirect(url_for('error', message=str(e)))

def check_columns(df_new):
    errors = []
    missing_columns = [col for col in expected_columns if col not in df_new.columns]
    if missing_columns:
        errors.append(f"Missing columns: {', '.join(missing_columns)}")
    
    if list(df_new.columns) != expected_columns:
        errors.append("Columns are not in the correct order.")
    
    return errors

def check_IBAN_BIC(df_new):
    errors = []
    # Add your IBAN/BIC validation logic here and append errors
    return errors

def check_districts(df_new):
    errors = []
    try:
        conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};' \
                   f'SERVER={server};DATABASE={database};Trusted_Connection=yes;Encrypt=no'
        conn = pyodbc.connect(conn_str)
        query = "SELECT district_name FROM CRM_MSCRM.[dbo].[ext_BI_FR_DIM_DIST]"
        df_districts = pd.read_sql(query, conn)
        conn.close()
        
        valid_districts = df_districts['district_name'].tolist()
        invalid_districts = df_new[~df_new['District'].isin(valid_districts)]['District'].unique().tolist()
        
        if invalid_districts:
            errors.append(f"Invalid districts: {', '.join(invalid_districts)}")
    except Exception as e:
        errors.append(f"Database error: {str(e)}")
    
    return errors

def check_date_format(df_new):
    errors = []
    # Add date format validation logic
    return errors

def check_opt_in_values(df_new):
    errors = []
    # Add opt-in validation logic
    return errors

def check_welcome_call_values(df_new):
    errors = []
    # Add welcome call validation logic
    return errors

def check_trinity_reference_values(df_new):
    errors = []
    # Add Trinity Reference validation logic
    return errors

def check_special_characters(df_new):
    errors = []
    # Add special character validation logic
    return errors

def check_eircode_format(df_new):
    errors = []
    # Add Eircode validation logic
    return errors

@app.route('/download/<path:filename>')
def download_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)

@app.route('/error')
def error():
    message = request.args.get('message', 'An unknown error occurred')
    return render_template('error.html', message=message)

if __name__ == '__main__':
    if not os.path.exists(app.config['UPLOAD_FOLDER']):
        os.makedirs(app.config['UPLOAD_FOLDER'])
    app.run(debug=True)