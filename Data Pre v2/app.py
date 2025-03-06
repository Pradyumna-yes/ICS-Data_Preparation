import os
import re
import logging
import pandas as pd
import msoffcrypto
import pyodbc
import io
from datetime import datetime
from flask import (
    Flask, render_template, request,
    redirect, url_for, send_from_directory, flash
)
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'default-secret-key')

# Configuration
app.config.update({
    'UPLOAD_FOLDER': 'uploads/',
    'ALLOWED_EXTENSIONS': {'xls', 'xlsx'},
    'MAX_CONTENT_LENGTH': 16 * 1024 * 1024,  # 16MB
})

# Database configuration
SERVER = os.getenv('SQL_SERVER', 'CRMServer3')
DATABASE = os.getenv('SQL_DATABASE', 'CRM_MSCRM')

EXPECTED_COLUMNS = [
    'Trinity Reference', 'Sign up date', 'Date verified by agency', 'Title',
    'Forenames', 'Surname', 'Address_1', 'Address_2', 'Address_3', 'Town',
    'District', 'County', 'Eircode', 'Landline Tel', 'Mobile Tel', 'Email',
    'DOB', 'Amount', 'Frequency', 'Debtor Name', 'BIC', 'IBAN', 'Tax Status',
    'Post Opt-In', 'Newsletter Opt-In', 'Email Opt-In', 'Phone Opt-In',
    'Field Representative Name', 'Source', 'Start Date', 'Welcome Call', 'Channel'
]

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataValidator:
    @staticmethod
    def validate_columns(df):
        errors = []
        missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
        if missing:
            errors.append({
                'type': 'Structure', 
                'message': f'Missing columns: {", ".join(missing)}'
            })
        
        if list(df.columns) != EXPECTED_COLUMNS:
            errors.append({
                'type': 'Structure', 
                'message': 'Columns are not in correct order'
            })
            
        return errors

    @staticmethod
    def validate_iban_bic(df):
        errors = []
        iban_pattern = re.compile(r'^IE[0-9A-Z]{22}$', re.IGNORECASE)
        bic_pattern = re.compile(r'^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$')

        for idx, row in df.iterrows():
            # IBAN Validation
            iban = str(row.get('IBAN', '')).replace(' ', '')
            if not iban_pattern.match(iban):
                errors.append({
                    'type': 'Validation',
                    'row': idx + 2,
                    'field': 'IBAN',
                    'message': f'Invalid IBAN format: {row.get("IBAN", "")}'
                })

            # BIC Validation
            bic = str(row.get('BIC', ''))
            if not bic_pattern.match(bic):
                errors.append({
                    'type': 'Validation',
                    'row': idx + 2,
                    'field': 'BIC',
                    'message': f'Invalid BIC format: {bic}'
                })
                
        return errors

    @staticmethod
    def validate_districts(df):
        errors = []
        try:
            conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};' \
                       f'SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;Encrypt=no'
            with pyodbc.connect(conn_str) as conn:
                query = "SELECT district_name FROM CRM_MSCRM.[dbo].[ext_BI_FR_DIM_DIST]"
                df_districts = pd.read_sql(query, conn)
                
            valid_districts = df_districts['district_name'].tolist()
            invalid = df[~df['District'].isin(valid_districts)]['District'].dropna().unique()
            
            if len(invalid) > 0:
                errors.append({
                    'type': 'Validation',
                    'message': f'Invalid districts: {", ".join(invalid)}'
                })
                
        except Exception as e:
            errors.append({
                'type': 'System',
                'message': f'District validation failed: {str(e)}'
            })
            
        return errors

    @staticmethod
    def validate_dates(df):
        errors = []
        date_fields = ['Sign up date', 'Date verified by agency', 'DOB', 'Start Date']
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')

        for field in date_fields:
            for idx, value in df[field].items():
                if pd.notna(value):
                    if not date_pattern.match(str(value)):
                        errors.append({
                            'type': 'Validation',
                            'row': idx + 2,
                            'field': field,
                            'message': f'Invalid date format: {value} (expected YYYY-MM-DD)'
                        })
                        
        return errors

    @staticmethod
    def validate_optins(df):
        errors = []
        optin_fields = ['Post Opt-In', 'Newsletter Opt-In', 'Email Opt-In', 'Phone Opt-In']
        valid_values = ['Yes', 'No', 'Pending']

        for field in optin_fields:
            for idx, value in df[field].items():
                if str(value).strip() not in valid_values:
                    errors.append({
                        'type': 'Validation',
                        'row': idx + 2,
                        'field': field,
                        'message': f'Invalid opt-in value: {value} (allowed: {", ".join(valid_values)})'
                    })
                    
        return errors

    @staticmethod
    def validate_eircode(df):
        errors = []
        eircode_pattern = re.compile(r'^[A-Z0-9]{7}$')

        for idx, value in df['Eircode'].items():
            if pd.notna(value):
                code = str(value).replace(' ', '').upper()
                if not eircode_pattern.match(code):
                    errors.append({
                        'type': 'Validation',
                        'row': idx + 2,
                        'field': 'Eircode',
                        'message': f'Invalid Eircode format: {value}'
                    })
                    
        return errors

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(url_for('index'))
    
    file = request.files['file']
    password = request.form.get('password', '')
    
    if file.filename == '':
        flash('No selected file')
        return redirect(url_for('index'))
    
    if not allowed_file(file.filename):
        flash('Invalid file type')
        return redirect(url_for('index'))

    try:
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        excel_password = password or os.getenv('EXCEL_PASSWORD', '')
        return process_file(file_path, filename, excel_password)
    
    except Exception as e:
        logger.error(f'Upload error: {str(e)}')
        flash(str(e))
        return redirect(url_for('index'))

def process_file(file_path, filename, password):
    try:
        with open(file_path, 'rb') as f:
            file_data = io.BytesIO()
            office_file = msoffcrypto.OfficeFile(f)
            
            if office_file.is_encrypted():
                if not password:
                    raise ValueError('Password required for encrypted file')
                
                office_file.load_key(password=password)
                office_file.decrypt(file_data)
            else:
                file_data = io.BytesIO(f.read())
            
            file_data.seek(0)
            df = pd.read_excel(file_data, engine='xlrd', dtype={'Start Date': str})

        # Clean data
        df.columns = df.columns.str.strip()
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

        # Perform validations
        validator = DataValidator()
        errors = []
        errors += validator.validate_columns(df)
        errors += validator.validate_iban_bic(df)
        errors += validator.validate_districts(df)
        errors += validator.validate_dates(df)
        errors += validator.validate_optins(df)
        errors += validator.validate_eircode(df)

        # Generate output
        today_date = datetime.now().strftime('%Y-%m-%d')
        output_filename = f'Pledge_Import_{today_date}.csv'
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
        df.to_csv(output_path, index=False)

        return render_template('results.html',
                             filename=filename,
                             errors=errors,
                             output_file=output_filename)

    except msoffcrypto.exceptions.InvalidKeyError:
        logger.error('Invalid file password')
        flash('Invalid file password')
        return redirect(url_for('index'))
    except Exception as e:
        logger.error(f'Processing error: {str(e)}')
        flash(str(e))
        return redirect(url_for('index'))

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)

@app.route('/error')
def error():
    message = request.args.get('message', 'An unknown error occurred')
    return render_template('error.html', message=message)

if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=os.getenv('FLASK_DEBUG', False))