import os
import re
import logging
import pandas as pd
import msoffcrypto
#import pyodbc
import io
import uuid
from datetime import datetime
from flask import (
    Flask, render_template, request,
    redirect, url_for, send_from_directory, flash
)
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'default-secret-key')

# Configuration
app.config.update({
    'UPLOAD_FOLDER': 'uploads/',
    'ALLOWED_EXTENSIONS': {'xls', 'xlsx', 'csv'},  # Include CSV files
    'MAX_CONTENT_LENGTH': 16 * 1024 * 1024,       # 16MB max upload size
    'TEMP_FILE_AGE': 3600                         # 1 hour temp files
})

# Database configuration
SERVER = os.getenv('SQL_SERVER', 'CRMServer3')
DATABASE = os.getenv('SQL_DATABASE', 'CRM_MSCRM')

# Expected columns for structural validation
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
        extra = [col for col in df.columns if col not in EXPECTED_COLUMNS]
        
        if missing:
            errors.append({
                'type': 'Structure',
                'message': f'Missing columns: {", ".join(missing)}. '
                           f'Suggestion: Add these columns to your file.'
            })
        if extra:
            errors.append({
                'type': 'Structure',
                'message': f'Unexpected columns found: {", ".join(extra)}. '
                           f'Suggestion: Remove these columns from your file.'
            })
       # if list(df.columns) != EXPECTED_COLUMNS:
        #    errors.append({
         #       'type': 'Structure',
          #      'message': 'Columns are not in the correct order. '
           #                f'Suggested order: {", ".join(EXPECTED_COLUMNS)}'
            ##})
        return errors

    @staticmethod
    def validate_iban_bic(df):
        errors = []
        iban_pattern = re.compile(r'^IE\d{2}[A-Z0-9]{4}\d{14}$', re.IGNORECASE)
        bic_pattern = re.compile(r'^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$')
        
        for idx, row in df.iterrows():
            iban = str(row.get('IBAN', '')).replace(' ', '')
            if not iban_pattern.match(iban):
                errors.append({
                    'type': 'Validation',
                    'row': idx + 2,
                    'field': 'IBAN',
                    'message': f'Invalid IBAN format: {row.get("IBAN", "")}'
                })
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
        # Database connection
        conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;Encrypt=no'
        try:
            conn = pyodbc.connect(conn_str)
            query = "SELECT district_name FROM CRM_MSCRM.[dbo].[ext_BI_FR_DIM_DIST]"
            df_districts = pd.read_sql(query, conn)
            conn.close()
            valid_districts = set(df_districts['district_name'].str.strip())
            for idx, value in df['District'].items():
                if pd.notna(value) and str(value).strip() not in valid_districts:
                    errors.append({
                        'type': 'Validation',
                        'row': idx + 2,
                        'field': 'District',
                        'message': f'Invalid District: {value} (not found in database)'
                    })
        except Exception as e:
            logger.error(f"District validation error: {str(e)}")
            errors.append({
                'type': 'Validation',
                'message': 'Unable to validate Districts due to a database error.'
            })
        return errors    

    @staticmethod
    def validate_dates(df):
        errors = []
        date_fields = ['Sign up date', 'Date verified by agency', 'DOB', 'Start Date']
        
        for field in date_fields:
            for idx, value in df[field].items():
                if pd.isna(value) or value == '':
                    errors.append({
                        'type': 'Validation',
                        'row': idx + 2,
                        'field': field,
                        'message': f'Missing date value'
                    })
                elif not isinstance(value, str) or not re.match(r'\d{4}-\d{2}-\d{2}', str(value)):
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
        valid_values = ['1', '0', 'Nan']
        
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
        eircode_pattern = re.compile(r'^[A-Z0-9]{3} [A-Z0-9]{4}$', re.IGNORECASE)

        for idx, value in df['Eircode'].items():
            if pd.notna(value):
                code = str(value).strip()
                if not eircode_pattern.match(code):
                    errors.append({
                        'type': 'Validation',
                        'row': idx + 2,
                        'field': 'Eircode',
                        'message': f'Invalid Eircode format: {value} (expected format: A11 AA11)'
                    })
        return errors

    @staticmethod
    def get_current_value(df, row, field):
        try:
            return df.iloc[row][field]
        except (IndexError, KeyError):
            return None

def clean_data(df):
    try:
        date_fields = ['Sign up date', 'Date verified by agency', 'DOB', 'Start Date']
        for field in date_fields:
            df[field] = pd.to_datetime(df[field], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
        
        optin_fields = ['Post Opt-In', 'Newsletter Opt-In', 'Email Opt-In', 'Phone Opt-In']
        df[optin_fields] = df[optin_fields].replace({'1': 'Yes', '0': 'No'})
        
        df['Eircode'] = df['Eircode'].str.replace(r'\s+', ' ', regex=True).str.strip().str.upper()
        
        str_cols = df.select_dtypes(include='object').columns
        df[str_cols] = df[str_cols].apply(lambda x: x.str.strip())
        
        return df
    except Exception as e:
        logger.error(f'Data cleaning error: {str(e)}')
        raise

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
    finally:
        # Delete the uploaded file after processing
        if os.path.exists(file_path):
            os.remove(file_path)    

def process_file(file_path, filename, password):
    try:
        if filename.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
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
        
        df = clean_data(df)
        df.columns = df.columns.str.strip()
        validator = DataValidator()
        errors = []
        errors += validator.validate_columns(df)
        errors += validator.validate_iban_bic(df)
        errors += validator.validate_dates(df)
        errors += validator.validate_optins(df)
        errors += validator.validate_eircode(df)
        errors += validator.validate_districts(df)  # Add district validation here
        
        for error in errors:
            if 'row' in error:
                error['current_value'] = DataValidator.get_current_value(
                    df, error['row'] - 2, error['field']
                )
        
        file_id = str(uuid.uuid4())
        temp_file = os.path.join(app.config['UPLOAD_FOLDER'], f"{file_id}_clean.xlsx")
        df.to_excel(temp_file, index=False)
        return render_template('results.html',
                               filename=filename,
                               errors=errors,
                               file_id=file_id)
    except msoffcrypto.exceptions.InvalidKeyError:
        logger.error('Invalid file password')
        flash('Invalid file password')
        return redirect(url_for('index'))
    except Exception as e:
        logger.error(f'Processing error: {str(e)}')
        flash(str(e))
        return redirect(url_for('index'))

@app.route('/apply-fixes', methods=['POST'])
def apply_fixes():
    try:
        # Retrieve file ID from the form
        file_id = request.form['file_id']
        temp_file = os.path.join(app.config['UPLOAD_FOLDER'], f"{file_id}_clean.xlsx")

        # Check if the temporary file exists
        if not os.path.exists(temp_file):
            flash("Session expired. Please upload the file again.")
            return redirect(url_for('index'))

        # Load the temporary file into a DataFrame
        df = pd.read_excel(temp_file)

        # Apply user-provided fixes
        corrections = {}
        for key, value in request.form.items():
            if key.startswith('fix_'):
                parts = key.split('_')
                row_idx = int(parts[1]) - 2  # Convert to zero-based index
                field = parts[2]
                if 0 <= row_idx < len(df):  # Ensure the row index is valid
                    df.at[row_idx, field] = value
                    corrections[(row_idx, field)] = value

        # Re-validate corrected fields
        validator = DataValidator()
        revalidation_errors = []
        for (row_idx, field), value in corrections.items():
            # Create a temporary DataFrame for re-validation
            temp_df = pd.DataFrame([df.iloc[row_idx]])

            if field == 'IBAN' or field == 'BIC':
                revalidation_errors += validator.validate_iban_bic(temp_df)
            elif field in ['Sign up date', 'Date verified by agency', 'DOB', 'Start Date']:
                revalidation_errors += validator.validate_dates(temp_df)
            elif field == 'Eircode':
                revalidation_errors += validator.validate_eircode(temp_df)
            elif field in ['Post Opt-In', 'Newsletter Opt-In', 'Email Opt-In', 'Phone Opt-In']:
                revalidation_errors += validator.validate_optins(temp_df)
            elif field == 'District':
                revalidation_errors += validator.validate_districts(temp_df)

        # If there are re-validation errors, return them to the user
        if revalidation_errors:
            for error in revalidation_errors:
                flash(f"Correction failed validation: {error['message']}")
            return render_template('results.html',
                                   filename=request.form.get('filename'),
                                   errors=revalidation_errors,
                                   file_id=file_id)

        # Save the corrected file
        corrected_filename = f"Corrected_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
        corrected_path = os.path.join(app.config['UPLOAD_FOLDER'], corrected_filename)
        df.to_csv(corrected_path, index=False)

        # Delete the temporary file
        if os.path.exists(temp_file):
            os.remove(temp_file)

        # Return the corrected file for download
        return send_from_directory(
            app.config['UPLOAD_FOLDER'],
            corrected_filename,
            as_attachment=True,
            download_name=corrected_filename
        )

    except Exception as e:
        logger.error(f"Error applying fixes: {str(e)}")
        flash("Error processing corrections. Please try again.")
        return redirect(url_for('index'))

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename, as_attachment=True)

@app.route('/error')
def error():
    message = request.args.get('message', 'An unknown error occurred')
    return render_template('error.html', message=message)

if __name__ == "__main__":
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(
        host='0.0.0.0',  # ✅ Must be set to 0.0.0.0 for external access
        port=int(os.getenv('PORT', 8000)),
        debug=os.getenv('FLASK_DEBUG', False)
    )
