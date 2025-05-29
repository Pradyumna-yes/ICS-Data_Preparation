import os
import time
from datetime import datetime
from flask import current_app as app  # Correct way to import app context

def cleanup_temp_files():
    """
    Cleans up files in UPLOAD_FOLDER older than TEMP_FILE_AGE seconds.
    """
    try:
        upload_folder = app.config['UPLOAD_FOLDER']
        max_age = app.config.get('TEMP_FILE_AGE', 3600)  # Fallback to 1 hour
        
        if not os.path.exists(upload_folder):
            print(f"Upload folder {upload_folder} does not exist")
            return
        
        now = time.time()
        deleted_files = []
        
        for filename in os.listdir(upload_folder):
            file_path = os.path.join(upload_folder, filename)
            if os.path.isfile(file_path):
                file_age = now - os.path.getmtime(file_path)
                if file_age > max_age:
                    os.remove(file_path)
                    deleted_files.append(filename)
        
        if deleted_files:
            print(f"[{datetime.now()}] Deleted {len(deleted_files)} files: {', '.join(deleted_files)}")
        else:
            print(f"[{datetime.now()}] No files to delete")
    
    except Exception as e:
        print(f"Error during cleanup: {str(e)}")

if __name__ == '__main__':
    # Ensure Flask app context is available when run standalone
    # This requires your app configuration to be loaded
    from app import app  # Adjust import path to your app module
    with app.app_context():
        cleanup_temp_files()