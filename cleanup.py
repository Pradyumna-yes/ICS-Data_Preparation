import os
import time
from __name__ import app  # Import your Flask app configuration

def cleanup_temp_files():
    now = time.time()
    for filename in os.listdir(app.config['UPLOAD_FOLDER']):
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if os.path.isfile(file_path):
            file_age = now - os.path.getmtime(file_path)
            if file_age > app.config['TEMP_FILE_AGE']:
                os.remove(file_path)
                print(f"Deleted stale file: {filename}")

if __name__ == '__main__':
    cleanup_temp_files()