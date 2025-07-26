import os
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime

# === CONFIG ===
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCOPES = ['https://www.googleapis.com/auth/drive']
DRIVE_FOLDER_ID = '1M93UsOD7-Edm77CdZGDHkvR3aMmk9isP'  # ⚠️ cần sửa nếu khác
LOCAL_NEW_FILE = 'crypto_full_data.csv'

def upload_to_drive():
    # Kiểm tra file tồn tại và không rỗng
    if not os.path.exists(LOCAL_NEW_FILE):
        print(f"❌ File {LOCAL_NEW_FILE} không tồn tại.")
        return
    if os.path.getsize(LOCAL_NEW_FILE) == 0:
        print(f"❌ File {LOCAL_NEW_FILE} rỗng, không thể đọc.")
        return

    # Đọc dữ liệu để xác nhận hợp lệ
    try:
        df_new = pd.read_csv(LOCAL_NEW_FILE)
        if df_new.empty:
            print("❌ File có header nhưng không có dữ liệu.")
            return
    except Exception as e:
        print(f"❌ Lỗi đọc file CSV: {e}")
        return

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    service = build('drive', 'v3', credentials=credentials)

    now = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"crypto_data_{now}.csv"
    file_metadata = {
        'name': filename,
        'parents': [DRIVE_FOLDER_ID]
    }
    media = MediaFileUpload(LOCAL_NEW_FILE, mimetype='text/csv')

    file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

    print(f"✅ Đã upload file lên Google Drive: {filename}")

# Có thể gọi từ script ngoài
if __name__ == "__main__":
    upload_to_drive()
