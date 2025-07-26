from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import os
import io
import json

SCOPES = ['https://www.googleapis.com/auth/drive']
FOLDER_ID = "1M93UsOD7-Edm77CdZGDHkvR3aMmk9isP"
FILENAME = "crypto_full_data.csv"
LOCAL_NEW_FILE = "crypto_full_data.csv"

def get_existing_file_id(service):
    query = f"name='{FILENAME}' and '{FOLDER_ID}' in parents and trashed=false"
    results = service.files().list(
        q=query,
        supportsAllDrives=True,
        spaces='drive',
        fields='files(id, name)',
        includeItemsFromAllDrives=True
    ).execute()
    files = results.get('files', [])
    return files[0]['id'] if files else None

def download_drive_file_raw(service, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()

def upload_to_drive():
    if not os.path.exists(LOCAL_NEW_FILE):
        print(f"❌ Không tìm thấy file local: {LOCAL_NEW_FILE}")
        return

    with open(LOCAL_NEW_FILE, "r", encoding="utf-8-sig") as f:
        new_lines = f.readlines()

    if len(new_lines) <= 1:
        print("❌ File mới không có dữ liệu – không upload lên Drive.")
        return

    service_account_info = json.loads(os.environ["GDRIVE_KEY"])
    creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)

    file_id = get_existing_file_id(service)
    if file_id:
        print("📥 Đã tìm thấy file cũ – tải raw để nối dòng")
        try:
            old_bytes = download_drive_file_raw(service, file_id)
            old_text = old_bytes.decode("utf-8-sig")
            old_lines = old_text.splitlines(keepends=True)

            header = old_lines[0]
            combined_lines = old_lines[1:] + new_lines[1:]

            # Loại trùng theo dòng
            seen = set()
            deduped = []
            for line in combined_lines:
                if line not in seen:
                    deduped.append(line)
                    seen.add(line)

            with open(LOCAL_NEW_FILE, "w", encoding="utf-8-sig") as f_out:
                f_out.write(header)
                f_out.writelines(deduped)

            print(f"🔄 Đã gộp & loại trùng: {len(deduped)} dòng mới + header")
        except Exception as e:
            print(f"⚠️ Không thể tải file cũ – dùng file mới. Lý do: {e}")
    else:
        print("📄 Chưa có file cũ – dùng file mới luôn")

    # Upload lên Drive
    media = MediaFileUpload(LOCAL_NEW_FILE, mimetype='text/csv', resumable=False)

    if file_id:
        service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()
        print("✅ Đã cập nhật file trên Drive.")
    else:
        file_metadata = {'name': FILENAME, 'parents': [FOLDER_ID]}
        service.files().create(
            body=file_metadata,
            media_body=media,
            supportsAllDrives=True,
            fields='id'
        ).execute()
        print("✅ Đã tạo file mới trên Drive.")

if __name__ == "__main__":
    upload_to_drive()
