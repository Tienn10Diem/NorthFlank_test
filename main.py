import pandas as pd
import requests
import time
from datetime import datetime
import upload_DR
import os

# === CONFIG ===
INPUT_FILE = '500_coins.csv'
OUTPUT_FILE = 'crypto_full_data.csv'
BATCH_SIZE = 100

def get_price_data(ids, retries=3):
    url = 'https://api.coingecko.com/api/v3/simple/price'
    params = {
        'ids': ','.join(ids),
        'vs_currencies': 'usd',
        'include_market_cap': 'true',
        'include_24hr_vol': 'true',
        'include_24hr_change': 'true',
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MyCryptoBot/1.0; +https://example.com)"
    }

    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 403:
                print(f"⛔ Lỗi 403 - bị từ chối. Thử lại sau 10s (lần {attempt + 1}/{retries})")
                time.sleep(10)
            else:
                print(f"⚠️ Lỗi {r.status_code} không xác định ở batch: {ids[:3]}")
                break
        except Exception as e:
            print(f"🚨 Lỗi mạng ở batch {ids[:3]}: {e}")
            time.sleep(5)
    return {}

def main():
    # Hiển thị IP server
    try:
        ip = requests.get("https://api.ipify.org").text
        print(f"🌐 IP server hiện tại: {ip}")
    except:
        print("⚠️ Không lấy được IP.")

    if not os.path.exists(INPUT_FILE):
        print(f"❌ File {INPUT_FILE} không tồn tại.")
        return

    df_ids = pd.read_csv(INPUT_FILE)
    ids = df_ids['id'].tolist()

    all_data = []
    now = datetime.utcnow()

    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i:i + BATCH_SIZE]
        data = get_price_data(batch)

        print(f"📥 Batch {i // BATCH_SIZE + 1}: Nhận được {len(data)} coin.")

        for coin_id, info in data.items():
            try:
                entry = {
                    'id': coin_id,
                    'symbol': df_ids[df_ids['id'] == coin_id]['symbol'].values[0],
                    'current_price_usd': info.get('usd'),
                    'market_cap': info.get('usd_market_cap'),
                    'volume_24h': info.get('usd_24hr_vol'),
                    'price_change_24h': info.get('usd_24hr_change'),
                    'time_collected': now
                }
                all_data.append(entry)
            except Exception as e:
                print(f"❗ Lỗi xử lý coin {coin_id}: {e}")

        time.sleep(5)  # nghỉ 5s giữa các batch tránh bị rate-limit

    print(f"📊 Tổng số coin thu được: {len(all_data)}")

    if not all_data:
        print("❌ Không thu được dữ liệu nào. Dừng lưu và upload.")
        if os.path.exists(OUTPUT_FILE):
            os.remove(OUTPUT_FILE)  # xóa file rỗng nếu có
        return

    df = pd.DataFrame(all_data)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"✅ Đã lưu file: {OUTPUT_FILE}")

    upload_DR.upload_to_drive()

if __name__ == "__main__":
    main()
