import pandas as pd
import requests
import time
from datetime import datetime

# === CONFIG ===
INPUT_FILE = '500_coins.csv'
OUTPUT_FILE = 'crypto_full_data.csv'
BATCH_SIZE = 100

def get_price_data(ids):
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
    r = requests.get(url, params=params, headers=headers)
    if r.status_code == 200:
        return r.json()
    else:
        print(f"Lỗi {r.status_code} ở batch: {ids[:3]}")
        return {}

def main():
    df_ids = pd.read_csv(INPUT_FILE)
    ids = df_ids['id'].tolist()

    all_data = []
    now = datetime.utcnow()

    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i:i+BATCH_SIZE]
        data = get_price_data(batch)
        for coin_id, info in data.items():
            entry = {
                'id': coin_id,
                'symbol': df_ids[df_ids['id'] == coin_id]['symbol'].values[0],
                'current_price_usd': info.get('usd'),
                'market_cap': info.get('usd_market_cap'),
                'volume_24h': info.get('usd_24h_vol'),
                'price_change_24h': info.get('usd_24h_change'),
                'time_collected': now
            }
            all_data.append(entry)
        time.sleep(1.2)  # tránh bị chặn

    if not all_data:
        print("❌ Không thu được dữ liệu nào. Dừng lưu và upload.")
        return

    df = pd.DataFrame(all_data)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Đã lưu file: {OUTPUT_FILE}")

    # Gọi upload
    import upload_DR
    upload_DR.upload_to_drive()

if __name__ == "__main__":
    main()
