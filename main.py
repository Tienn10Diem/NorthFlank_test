import pandas as pd
import requests
import time
from datetime import datetime
import os

OUTPUT_FILE = "crypto_full_data.csv"
df_ids = pd.read_csv("500_coins.csv")
coin_ids = df_ids["id"].tolist()

now = datetime.now()
now_str = now.strftime("%Y-%m-%d %H:%M:%S")
all_data = []

for i in range(0, len(coin_ids), 100):
    batch = coin_ids[i:i+100]
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ",".join(batch),
        "sparkline": False,
        "price_change_percentage": "24h"
    }
    r = requests.get(url, params=params)

    if r.status_code == 429:
        print(f"❗ Lỗi 429 ở batch {i//100+1} – đợi 10 giây rồi thử lại...")
        time.sleep(10)
        r = requests.get(url, params=params)

    if r.status_code != 200:
        print(f"Lỗi {r.status_code} ở batch {i//100+1}")
        continue

    data = r.json()
    for coin in data:
        all_data.append({
            "id": coin["id"],
            "name": coin["name"],
            "symbol": coin["symbol"],
            "current_price_usd": coin["current_price"],
            "market_cap": coin["market_cap"],
            "market_cap_rank": coin["market_cap_rank"],
            "price_change_24h": coin["price_change_percentage_24h"],
            "total_volume": coin["total_volume"],
            "circulating_supply": coin["circulating_supply"],
            "total_supply": coin["total_supply"],
            "image": coin["image"],
            "time_collected": now_str
        })

    time.sleep(2.1)  # tránh bị chặn API

df = pd.DataFrame(all_data)

if df.empty:
    print("⚠️ Không thu được dữ liệu từ CoinGecko. Không gọi upload.")
else:
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    os.system("python upload_DR.py")
    print(f"✅ Đã lưu file: {OUTPUT_FILE}")
