from kafka import KafkaProducer
import yfinance as yf
import json
import time
from datetime import datetime
import pytz

# ======================
# Cấu hình
# ======================
SYMBOLS = {
    "Vietcombank": "VCB.VN",   # Ngân hàng TMCP Ngoại thương Việt Nam
    "VietinBank": "CTG.VN",    # Ngân hàng TMCP Công Thương Việt Nam
    "BIDV": "BID.VN"           # Ngân hàng TMCP Đầu tư và Phát triển Việt Nam
}

TOPIC = "bank_prices" 

# ======================
# Khởi tạo Kafka Producer
# ======================
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ======================
# Lấy dữ liệu Yahoo Finance
# ======================
def fetch_yahoo_data(symbol, ticker):
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        data = {
            "symbol": symbol,
            "ticker": ticker,
            "current_price": info.get("currentPrice"),
            "previous_close": info.get("previousClose"),
            "open": info.get("open"),
            "day_high": info.get("dayHigh"),
            "day_low": info.get("dayLow"),
            "volume": info.get("volume"),
            "market_cap": info.get("marketCap"),
            "pe_ratio": info.get("trailingPE"),
            "time": datetime.now(pytz.utc).isoformat()
        }

        producer.send(TOPIC, value=data)
        print(f"Sent {symbol}: {data}")

    except Exception as e:
        print(f"Error fetching {symbol}: {e}")

# ======================
# Main loop
# ======================
if __name__ == "__main__":
    print("Yahoo Finance Big 3 Bank Data Crawler Started...")
    while True:
        for symbol, ticker in SYMBOLS.items():
            fetch_yahoo_data(symbol, ticker)
        print("Chờ 60 giây để cập nhật lại...\n")
        time.sleep(60)
