from flask import Flask
import main

app = Flask(__name__)

@app.route("/")
def home():
    return "🚀 Flask server is running."

@app.route("/run")
def run_script():
    try:
        main.main()
        return "✅ Script đã chạy thành công!"
    except Exception as e:
        return f"❌ Lỗi: {str(e)}", 500
