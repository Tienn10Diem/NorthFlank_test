from flask import Flask
import main
import traceback

app = Flask(__name__)

@app.route("/")
def home():
    return "🚀 Flask server is running."

@app.route("/run")
def run_script():
    try:
        main.main()
        return "✅ Script executed successfully.", 200
    except Exception as e:
        # Ghi lỗi ra file log (nếu cần), tránh in toàn bộ traceback ra HTTP response
        with open("error_log.txt", "w") as f:
            f.write(traceback.format_exc())
        return "❌ Script failed. Check error_log.txt for details.", 500
