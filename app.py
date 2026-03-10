"""
WIOM Recharge Dashboard - Web Server
Deployed on Render. Every page load re-fetches live data from Metabase.
"""
import subprocess, sys, os, time, threading
from flask import Flask, Response, request

app = Flask(__name__)

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dashboard_builder.py')
HTML_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output_recharge')
HTML_PATH = os.path.join(HTML_DIR, 'recharge_dashboard.html')

_build_lock = threading.Lock()
_last_build_time = 0
_CACHE_SECONDS = 300  # serve cached version if built within last 5 minutes

LOADING_PAGE = """<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>WIOM Dashboard - Loading</title>
<style>
body{background:#0a0a1a;color:#e0e0e0;font-family:'Segoe UI',sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0}
.loader{text-align:center}
.spinner{width:50px;height:50px;border:4px solid #1a1a3e;border-top:4px solid #4ECDC4;border-radius:50%;animation:spin 1s linear infinite;margin:0 auto 20px}
@keyframes spin{0%{transform:rotate(0deg)}100%{transform:rotate(360deg)}}
h2{background:linear-gradient(90deg,#4ECDC4,#FF6B6B);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-size:22px}
p{color:#888;font-size:13px}
</style>
<meta http-equiv="refresh" content="3">
</head><body>
<div class="loader">
<div class="spinner"></div>
<h2>Connecting to Metabase...</h2>
<p>Fetching live data from Snowflake. This page will auto-reload in a few seconds.</p>
<p style="color:#4ECDC4;margin-top:20px">Please wait ~30-45 seconds</p>
</div></body></html>"""

def build_dashboard():
    env = os.environ.copy()
    result = subprocess.run(
        [sys.executable, SCRIPT],
        capture_output=True, text=True, timeout=600, env=env
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout or "Build failed")
    with open(HTML_PATH, 'r', encoding='utf-8') as f:
        return f.read()

def _background_build():
    global _last_build_time
    try:
        build_dashboard()
        _last_build_time = time.time()
        print(f"[{time.strftime('%H:%M:%S')}] Background build complete.")
    except Exception as e:
        print(f"[ERROR] Background build failed: {e}")
    finally:
        _build_lock.release()

@app.route('/')
def dashboard():
    global _last_build_time
    now = time.time()

    # If fresh cache exists (< 5 min old), serve it instantly
    if os.path.exists(HTML_PATH) and (now - _last_build_time) < _CACHE_SECONDS:
        print(f"[{time.strftime('%H:%M:%S')}] Serving cached ({int(now - _last_build_time)}s old)")
        with open(HTML_PATH, 'r', encoding='utf-8') as f:
            return Response(f.read(), mimetype='text/html')

    # Force refresh requested or cache is stale
    if _build_lock.acquire(blocking=False):
        print(f"[{time.strftime('%H:%M:%S')}] Starting build...")
        # If we have an old cached version, serve it and build in background
        if os.path.exists(HTML_PATH):
            t = threading.Thread(target=_background_build, daemon=True)
            t.start()
            with open(HTML_PATH, 'r', encoding='utf-8') as f:
                return Response(f.read(), mimetype='text/html')
        else:
            # No cache at all - show loading page, build in background
            t = threading.Thread(target=_background_build, daemon=True)
            t.start()
            return Response(LOADING_PAGE, mimetype='text/html')
    else:
        # Build already in progress
        if os.path.exists(HTML_PATH):
            with open(HTML_PATH, 'r', encoding='utf-8') as f:
                return Response(f.read(), mimetype='text/html')
        return Response(LOADING_PAGE, mimetype='text/html')

@app.route('/refresh')
def refresh():
    """Force a fresh rebuild, show loading page."""
    global _last_build_time
    _last_build_time = 0  # invalidate cache
    if _build_lock.acquire(blocking=False):
        print(f"[{time.strftime('%H:%M:%S')}] Force refresh triggered...")
        t = threading.Thread(target=_background_build, daemon=True)
        t.start()
    return Response(LOADING_PAGE, mimetype='text/html')

@app.route('/cached')
def cached():
    if os.path.exists(HTML_PATH):
        with open(HTML_PATH, 'r', encoding='utf-8') as f:
            return Response(f.read(), mimetype='text/html')
    return Response("No cached dashboard yet. Visit / first.", status=404)

@app.route('/health')
def health():
    return 'ok'

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5050))
    print(f"Starting on port {port}...")
    # Pre-build on startup
    print("Building initial dashboard...")
    try:
        build_dashboard()
        _last_build_time = time.time()
        print("Initial build complete.")
    except Exception as e:
        print(f"Initial build failed: {e}")
    app.run(host='0.0.0.0', port=port, debug=False)
