"""
WIOM Recharge Dashboard - Web Server
Deployed on Render. Every page load re-fetches live data from Metabase.
"""
import subprocess, sys, os, time, threading
from flask import Flask, Response

app = Flask(__name__)

SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dashboard_builder.py')
HTML_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output_recharge')
HTML_PATH = os.path.join(HTML_DIR, 'recharge_dashboard.html')

# Lock to prevent multiple simultaneous builds
_build_lock = threading.Lock()

def build_dashboard():
    """Run the dashboard builder script and return the HTML."""
    env = os.environ.copy()
    result = subprocess.run(
        [sys.executable, SCRIPT],
        capture_output=True, text=True, timeout=600, env=env
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout or "Build failed")
    with open(HTML_PATH, 'r', encoding='utf-8') as f:
        return f.read()

@app.route('/')
def dashboard():
    start = time.time()
    print(f"\n[{time.strftime('%H:%M:%S')}] Building dashboard...")

    if not _build_lock.acquire(blocking=False):
        # Another request is already building
        if os.path.exists(HTML_PATH):
            with open(HTML_PATH, 'r', encoding='utf-8') as f:
                return Response(f.read(), mimetype='text/html')
        return Response("Dashboard is being built by another request. Refresh in 30s.", status=202)

    try:
        html = build_dashboard()
        elapsed = time.time() - start
        print(f"[{time.strftime('%H:%M:%S')}] Ready in {elapsed:.1f}s")
        return Response(html, mimetype='text/html')
    except Exception as e:
        print(f"[ERROR] {e}")
        return Response(
            f"<html><body style='background:#0a0a1a;color:#E74C3C;font-family:monospace;padding:40px'>"
            f"<h2>Dashboard Build Failed</h2><pre>{e}</pre>"
            f"<p style='color:#888'>Refresh to retry</p></body></html>",
            status=500, mimetype='text/html'
        )
    finally:
        _build_lock.release()

@app.route('/cached')
def cached():
    """Serve last generated dashboard instantly."""
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
    app.run(host='0.0.0.0', port=port, debug=False)
