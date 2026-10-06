import http.server
import json
import time
import subprocess
import sys
import threading

# =====================================================================
GRPC_HOST = "localhost:9094"
BEARER_TOKEN = input("Enter auth token: ")

LOCAL_PORT = 9090
# Use host.docker.internal so Docker container can send HTTP requests back to host.
# (If on Linux native Docker without host-gateway enabled, use http://172.17.0.1:9090/webhook)
CALLBACK_URL = f"http://host.docker.internal:{LOCAL_PORT}/webhook"
# =====================================================================

webhook_id = None


class WebhookReceiverHandler(http.server.BaseHTTPRequestHandler):

  def do_POST(self):
    content_length = int(self.headers.get("Content-Length", 0))
    body = self.rfile.read(content_length).decode("utf-8")

    print("\n" + "=" * 60)
    print("[GET MESSAGE] Received incoming webhook delivery from Docker!")
    print("=" * 60)
    print("Headers:")
    for key, val in self.headers.items():
      print(f"  {key}: {val}")

    print("\nPayload:")
    try:
      parsed_body = json.loads(body)
      print(json.dumps(parsed_body, indent=2))
    except json.JSONDecodeError:
      print(body)
    print("=" * 60 + "\n")

    self.send_response(200)
    self.send_header("Content-Type", "application/json")
    self.end_headers()
    self.wfile.write(json.dumps({"status": "received"}).encode("utf-8"))

  def log_message(self, format, *args):
    pass


def call_grpc(method: str, payload: dict):
  """Executes gRPC call via grpcurl."""
  cmd = ["grpcurl", "-plaintext"]
  if BEARER_TOKEN:
    cmd.extend(["-rpc-header", f"authorization: Bearer {BEARER_TOKEN}"])

  full_method = f"bnpb.webhook.v1.WebhookService/{method}"
  cmd.extend(["-d", json.dumps(payload), GRPC_HOST, full_method])

  res = subprocess.run(cmd, capture_output=True, text=True)
  if res.returncode != 0:
    raise RuntimeError(res.stderr.strip())

  return json.loads(res.stdout) if res.stdout.strip() else {}


def hook():
  """HOOK: Calls CreateWebhook RPC."""
  global webhook_id
  print(f"[HOOK] Registering webhook endpoint: {CALLBACK_URL}...")

  payload = {
      "url": CALLBACK_URL,
      "secret": "my-test-hmac-secret-123",
      "description": "gRPC Automated Test Runner",
      "view": "VIEW_SUMMARY",  # Enum VIEW_SUMMARY from bnpb.common.v1
      "filter": {},
  }

  try:
    data = call_grpc("CreateWebhook", payload)
    sub = data.get("subscription", {})
    webhook_id = sub.get("webhookId") or sub.get("webhook_id")
    print(f"[HOOK] Webhook hooked successfully! ID: {webhook_id}")
    return webhook_id
  except Exception as e:
    print(f"[HOOK] Hook failed: {e}")
    sys.exit(1)


def ping(target_id):
  """Calls PingWebhook RPC to deliver a test payload."""
  print(f"[PING] Triggering PingWebhook for {target_id}...")
  payload = {"webhook_id": target_id}

  try:
    res = call_grpc("PingWebhook", payload)
    print(f"[PING] Ping request response: {json.dumps(res, indent=2)}")
  except Exception as e:
    print(f"[PING] Ping request failed: {e}")


def unhook(target_id):
  """UNHOOK: Calls DeleteWebhook RPC."""
  if not target_id:
    return

  print(f"\n[UNHOOK] Deleting webhook subscription ({target_id})...")
  payload = {"webhook_id": target_id}

  try:
    res = call_grpc("DeleteWebhook", payload)
    print(f"[UNHOOK] Webhook unhooked successfully! Response: {res}")
  except Exception as e:
    print(f"[UNHOOK] Failed to unhook webhook: {e}")


def main():
  server = http.server.HTTPServer(
      ("0.0.0.0", LOCAL_PORT), WebhookReceiverHandler
  )
  server_thread = threading.Thread(target=server.serve_forever, daemon=True)
  server_thread.start()
  print(f"Local Listener listening on port {LOCAL_PORT}...")

  sub_id = None
  try:
    sub_id = hook()
    ping(sub_id)

    print("\nListener active. Press Ctrl+C to clean up (Unhook).\n")
    while True:
      time.sleep(0.5)

  except KeyboardInterrupt:
    print("\nInterrupt received (Ctrl+C). Cleaning up...")
  finally:
    if sub_id:
      unhook(sub_id)
    server.shutdown()
    print("Shut down successfully.")
    sys.exit(0)


if __name__ == "__main__":
  main()