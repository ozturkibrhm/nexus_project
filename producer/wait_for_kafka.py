import socket
import time
import sys
import subprocess

host = "kafka"
port = 29092
timeout = 120  # saniye

if len(sys.argv) < 2:
    print("⚠️  Kullanıcı config dosyası yolu belirtilmedi.")
    sys.exit(1)

config_path = sys.argv[1]

print(f"[WAIT-FOR-KAFKA] {host}:{port} adresine bağlanmaya çalışılıyor...")

start_time = time.time()

while True:
    try:
        with socket.create_connection((host, port), timeout=5):
            print(f"[WAIT-FOR-KAFKA] ✅ Bağlantı başarılı: {host}:{port}")
            break
    except OSError:
        time.sleep(1)
        elapsed = time.time() - start_time
        print(f"[WAIT-FOR-KAFKA] ⏳ {int(elapsed)}sn - bekleniyor...")
        if elapsed > timeout:
            print(f"[WAIT-FOR-KAFKA] ❌ Zaman aşımı ({timeout}s): {host}:{port}")
            sys.exit(1)

# Bağlantı kurulduktan sonra producer.py başlat
print(f"[WAIT-FOR-KAFKA] producer.py başlatılıyor... (config: {config_path})")
subprocess.run(["python", "producer.py", config_path])
