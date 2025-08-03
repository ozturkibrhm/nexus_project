import socket
import time
import sys
import subprocess
import psycopg2 # type: ignore
import os

KAFKA_HOST = "kafka"
KAFKA_PORT = 29092
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = 5432
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

TIMEOUT_SECONDS = 120

def wait_for_socket(host: str, port: int, timeout: int, label: str):
    print(f"[WAIT] {label}: {host}:{port} adresine bağlanmaya çalışılıyor...")
    start_time = time.time()

    while True:
        try:
            with socket.create_connection((host, port), timeout=5):
                print(f"[WAIT] ✅ {label} bağlantısı başarılı: {host}:{port}")
                return
        except (OSError, socket.error):
            elapsed = time.time() - start_time
            print(f"[WAIT] ⏳ {label} - {int(elapsed)}sn geçti, bekleniyor...")
            if elapsed > timeout:
                print(f"[WAIT] ❌ {label} zaman aşımı ({timeout}s): {host}:{port}")
                sys.exit(1)
            time.sleep(1)

def wait_for_postgres(timeout: int):
    print(f"[WAIT] PostgreSQL bağlantısı deneniyor...")
    start_time = time.time()

    while True:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            conn.close()
            print(f"[WAIT] ✅ PostgreSQL bağlantısı başarılı.")
            return
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"[WAIT] ⏳ PostgreSQL bekleniyor ({int(elapsed)}sn): {e}")
            if elapsed > timeout:
                print(f"[WAIT] ❌ PostgreSQL zaman aşımı ({timeout}s)")
                sys.exit(1)
            time.sleep(2)

def start_consumer():
    print("[START] ▶ consumer.py başlatılıyor...")
    try:
        result = subprocess.run(["python", "consumer.py"], check=True)
        sys.exit(result.returncode)
    except subprocess.CalledProcessError as e:
        print(f"❌ consumer.py çalıştırılırken hata oluştu: {e}")
        sys.exit(1)

if __name__ == "__main__":
    wait_for_socket(KAFKA_HOST, KAFKA_PORT, TIMEOUT_SECONDS, "Kafka")
    wait_for_postgres(TIMEOUT_SECONDS)
    start_consumer()
