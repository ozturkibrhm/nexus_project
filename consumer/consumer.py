import os
import json
import psycopg2  # type: ignore
from kafka import KafkaConsumer  # type: ignore
from datetime import datetime

# ✅ Ortam değişkenlerini kontrol et
required_envs = ["POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "KAFKA_BROKER", "KAFKA_TOPIC"]
missing = [env for env in required_envs if not os.environ.get(env)]

if missing:
    print(f"❌ Eksik ortam değişkenleri: {', '.join(missing)}")
    exit(1)

# 🌐 PostgreSQL bağlantı bilgileri
POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

# 🔗 Kafka bağlantı bilgileri
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

# 🗄️ PostgreSQL bağlantısı kur
try:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    conn.autocommit = True
    print("✅ PostgreSQL bağlantısı kuruldu.")
except Exception as e:
    print(f"❌ PostgreSQL bağlantı hatası: {e}")
    exit(1)

# 📦 Gerekli tabloları oluştur
try:
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wcenter_data (
                id SERIAL PRIMARY KEY,
                hostname TEXT,
                cpu_usage_pct INTEGER,
                ram_usage_gb INTEGER,
                status TEXT,
                timestamp TIMESTAMP,
                ip TEXT,
                port INTEGER,
                username TEXT,
                password TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wcenter_vm_data (
                id SERIAL PRIMARY KEY,
                vm_name TEXT,
                vm_state TEXT,
                vm_cpu INTEGER,
                vm_cpu_usage_mhz INTEGER,
                vm_mem_mb INTEGER,
                vm_mem_usage_mb INTEGER,
                host_timestamp TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
    print("✅ Tablolar kontrol edildi/oluşturuldu.")
except Exception as e:
    print(f"❌ Tablo oluşturulurken hata: {e}")
    exit(1)

# 🎧 Kafka Consumer başlat
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id="wcenter-group",
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    print(f"🎧 Kafka consumer başlatıldı. Topic: {KAFKA_TOPIC}")
except Exception as e:
    print(f"❌ Kafka consumer başlatılamadı: {e}")
    exit(1)

# 📥 Mesajları dinleyip veritabanına yaz
def save_to_db(data):
    try:
        with conn.cursor() as cursor:
            # Yeni veri formatı: host + vms aynı dict içinde
            if isinstance(data, dict) and "hostname" in data and "vms" in data:
                vms = data["vms"]
                ts = data.get("host_timestamp")
                try:
                    ts = datetime.fromisoformat(ts) if ts else None
                except ValueError:
                    ts = None

                cursor.execute(
                    """
                    INSERT INTO wcenter_data (
                        hostname, cpu_usage_pct, ram_usage_gb, status, timestamp,
                        ip, port, username, password
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        data.get("hostname"),
                        data.get("cpu_usage_mhz"),
                        data.get("ram_usage_gb", 0),
                        "OK",
                        ts,
                        data.get("ip"),
                        int(data.get("port", 0)) if data.get("port") else None,
                        data.get("username"),
                        data.get("password")
                    )
                )

                for vm in vms:
                    cursor.execute(
                        """
                        INSERT INTO wcenter_vm_data (
                            vm_name, vm_state, vm_cpu, vm_cpu_usage_mhz,
                            vm_mem_mb, vm_mem_usage_mb, host_timestamp
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            vm.get("vm_name"),
                            vm.get("vm_state"),
                            vm.get("vm_cpu"),
                            vm.get("vm_cpu_usage_mhz"),
                            vm.get("vm_mem_mb"),
                            vm.get("vm_mem_usage_mb"),
                            ts
                        )
                    )
                print(f"✔ Host + {len(vms)} VM verisi kaydedildi.")

            else:
                # Eski format: tekil host verisi
                ts = data.get("timestamp")
                try:
                    ts = datetime.fromisoformat(ts) if ts else None
                except ValueError:
                    ts = None

                try:
                    port = int(data.get("port", 0)) if data.get("port") else None
                except ValueError:
                    port = None

                cursor.execute(
                    """
                    INSERT INTO wcenter_data (
                        hostname, cpu_usage_pct, ram_usage_gb, status, timestamp,
                        ip, port, username, password
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        data.get("hostname"),
                        data.get("cpu_usage_pct"),
                        data.get("ram_usage_gb"),
                        data.get("status"),
                        ts,
                        data.get("ip"),
                        port,
                        data.get("username"),
                        data.get("password")
                    )
                )
                print(f"✔ Tekil veri kaydedildi: {data}")

    except Exception as e:
        print(f"❌ Kaydetme hatası: {e} | Veri: {data}")

# 🔄 Kafka'dan veri dinle
for msg in consumer:
    print(f"[RECEIVED] {msg.value}")
    save_to_db(msg.value)
