#!/usr/bin/env python3
import ssl, atexit, json, os, time, sys
from datetime import datetime
from kafka import KafkaProducer  # type: ignore
from pyVim.connect import SmartConnect, Disconnect  # type: ignore
from pyVmomi import vim, vmodl  # type: ignore

# ESXi’ye bağlanma
def connect_esxi(host, port, user, pwd):
    ctx = ssl._create_unverified_context()
    si = SmartConnect(host=host, port=int(port),
                      user=user, pwd=pwd, sslContext=ctx)
    atexit.register(Disconnect, si)
    print("✔ ESXi’ye bağlanıldı.")
    return si

# Host metriklerini topla
def gather_host_info(si):
    content = si.RetrieveContent()
    h = content.rootFolder.childEntity[0].hostFolder.childEntity[0].host[0]
    s = h.summary
    return {
        "hostname": getattr(s.config, 'name', None),
        "host_timestamp": datetime.utcnow().isoformat(),
        "cpu_cores": s.hardware.numCpuCores or 0,
        "cpu_threads": s.hardware.numCpuThreads or 0,
        "cpu_mhz": s.hardware.cpuMhz or 0,
        "cpu_usage_mhz": getattr(s.quickStats, 'overallCpuUsage', 0) or 0,
        "ram_usage_gb": getattr(s.quickStats, 'guestMemoryUsage', 0) or 0,
        "ram_total_gb": (s.hardware.memorySize or 0) // (1024*1024)
    }

# VM metriklerini topla (liste halinde)
def gather_vm_info(si):
    content = si.RetrieveContent()
    vm_view = content.viewManager.CreateContainerView(
        content.rootFolder, [vim.VirtualMachine], True)
    vms = []
    for vm in vm_view.view:
        s = vm.summary
        vms.append({
            "vm_name": s.config.name,
            "vm_state": getattr(vm.runtime, 'powerState', 'N/A'),
            "vm_cpu": s.config.numCpu or 0,
            "vm_cpu_usage_mhz": getattr(s.quickStats, 'overallCpuUsage', 0) or 0,
            "vm_mem_mb": s.config.memorySizeMB or 0,
            "vm_mem_usage_mb": getattr(s.quickStats, 'guestMemoryUsage', 0) or 0,
        })
    vm_view.Destroy()
    return vms

# JSON içinden kullanıcı meta verisi bul
def find_user(ip, port, username, password, json_path):
    try:
        with open(json_path) as f:
            users = json.load(f)
    except Exception as e:
        print(f"❌ JSON okunamadı: {e}")
        sys.exit(1)
    for u in users.values():
        if (u.get("ip")==ip and str(u.get("port"))==str(port)
            and u.get("username")==username and u.get("password")==password):
            return u
    return None

# Kafka producer oluştur
def create_kafka_producer(broker):
    for i in range(5):
        try:
            p = KafkaProducer(
                bootstrap_servers=[broker],
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print(f"✔ Kafka'ya bağlanıldı: {broker}")
            return p
        except Exception as e:
            print(f"[Retry {i+1}/5] Kafka hatası: {e}")
            time.sleep(5)
    print("❌ Kafka’ya bağlanılamadı.")
    sys.exit(1)

def main():
    if len(sys.argv)!=6:
        print("❌ Kullanım: python producer.py <esxi_ip> <esxi_port> <esxi_user> <esxi_pass> <json_path>")
        sys.exit(1)

    esxi_ip, esxi_port, esxi_user, esxi_pass, json_path = sys.argv[1:]

    # Kullanıcı meta’sını bekle
    wait = 0
    while not os.path.exists(json_path):
        print(f"⏳ JSON yok: {json_path}")
        time.sleep(2); wait+=2
        if wait>60:
            print("❌ JSON bulunamadı.")
            sys.exit(1)

    user_meta = find_user(esxi_ip, esxi_port, esxi_user, esxi_pass, json_path)
    if not user_meta:
        print("❌ Kullanıcı bulunamadı.")
        sys.exit(1)
    print(f"✔ Kullanıcı bulundu: {user_meta}")

    # ESXi ve Kafka bağlantıları
    si = connect_esxi(esxi_ip, esxi_port, esxi_user, esxi_pass)
    broker = os.getenv("KAFKA_BROKER","kafka:29092")
    topic  = os.getenv("KAFKA_TOPIC","wcenter-data")
    producer = create_kafka_producer(broker)

    # Döngü: host+VM+user verilerini tek mesajda gönder
    while True:
        host_stats = gather_host_info(si)
        vm_list    = gather_vm_info(si)

        # Flatten edilmiş payload
        payload = {
            **host_stats,
            **user_meta,
            "vms": vm_list
        }

        try:
            producer.send(topic, payload)
            producer.flush()
            print(f"[DATA SENT] {payload}")
        except Exception as e:
            print(f"❌ Kafka'ya gönderilemedi: {e}")
        time.sleep(10)

if __name__=="__main__":
    main()
