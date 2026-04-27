import ftplib
import os
import json
from datetime import datetime, timezone, timedelta

# Ajuste de fuso horário
FUSO_BR = timezone(timedelta(hours=-3))

def get_hora_brasilia():
    return datetime.now(FUSO_BR)

# Caminho para o banco de clientes
DB_CLIENTS = "/var/data/clients_data.json" if os.path.exists("/var/data") else "clients_data.json"

def load_db(file, default_data):
    if os.path.exists(file):
        try:
            with open(file, "r", encoding="utf-8") as f:
                conteudo = f.read()
                if not conteudo.strip():
                    return default_data
                return json.loads(conteudo)
        except Exception:
            return default_data
    return default_data

def save_db(file, data):
    if data is None:
        return
    try:
        with open(file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"Erro ao salvar banco de dados: {e}")

def enviar_pedidos_via_ftp(client_id: str, pedidos: list, mapa: str = "Chernarus") -> bool:
    """
    Envia um arquivo JSON com os pedidos para o servidor via FTP.
    Arquivo é enviado para: mpmissions/dayzOffline.{mapa}/custom/loja_pedidos.json
    """
    try:
        db_atual = load_db(DB_CLIENTS, {})
        if client_id not in db_atual:
            return False
        
        conf = db_atual[client_id].get("ftp", {})
        if not conf or not conf.get("host"):
            return False
        
        # Define caminho remoto de acordo com o mapa
        mapa_lower = mapa.lower()
        if "enoch" in mapa_lower or "livonia" in mapa_lower:
            remote_base = "mpmissions/dayzOffline.enoch"
        else:
            remote_base = "mpmissions/dayzOffline.chernarusplus"
        
        remote_dir = f"{remote_base}/custom"
        
        # Cria JSON com pedidos
        pedidos_data = {
            "timestamp": get_hora_brasilia().isoformat(),
            "pedidos": pedidos
        }
        
        # Salva em arquivo temporário
        temp_file = f"/tmp/loja_pedidos_{client_id}.json"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(pedidos_data, f, indent=4, ensure_ascii=False)
        
        # Envia via FTP
        ftp = ftplib.FTP()
        ftp.connect(conf["host"], int(conf.get("port", 21)), timeout=15)
        ftp.login(conf["user"], conf["pass"])
        ftp.cwd(remote_dir)
        
        with open(temp_file, "rb") as f:
            ftp.storbinary(f"STOR loja_pedidos.json", f)
        
        ftp.quit()
        
        # Remove arquivo temporário
        try:
            os.remove(temp_file)
        except:
            pass
        
        return True
    except Exception as e:
        print(f"Erro ao enviar pedidos via FTP: {e}")
        return False

