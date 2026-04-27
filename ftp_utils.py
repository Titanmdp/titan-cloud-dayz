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

def converter_pedidos_para_dayz_json(pedidos):
    """
    Converte lista de pedidos da loja para formato DayZ (Objects).
    Usa a coluna 'quantidade' do item para gerar múltiplos objetos.
    """
    objetos = []
    for pedido in pedidos:
        try:
            coords = pedido.get("coordenadas", "0 / 0").split("/")
            x = float(coords[0].strip())
            z = float(coords[1].strip())
            y = 0.0  # altura padrão

            qtd = int(pedido.get("quantidade", 1))

            for i in range(qtd):
                objetos.append({
                    "name": pedido.get("item_classe", "Unknown"),
                    "pos": [x, y, z],
                    "ypr": [0.0, 0.0, 0.0],
                    "scale": 1.0,
                    "enableCEPersistency": 0,
                    "customString": f"Pedido {pedido.get('id')} #{i+1}"
                })
        except Exception as e:
            print(f"Erro ao converter pedido {pedido.get('id')}: {e}")

    return {"Objects": objetos}

def enviar_pedidos_via_ftp(client_id: str, pedidos: list, mapa: str = "Chernarus") -> bool:
    """
    Envia os pedidos convertidos para formato DayZ como arquivo JSON via FTP.
    Apenas o arquivo loja_spawn.json é enviado para o servidor.
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
            remote_base = "dayzxb_missions/dayzOffline.enoch"
        else:
            remote_base = "dayzxb_missions/dayzOffline.chernarusplus"
        
        remote_dir = f"{remote_base}/custom"
        
        # Converte pedidos para formato DayZ
        pedidos_dayz = converter_pedidos_para_dayz_json(pedidos)
        
        # Salva em arquivo temporário
        temp_file = f"/tmp/loja_spawn_{client_id}.json"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(pedidos_dayz, f, indent=4, ensure_ascii=False)
        
        # Envia via FTP
        ftp = ftplib.FTP()
        ftp.connect(conf["host"], int(conf.get("port", 21)), timeout=15)
        ftp.login(conf["user"], conf["pass"])
        ftp.cwd(remote_dir)
        
        with open(temp_file, "rb") as f:
            ftp.storbinary(f"STOR loja_spawn.json", f)
        
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
