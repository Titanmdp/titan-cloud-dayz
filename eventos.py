import streamlit as st
import ftplib
import os
import json
import time
import threading
import secrets
import string
import requests
import shutil
import smtplib
import xml.etree.ElementTree as ET
import pandas as pd
import base64
import plotly.express as px
import threading
from email.message import EmailMessage
from datetime import datetime, timedelta, timezone
from streamlit_javascript import st_javascript

# Lock global para evitar escrita/leitura simultânea no JSON
db_lock = threading.Lock()

# =========================================================
# 1. CONFIG / AMBIENTE / CONSTANTES
# =========================================================

# --- DETECÇÃO DE AMBIENTE E PERSISTÊNCIA DE DADOS ---
IS_DEV = os.environ.get("IS_DEV", "False") == "True"

# Se existir um disk montado em /var/data (Render), usamos sempre ele
if os.path.exists("/var/data"):
    DB_USERS = "/var/data/users_db.json"
    DB_CLIENTS = "/var/data/clients_data.json"
# Senão, usamos arquivos locais na pasta do projeto (para rodar no seu PC)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    if IS_DEV:
        DB_USERS = os.path.join(BASE_DIR, "users_db_dev.json")
        DB_CLIENTS = os.path.join(BASE_DIR, "clients_data_dev.json")
    else:
        DB_USERS = os.path.join(BASE_DIR, "users_db.json")
        DB_CLIENTS = os.path.join(BASE_DIR, "clients_data.json")

# --- CONFIGURAÇÃO DA PÁGINA (antes de qualquer sidebar) ---
st.set_page_config(page_title="Titan Cloud PRO", layout="wide", page_icon="🚀")

if IS_DEV:
    st.sidebar.warning("🚧 AMBIENTE DE TESTES (DEV)")
    st.sidebar.warning("⚠️ AMBIENTE DE DESENVOLVIMENTO (TESTES)")

# --- CONFIGURAÇÃO DE FUSO HORÁRIO (BRASÍLIA) ---
FUSO_BR = timezone(timedelta(hours=-3))


def get_hora_brasilia():
    return datetime.now(FUSO_BR)


# --- DEFINIÇÃO DE LIMITES POR PLANO ---
PLANOS = {
    "Starter": 2,
    "Pro": 8,
    "Enterprise": 16,
}

def plano_permite(plano_atual: str, funcionalidade: str) -> bool:
    """
    Retorna True se o plano do cliente permite acessar a funcionalidade.
    """
    PERMISSOES = {
        # Livre para todos
        "banco_carteira":        ["Starter", "Pro", "Enterprise"],
        "loja":                  ["Starter", "Pro", "Enterprise"],
        "worker_dzcoins_auto":   ["Starter", "Pro", "Enterprise"],
        "agendamento":           ["Starter", "Pro", "Enterprise"],
        # Apenas Pro e Enterprise
        "editor_types":          ["Pro", "Enterprise"],
        "editor_globals":        ["Pro", "Enterprise"],
        "editor_cfggameplay":    ["Pro", "Enterprise"],
        "editor_events":         ["Pro", "Enterprise"],
        "editor_messages":       ["Pro", "Enterprise"],
        "editor_cfgeventspawns": ["Pro", "Enterprise"],
        "ranking_semanal":       ["Starter", "Pro", "Enterprise"],
        "transferencia_jogador": ["Starter", "Pro", "Enterprise"],
        "multimapa":             ["Pro", "Enterprise"],
        "jogadores_completo":    ["Starter", "Pro", "Enterprise"],
    }
    return plano_atual in PERMISSOES.get(funcionalidade, [])


def bloquear_funcionalidade(plano_atual: str, funcionalidade_nome: str, plano_minimo: str = "Pro"):
    """
    Exibe aviso de bloqueio amigável quando o plano não permite acesso.
    """
    st.markdown(
        f"""
        <div style="
            background:#1a1a2e;
            border:1px solid #7a4b1f;
            border-radius:10px;
            padding:28px 20px;
            text-align:center;
            margin-top:20px;
        ">
            <div style="font-size:36px; margin-bottom:10px;">🔒</div>
            <div style="font-size:16px; font-weight:bold;
                        color:#ffcc66; margin-bottom:8px;">
                {funcionalidade_nome}
            </div>
            <div style="font-size:13px; color:#aaa; margin-bottom:12px;">
                Esta funcionalidade está disponível a partir do plano
                <b style="color:#00d4ff;">{plano_minimo}</b>.<br>
                Seu plano atual é
                <b style="color:#ff6b6b;">{plano_atual}</b>.
            </div>
            <div style="font-size:12px; color:#666;">
                Entre em contato com o suporte para fazer upgrade do seu plano.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# Caminhos padrão do types.xml por mapa no servidor DayZ
TYPES_REMOTE_PATHS = {
    "Chernarus": "dayzxb_missions/dayzOffline.chernarusplus/db",
    "Livonia": "dayzxb_missions/dayzOffline.enoch/db",
}

GLOBALS_REMOTE_PATHS = {
    "Chernarus": "dayzxb_missions/dayzOffline.chernarusplus/db",
    "Livonia": "dayzxb_missions/dayzOffline.enoch/db",
}

EVENTS_REMOTE_PATHS = {
    "Chernarus": "dayzxb_missions/dayzOffline.chernarusplus/db",
    "Livonia": "dayzxb_missions/dayzOffline.enoch/db",
}

MESSAGES_REMOTE_PATHS = {
    "Chernarus": "dayzxb_missions/dayzOffline.chernarusplus/db",
    "Livonia": "dayzxb_missions/dayzOffline.enoch/db",
}

CFGEVENTSPAWNS_REMOTE_PATHS = {
    "Chernarus": "dayzxb_missions/dayzOffline.chernarusplus",
    "Livonia": "dayzxb_missions/dayzOffline.enoch",
}

CFGGAMEPLAY_REMOTE_PATHS = {
    "Chernarus": "dayzxb_missions/dayzOffline.chernarusplus",
    "Livonia": "dayzxb_missions/dayzOffline.enoch",
}

# --- BANCO DE DADOS (JSON) / UPLOADS ---
if os.path.exists("/var/data"):
    UPLOAD_DIR = "/var/data/uploads"
else:
    UPLOAD_DIR = "uploads"

os.makedirs(UPLOAD_DIR, exist_ok=True)


# =========================================================
# 2. FUNÇÕES UTILITÁRIAS / INFRA
# =========================================================

def str_to_time(data_str, hora_str):
    """
    Converte data (dd/mm/yyyy) + hora (HH:MM) para datetime aware (FUSO_BR).
    Usa replace() apenas para definir o fuso — compatível com pytz-free.
    """
    try:
        return datetime.strptime(
            f"{data_str} {hora_str}",
            "%d/%m/%Y %H:%M"
        ).replace(tzinfo=FUSO_BR)
    except Exception:
        return None

def buscar_localizacao_cliente():
    url_api = "https://ipapi.co/json/"
    js_code = f"await fetch('{url_api}').then(res => res.json())"
    result = st_javascript(js_code)

    if result:
        return {
            "cidade": result.get("city", "Desconhecido"),
            "estado": result.get("region", "---"),
        }
    return None


def manter_vivo():
    while True:
        try:
            url = "https://titan-cloud-dayz-dev.onrender.com"
            requests.get(url, timeout=10)
        except Exception:
            pass
        time.sleep(600)

threading.Thread(target=manter_vivo, daemon=True).start()


def load_db(file, default_data):
    with db_lock:
        if os.path.exists(file):
            try:
                with open(file, "r", encoding="utf-8") as f:
                    conteudo = f.read()
                    if not conteudo.strip():
                        return default_data
                    return json.loads(conteudo)
            except Exception as e:
                backup = file + ".bak"
                if os.path.exists(backup):
                    try:
                        with open(backup, "r", encoding="utf-8") as f:
                            return json.loads(f.read())
                    except Exception:
                        pass
                print(f"Erro ao carregar {file}: {e}")
        return default_data


def save_db(file, data):
    with db_lock:  # ADICIONAR ESTA LINHA
        if data is None:
            return

    try:
        # Cria backup antes de sobrescrever, se existir
        if os.path.exists(file):
            shutil.copy(file, file + ".bak")

        with open(file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        st.error(f"Erro ao salvar banco de dados: {e}")


def enviar_email(destino, assunto, mensagem):
    email_user = os.environ.get("EMAIL_USER")
    email_pass = os.environ.get("EMAIL_PASS")

    if not email_user or not email_pass:
        print("Erro: Credenciais de e-mail não configuradas no ambiente.")
        return False

    try:
        msg = EmailMessage()
        msg.set_content(mensagem)
        msg["Subject"] = assunto
        msg["To"] = destino
        msg["From"] = email_user

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(email_user, email_pass)
            smtp.send_message(msg)
        return True
    except Exception as e:
        print(f"Erro detalhado ao enviar e-mail: {e}")
        return False


def enviar_whatsapp(numero, mensagem):
    """
    Envia uma mensagem via WhatsApp (integração com serviço externo).
    Este é um stub que retorna False, pois requer integração com API.
    Para implementar: usar twilio, baileys ou similar.
    """
    try:
        print(f"Tentativa de enviar WhatsApp para {numero}: {mensagem[:50]}...")
        return False
    except Exception as e:
        print(f"Erro ao enviar WhatsApp: {e}")
        return False


def registrar_log(client_id, mensagem, tipo="info"):
    db_disco = load_db(DB_CLIENTS, {})

    if client_id not in db_disco:
        db_disco[client_id] = {
            "ftp": {"host": "", "user": "", "pass": "", "port": "21"},
            "agendas": [],
            "logs": [],
        }

    if "logs" not in db_disco[client_id]:
        db_disco[client_id]["logs"] = []

    timestamp = get_hora_brasilia().strftime("%H:%M:%S")
    icone = "🟢" if tipo == "sucesso" else "🔴" if tipo == "erro" else "📡"
    db_disco[client_id]["logs"].insert(0, f"[{timestamp}] {icone} {mensagem}")

    db_disco[client_id]["logs"] = db_disco[client_id]["logs"][:50]
    save_db(DB_CLIENTS, db_disco)


def validar_acesso(key):
    if key == st.session_state.db_users["admin_key"]:
        return True, "admin"
    keys = st.session_state.db_users["keys"]
    if key in keys:
        validade = datetime.strptime(keys[key]["expires"], "%d/%m/%Y").date()
        if validade >= get_hora_brasilia().date():
            return True, "client"
        return False, "Sua KeyUser expirou!"
    return False, "KeyUser inválida!"


def get_user_location():
    try:
        response = requests.get("http://ip-api.com/json/", timeout=5).json()
        if response["status"] == "success":
            return {
                "ip": response["query"],
                "cidade": response["city"],
                "estado": response["regionName"],
                "pais": response["country"],
            }
    except Exception:
        pass

    return {
        "ip": "0.0.0.0",
        "cidade": "Desconhecido",
        "estado": "---",
        "pais": "---",
    }

def enviar_ao_discord(webhook_url: str, titulo: str, mensagem: str, cor: int = 65280):
    """
    Envia um Embed formatado para o Discord via Webhook.
    Cores comuns (Decimal): Verde (65280), Vermelho (16711680), Azul (255).
    """
    if not webhook_url:
        return

    payload = {
        "embeds": [{
            "title": titulo,
            "description": mensagem,
            "color": cor,
            "footer": {"text": "Titan Cloud PRO • Auditoria Automatizada"},
            "timestamp": datetime.now(FUSO_BR).isoformat()
        }]
    }

    try:
        requests.post(webhook_url, json=payload, timeout=5)
    except Exception as e:
        print(f"Erro ao enviar Webhook Discord: {e}")


# --- Eventos disponíveis para webhooks ---
WEBHOOK_EVENTOS_DISPONIVEIS = {
    "players_online":       {"label": "👥 Players Online",              "categoria": "jogadores", "cor": 0x00FF88},
    "player_conectou":      {"label": "🟢 Player Entrou no Servidor",   "categoria": "jogadores", "cor": 0x00FF88},
    "player_desconectou":   {"label": "🔴 Player Saiu do Servidor",     "categoria": "jogadores", "cor": 0xFF4444},
    "pvp_kill":             {"label": "⚔️ Kill PvP",                    "categoria": "pvp",       "cor": 0xFF6600},
    "pve_hit":              {"label": "🧟 Hit por Infected",            "categoria": "pve",       "cor": 0xAA44FF},
    "morte_ambiente":       {"label": "💀 Morte por Ambiente/Suicídio", "categoria": "mortes",    "cor": 0x888888},
    "compra_loja":          {"label": "🛒 Compra na Loja",              "categoria": "loja",      "cor": 0xFFD700},
    "dzcoins_distribuicao": {"label": "💰 Distribuição de DzCoins",     "categoria": "dzcoins",   "cor": 0x00D4FF},
    "ranking_atualizacao":  {"label": "🏆 Atualização do Ranking",      "categoria": "ranking",   "cor": 0xFFAA00},
    "reset_servidor":       {"label": "🔄 Reset do Servidor",           "categoria": "servidor",  "cor": 0x4488FF},
    "anti_glitch":          {"label": "🚨 Anti-Glitch Detectado",       "categoria": "auditoria", "cor": 0xFF0000},
}


def enviar_webhook_evento(
    client_data: dict,
    evento: str,
    titulo: str,
    descricao: str,
    campos: list = None,
):
    """
    Envia um evento para todos os webhooks configurados que
    têm esse evento habilitado.

    client_data : dict do servidor
    evento      : chave do evento ex. 'pvp_kill'
    titulo      : título do embed
    descricao   : descrição do embed
    campos      : lista de dicts com 'name' e 'value' para fields do embed
    """
    webhooks_cfg = client_data.get("webhooks_config", [])
    if not webhooks_cfg:
        return

    info_evento = WEBHOOK_EVENTOS_DISPONIVEIS.get(evento, {})
    cor = info_evento.get("cor", 0x00D4FF)

    embed = {
        "title": titulo,
        "description": descricao,
        "color": cor,
        "footer": {"text": "Titan Cloud PRO • Auditoria Automatizada"},
        "timestamp": datetime.now(FUSO_BR).isoformat(),
    }

    if campos:
        embed["fields"] = [
            {
                "name": c["name"],
                "value": c["value"],
                "inline": c.get("inline", True),
            }
            for c in campos
        ]

    payload = {"embeds": [embed]}

    for wh in webhooks_cfg:
        if not wh.get("ativo", True):
            continue
        if evento not in wh.get("eventos", []):
            continue

        url = wh.get("url", "").strip()
        if not url:
            continue

        try:
            requests.post(url, json=payload, timeout=5)
        except Exception as e:
            print(f"[Webhook] Erro ao enviar para '{wh.get('nome', '?')}': {e}")

def render_heatmap(client_data):
    st.subheader("🔥 Mapa de Calor de Atividade")

    data = client_data.get("heatmap_data", [])
    if not data:
        st.info("Ainda não há dados suficientes para gerar o mapa. Aguarde o processamento dos logs.")
        return

    try:
        df = pd.DataFrame(data, columns=["Z", "X"])
    except Exception as e:
        st.error(f"Erro ao processar dados do mapa de calor: {e}")
        return

    fig = px.density_heatmap(
        df,
        x="X",
        y="Z",
        nbinsx=100,
        nbinsy=100,
        color_continuous_scale="Viridis",
        title="Zonas de Maior Atividade (Últimos Logs)"
    )

    fig.update_layout(
        plot_bgcolor="black",
        paper_bgcolor="black",
        font_color="white"
    )

    st.plotly_chart(fig, use_container_width=True)

# ---------- HELPERS TYPES.XML (ECONOMIA) ----------

def parse_types_xml(xmlbytes):
    """
    Recebe bytes de um types.xml e devolve:
    - tree: objeto ET.ElementTree
    - root: elemento raiz
    - df: DataFrame com colunas principais para edição
    """
    tree = ET.ElementTree(ET.fromstring(xmlbytes))
    root = tree.getroot()
    rows = []

    for t in root.findall("type"):
        name = t.get("name", "")
        cat = None
        catelem = t.find("category")
        if catelem is not None:
            cat = catelem.get("name")

        def getint(tag, default=None):
            elem = t.find(tag)
            if elem is not None and elem.text is not None and elem.text.strip() != "":
                try:
                    return int(elem.text.strip())
                except Exception:
                    return default
            return default

        nominal = getint("nominal", 0)
        minv = getint("min", 0)
        lifetime = getint("lifetime", 0)

        rows.append(
            {
                "name": name,
                "category": cat,
                "nominal": nominal,
                "min": minv,
                "lifetime": lifetime,
            }
        )

    df = pd.DataFrame(rows)
    return tree, root, df


def apply_df_to_types_xml(tree, root, df):
    """
    Aplica as alterações do DataFrame de volta no XML
    e devolve bytes do novo types.xml.
    """
    dfindexed = df.set_index("name")

    for t in root.findall("type"):
        name = t.get("name", "")
        if name not in dfindexed.index:
            continue
        row = dfindexed.loc[name]

        def setint(tag, value):
            if pd.isna(value):
                return
            elem = t.find(tag)
            if elem is None:
                elem = ET.SubElement(t, tag)
            elem.text = str(int(value))

        setint("nominal", row.get("nominal"))
        setint("min", row.get("min"))
        setint("lifetime", row.get("lifetime"))

    xmlbytes = ET.tostring(root, encoding="utf-8", method="xml")
    header = b'<?xml version="1.0" encoding="utf-8"?>\n'
    return header + xmlbytes


def dispararftppro(clientid, acao, filename, localpath, mapapath):
    dbatual = load_db(DB_CLIENTS, {})
    if clientid not in dbatual:
        return False, "Erro"

    conf = dbatual[clientid]["ftp"]
    try:
        ftp = ftplib.FTP()
        ftp.connect(conf["host"], int(conf["port"]), timeout=15)
        ftp.login(conf["user"], conf["pass"])
        ftp.cwd(mapapath)
        if acao == "UPLOAD":
            with open(localpath, "rb") as f:
                ftp.storbinary(f"STOR {filename}", f)
        elif acao == "DELETE":
            try:
                ftp.delete(filename)
            except Exception:
                pass
        ftp.quit()
        return True, "Sucesso"
    except Exception:
        return False, "Erro"

def worker_dzcoins_automatico():
    """
    Worker que distribui DzCoins automaticamente APENAS para jogadores
    que estão online no servidor no momento da distribuição.
    Roda em loop contínuo em thread separada.
    """
    import ftplib
    import io
    import re

    def get_online_players_ftp(ftp_cfg: dict) -> list:
        """
        Lê o último arquivo .ADM via FTP e retorna lista
        de jogadores atualmente online.
        """
        if not ftp_cfg:
            return []

        try:
            with ftplib.FTP() as ftp:
                ftp.connect(ftp_cfg["host"], int(ftp_cfg["port"]), timeout=15)
                ftp.login(ftp_cfg["user"], ftp_cfg["pass"])
                ftp.cwd("dayzxb/config")

                arquivos = [
                    a for a in ftp.nlst()
                    if a.upper().endswith(".ADM")
                ]
                if not arquivos:
                    return []

                arquivos.sort(reverse=True)
                ultimo = arquivos[0]

                buffer = io.BytesIO()
                ftp.retrbinary(f"RETR {ultimo}", buffer.write)
                texto = buffer.getvalue().decode("utf-8", errors="ignore")

        except Exception as e:
            print(f"[DzCoins Worker] Erro ao ler ADM via FTP: {e}")
            return []

        # Parser de quem está online
        conectados = {}
        re_player = re.compile(r'\d{2}:\d{2}:\d{2} \| Player "(.+?)"')

        for line in texto.splitlines():
            line = line.strip()
            if not line:
                continue

            m = re_player.match(line)
            if not m:
                continue

            nome = m.group(1)

            if "DEAD" in line:
                continue

            if "is connected" in line and "is connecting" not in line:
                conectados[nome] = True
            elif "has been disconnected" in line:
                conectados.pop(nome, None)

        return [n for n, v in conectados.items() if v]

    while True:
        try:
            db = load_db(DB_CLIENTS, {})

            for server_id, client_data in db.items():
                config = client_data.get("dzcoins_config", {})

                # Verifica se o worker está ativo
                if not config.get("ativo", False):
                    continue

                quantidade = int(config.get("quantidade_dzcoins", 0))
                intervalo = int(config.get("intervalo_minutos", 60))

                if quantidade <= 0 or intervalo <= 0:
                    continue

                # Pega configuração FTP do servidor
                ftp_cfg = client_data.get("ftp", {})
                host = ftp_cfg.get("host", "")
                user = ftp_cfg.get("user", "")
                pwd  = ftp_cfg.get("pass", "")
                port = int(ftp_cfg.get("port", 21) or 21)

                if not host or not user or not pwd:
                    print(f"[DzCoins Worker] FTP não configurado para {server_id}, pulando.")
                    continue

                ftp_config = {
                    "host": host,
                    "user": user,
                    "pass": pwd,
                    "port": port,
                }

                # Busca jogadores online via ADM
                jogadores_online = get_online_players_ftp(ftp_config)

                if not jogadores_online:
                    print(f"[DzCoins Worker] Nenhum jogador online em {server_id}.")
                    continue

                # Distribui DzCoins apenas para quem está online e vinculado
                players_vinculados = client_data.get("players", {})
                wallets = client_data.setdefault("wallets", {})
                hora_atual = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")

                alterou = False
                contemplados = []

                for gamertag in jogadores_online:
                    if gamertag not in players_vinculados:
                        continue

                    wallet = wallets.setdefault(
                        gamertag, {"balance": 0, "historico": []}
                    )
                    wallet["balance"] = wallet.get("balance", 0) + quantidade
                    wallet.setdefault("historico", []).append(
                        f"[{hora_atual}] +{quantidade} DzCoins (tempo de jogo online)"
                    )
                    contemplados.append(gamertag)
                    alterou = True

                if alterou:
                    db[server_id] = client_data
                    save_db(DB_CLIENTS, db)
                    print(
                        f"[DzCoins Worker] {server_id} — "
                        f"{len(contemplados)} jogador(es) contemplado(s): "
                        f"{', '.join(contemplados)}"
                    )

                    # --- Dispara webhook de distribuição de DzCoins ---
                    try:
                        _webhooks_cfg = client_data.get("webhooks_config", [])

                        for _wh in _webhooks_cfg:
                            if not _wh.get("ativo", True):
                                continue
                            if "dzcoins_distribuicao" not in _wh.get("eventos", []):
                                continue

                            _url = _wh.get("url", "").strip()
                            if not _url:
                                continue

                            _payload = {
                                "embeds": [{
                                    "title": "💰 DzCoins Distribuídos",
                                    "description": (
                                        f"**{len(contemplados)}** jogador(es) receberam "
                                        f"DzCoins por tempo de jogo online."
                                    ),
                                    "color": 0x00D4FF,
                                    "fields": [
                                        {
                                            "name": "💎 DzCoins por jogador",
                                            "value": str(quantidade),
                                            "inline": True,
                                        },
                                        {
                                            "name": "👥 Total contemplados",
                                            "value": str(len(contemplados)),
                                            "inline": True,
                                        },
                                        {
                                            "name": "🕒 Horário",
                                            "value": hora_atual,
                                            "inline": True,
                                        },
                                        {
                                            "name": "👤 Jogadores",
                                            "value": "\n".join(
                                                f"• {g}" for g in contemplados
                                            ) or "Nenhum",
                                            "inline": False,
                                        },
                                    ],
                                    "footer": {
                                        "text": "Titan Cloud PRO • Worker DzCoins"
                                    },
                                    "timestamp": datetime.now(FUSO_BR).isoformat(),
                                }]
                            }

                            try:
                                requests.post(_url, json=_payload, timeout=5)
                            except Exception as _e:
                                print(
                                    f"[Webhook DzCoins] Erro ao enviar para "
                                    f"'{_wh.get('nome', '?')}': {_e}"
                                )

                    except Exception as _e:
                        print(f"[Webhook DzCoins] Erro geral: {_e}")

        except Exception as e:
            print(f"[DzCoins Worker] Erro geral: {e}")

        # Aguarda o menor intervalo configurado entre todos os servidores
        try:
            db2 = load_db(DB_CLIENTS, {})
            intervalos = [
                int(c.get("dzcoins_config", {}).get("intervalo_minutos", 60))
                for c in db2.values()
                if c.get("dzcoins_config", {}).get("ativo", False)
            ]
            sleep_min = min(intervalos) if intervalos else 60
        except Exception:
            sleep_min = 60

        time.sleep(sleep_min * 60)


def proworker():
    while True:
        try:
            now = get_hora_brasilia()
            db_all = load_db(DB_CLIENTS, {})
            mudou = False

            for client_id, client_info in db_all.items():
                log_txt = ""
                eventos_pvp = []

                # --- TRAVA DE SEGURANÇA (GRC/GOVERNANÇA) ---
                # Se o administrador desativar "Baixar Logs Servidor", o worker ignora este cliente
                feeds_config = client_info.get("feeds_config", {})
                if not feeds_config.get("baixar_logs", True):
                    continue

                # --- 1. LÓGICA DE AGENDAS DE ARQUIVOS (EXISTENTE) ---
                for agenda in client_info.get("agendas", []):
                    hora_entrada = str_to_time(agenda.get("data"), agenda.get("in"))
                    hora_saida = str_to_time(agenda.get("data"), agenda.get("out"))

                    # Tolerância de 30s para eventos agendados muito próximos do horário atual
                    if (
                        hora_entrada
                        and now >= (hora_entrada - timedelta(seconds=30))
                        and agenda.get("status") == "Aguardando"
                    ):
                        if not os.path.exists(agenda["localpath"]) and agenda.get("filecontent"):
                            try:
                                os.makedirs(os.path.dirname(agenda["localpath"]), exist_ok=True)
                                with open(agenda["localpath"], "wb") as file_obj:
                                    file_obj.write(base64.b64decode(agenda["filecontent"]))
                            except Exception as exc:
                                print("Erro ao recriar arquivo:", exc)

                        if not os.path.exists(agenda["localpath"]):
                            agenda["status"] = "Erro"
                            registrar_log(client_id, f"Arquivo perdido: {agenda['file']}", "erro")
                            mudou = True
                        else:
                            ok, msg = dispararftppro(
                                client_id,
                                "UPLOAD",
                                agenda["file"],
                                agenda["localpath"],
                                agenda["path"],
                            )
                            agenda["status"] = "Ativo" if ok else "Erro"
                            registrar_log(
                                client_id,
                                f"UPLOAD {agenda['file']} {'OK' if ok else msg}",
                                "sucesso" if ok else "erro",
                            )
                            mudou = True

                    if hora_saida and now >= hora_saida and agenda.get("status") == "Ativo":
                        ok, msg = dispararftppro(
                            client_id,
                            "DELETE",
                            agenda["file"],
                            agenda["localpath"],
                            agenda["path"],
                        )
                        registrar_log(
                            client_id,
                            f"DELETE {agenda['file']} {'OK' if ok else msg}",
                            "sucesso" if ok else "erro",
                        )

                        if agenda.get("rec") == "Diário":
                            agenda["data"] = (now + timedelta(days=1)).strftime("%d/%m/%Y")
                            agenda["status"] = "Aguardando"
                        elif agenda.get("rec") == "Semanal":
                            agenda["data"] = (now + timedelta(days=7)).strftime("%d/%m/%Y")
                            agenda["status"] = "Aguardando"
                        else:
                            agenda["status"] = "Finalizado"

                        mudou = True

                # --- 2. LÓGICA DE AGENDAS DE RAID AUTOMÁTICO ---
                for raid_agenda in client_info.get("agendas_raid", []):
                    inicio_raid = str_to_time(raid_agenda["data"], raid_agenda["in"])
                    fim_raid = str_to_time(raid_agenda["data"], raid_agenda["out"])

                    # Ação: INICIAR RAID
                    if raid_agenda["status"] == "Aguardando" and now >= inicio_raid:
                        ok, content, msg = baixarcfggameplayviaftp(client_id, raid_agenda["mapa"])
                        if ok:
                            try:
                                cfg_json = json.loads(content.decode("utf-8"))
                                cfg_json["GeneralData"]["disableBaseDamage"] = False

                                temp_file = f"raid_on_{client_id}.json"
                                with open(temp_file, "w", encoding="utf-8") as file_obj:
                                    json.dump(cfg_json, file_obj, indent=4, ensure_ascii=False)

                                env_ok, env_msg = enviar_cfggameplay_via_ftp(
                                    client_id,
                                    temp_file,
                                    raid_agenda["mapa"],
                                )
                                if env_ok:
                                    raid_agenda["status"] = "Ativo"
                                    registrar_log(
                                        client_id,
                                        f"🔥 RAID INICIADO em {raid_agenda['mapa']}! Dano em bases ATIVADO.",
                                        "sucesso",
                                    )
                                    mudou = True
                            except Exception as exc:
                                print(f"Erro ao processar JSON de RAID ON: {exc}")

                    # Ação: ENCERRAR RAID
                    elif raid_agenda["status"] == "Ativo" and now >= fim_raid:
                        ok, content, msg = baixarcfggameplayviaftp(client_id, raid_agenda["mapa"])
                        if ok:
                            try:
                                cfg_json = json.loads(content.decode("utf-8"))
                                cfg_json["GeneralData"]["disableBaseDamage"] = True

                                temp_file = f"raid_off_{client_id}.json"
                                with open(temp_file, "w", encoding="utf-8") as file_obj:
                                    json.dump(cfg_json, file_obj, indent=4, ensure_ascii=False)

                                env_ok, env_msg = enviar_cfggameplay_via_ftp(
                                    client_id,
                                    temp_file,
                                    raid_agenda["mapa"],
                                )
                                if env_ok:
                                    if raid_agenda.get("rec") == "Diário":
                                        raid_agenda["data"] = (now + timedelta(days=1)).strftime("%d/%m/%Y")
                                        raid_agenda["status"] = "Aguardando"
                                    elif raid_agenda.get("rec") == "Semanal":
                                        raid_agenda["data"] = (now + timedelta(days=7)).strftime("%d/%m/%Y")
                                        raid_agenda["status"] = "Aguardando"
                                    else:
                                        raid_agenda["status"] = "Finalizado"

                                    proxima_execucao = raid_agenda.get("data", "finalizado")
                                    registrar_log(
                                        client_id,
                                        f"🛡️ RAID ENCERRADO em {raid_agenda['mapa']}! Próxima execução: {proxima_execucao}",
                                        "info",
                                    )
                                    mudou = True
                            except Exception as exc:
                                print(f"Erro ao processar JSON de RAID OFF: {exc}")

                # --- BAIXA O LOG UMA ÚNICA VEZ PARA OS PASSOS DE AUDITORIA/INTELIGÊNCIA ---
                precisa_log = (
                    feeds_config.get("glitch_subsolo")
                    or feeds_config.get("glitch_hortas")
                    or feeds_config.get("glitch_fogueiras")
                    or feeds_config.get("mapa_calor", True)
                    or feeds_config.get("ranking_auto", True)
                )

                if precisa_log:
                    try:
                        log_txt, _ = ftp_download_latest_adm(client_info["ftp"])
                    except Exception as exc:
                        print(f"Erro ao baixar ADM log de {client_id}: {exc}")
                        log_txt = ""

                if log_txt:
                    try:
                        eventos_pvp = extrair_eventos_pvp(log_txt)
                    except Exception as exc:
                        print(f"Erro ao extrair eventos PvP de {client_id}: {exc}")
                        eventos_pvp = []

                # --- PASSO 6: LÓGICA ANTI-GLITCH (GRC/GOVERNANÇA) ---
                webhook_admin_logs = feeds_config.get("webhook_admin_logs")

                if (
                    feeds_config.get("glitch_subsolo")
                    or feeds_config.get("glitch_hortas")
                    or feeds_config.get("glitch_fogueiras")
                ):
                    if log_txt:
                        mapa_atual = client_info.get("loja", {}).get("mapa_padrao", "Chernarus")
                        alertas = analisar_glitches(log_txt, feeds_config, client_info, mapa_atual)

                        for alerta in alertas:
                            if alerta.get("banir"):
                                sucesso = aplicar_banimento_ftp(client_info["ftp"], alerta["jogador"])
                                if sucesso:
                                    msg = f"🔨 BANIMENTO AUTOMÁTICO: {alerta['jogador']} por {alerta['tipo']}!"
                                    registrar_log(client_id, msg, "erro")

                                    if webhook_admin_logs:
                                        enviar_ao_discord(
                                            webhook_admin_logs,
                                            "🔨 PUNIÇÃO EXECUTADA",
                                            f"**Jogador:** {alerta['jogador']}\n**Motivo:** {alerta['tipo']}\n**Detalhe:** {alerta['detalhe']}",
                                            cor=16711680,
                                        )

                                    client_info.get("tracking_acoes", {}).pop(alerta["jogador"], None)
                                    mudou = True
                                else:
                                    registrar_log(
                                        client_id,
                                        f"❌ FALHA AO BANIR: {alerta['jogador']} (Erro FTP)",
                                        "erro",
                                    )
                            else:
                                msg_alerta = (
                                    f"🚨 ALERTA GLITCH: {alerta['jogador']} detectado em "
                                    f"{alerta['tipo']}! Pos: {alerta['pos']}"
                                )
                                registrar_log(client_id, msg_alerta, "erro")

                                if webhook_admin_logs:
                                    enviar_ao_discord(
                                        webhook_admin_logs,
                                        "🚨 SUSPEITA DE GLITCH",
                                        f"**Jogador:** {alerta['jogador']}\n**Tipo:** {alerta['tipo']}\n**Posição:** {alerta['pos']}",
                                        cor=16776960,
                                    )

                # --- PASSO 7: LÓGICA MAPA DE CALOR (INTELIGÊNCIA) ---
                if feeds_config.get("mapa_calor", True) and log_txt:
                    novas_coords = extrair_coordenadas_mapa(log_txt)
                    historico_coords = client_info.get("heatmap_data", [])
                    historico_coords.extend(novas_coords)
                    client_info["heatmap_data"] = historico_coords[-5000:]
                    mudou = True

                # --- PASSO 8: RANKING AUTOMATIZADO (GRC) ---
                if feeds_config.get("ranking_auto", True) and eventos_pvp:
                    ranking_atualizado = processar_ranking_global(eventos_pvp, [])
                    client_info["ranking_global"] = ranking_atualizado
                    mudou = True

            if mudou:
                save_db(DB_CLIENTS, db_all)

        except Exception as exc:
            print("Erro no proworker:", exc)

        time.sleep(15)


WORKER_STARTED = False
def start_worker_once():
    global WORKER_STARTED
    if not WORKER_STARTED:
        WORKER_STARTED = True
        threading.Thread(target=proworker, daemon=True).start()
        threading.Thread(target=worker_dzcoins_automatico, daemon=True).start()
        print("[Worker] proworker e worker_dzcoins iniciados.")

# Inicia o worker imediatamente no boot do app (independente de login)
start_worker_once()

# ---------- HELPER GESTÃO DE PEDIDOS (ADMIN SERVIDOR) ----------
def render_gestao_pedidos(client_data, server_id):
    st.subheader("📦 Auditoria e Gestão de Pedidos")
    st.info("Monitore as vendas e realize estornos de DzCoins se necessário.")

    pedidos = client_data.get("pedidos", [])
    if not pedidos:
        st.info("Nenhum pedido realizado na loja até o momento.")
        return

    # 1. Resumo Econômico
    col_r1, col_r2, col_r3 = st.columns(3)
    faturamento = sum(int(p.get("preco", 0)) for p in pedidos)
    col_r1.metric("Total de Pedidos", len(pedidos))
    col_r2.metric("Faturamento", f"{faturamento} DzCoins")
    col_r3.metric("Pendentes", len([p for p in pedidos if p.get("status") == "Aguardando Reset"]))

    # 2. Tabela de Pedidos
    df_pedidos = pd.DataFrame(pedidos)
    colunas = ['data_compra', 'gamertag', 'item_nome', 'preco', 'status', 'id']
    st.dataframe(df_pedidos[colunas], use_container_width=True, hide_index=True)

    # 3. Área de Estorno
    with st.expander("🔴 Realizar Estorno (Devolução)"):
        id_estorno = st.selectbox("Selecione o ID do Pedido:", df_pedidos['id'].tolist())
        motivo = st.text_input("Motivo do Estorno:")
        if st.button("Confirmar Estorno e Devolver Saldo", type="primary"):
            idx = next((i for i, p in enumerate(pedidos) if p['id'] == id_estorno), None)
            if idx is not None:
                p = pedidos[idx]
                secao = "wallets" if "Carteira" in p['origem_pagamento'] else "bank"
                gt = p['gamertag']
                valor = int(p['preco'])
                
                # Devolve o valor ao jogador
                client_data.setdefault(secao, {}).setdefault(gt, {"balance": 0, "historico": []})
                client_data[secao][gt]["balance"] += valor
                client_data[secao][gt]["historico"].append(f"[{get_hora_brasilia().strftime('%H:%M')}] ESTORNO: +{valor} (Motivo: {motivo})")
                
                pedidos.pop(idx) # Remove o pedido
                save_db(DB_CLIENTS, st.session_state.db_clients)
                st.success("✅ Estorno concluído!"); time.sleep(1); st.rerun()

# ---------- HELPER GENÉRICO DE DOWNLOAD VIA FTP ----------

def baixar_arquivo_via_ftp(clientid, remotedir, remotefilename):
    """
    Baixa um arquivo remoto via FTP e devolve:
    (ok: bool, file_bytes: bytes|None, msg: str)
    """
    dbatual = load_db(DB_CLIENTS, {})

    if clientid not in dbatual:
        return False, None, "Cliente não encontrado"

    conf = dbatual[clientid].get("ftp", {})
    if not conf or not conf.get("host"):
        return False, None, "Configuração FTP não encontrada"

    try:
        ftp = ftplib.FTP()
        ftp.connect(conf["host"], int(conf.get("port", 21)), timeout=20)
        ftp.login(conf["user"], conf["pass"])
        ftp.cwd(remotedir)

        chunks = []
        ftp.retrbinary(f"RETR {remotefilename}", chunks.append)
        ftp.quit()

        file_bytes = b"".join(chunks)
        return True, file_bytes, "Sucesso"

    except Exception as e:
        return False, None, str(e)

# ---------- WRAPPERS ESPECÍFICOS DE DOWNLOAD ----------

def baixartypesviaftp(clientid, mapa):
    remotedir = TYPES_REMOTE_PATHS.get(mapa)
    if not remotedir:
        return False, None, f"Caminho remoto não configurado para o mapa {mapa}"
    return baixar_arquivo_via_ftp(clientid, remotedir, "types.xml")

def baixarglobalsviaftp(clientid, mapa):
    remotedir = GLOBALS_REMOTE_PATHS.get(mapa)
    if not remotedir:
        return False, None, f"Caminho remoto não configurado para o mapa {mapa}"
    return baixar_arquivo_via_ftp(clientid, remotedir, "globals.xml")

def baixarcfggameplayviaftp(clientid, mapa):
    remotedir = CFGGAMEPLAY_REMOTE_PATHS.get(mapa)
    if not remotedir:
        return False, None, f"Caminho remoto não configurado para o mapa {mapa}"
    return baixar_arquivo_via_ftp(clientid, remotedir, "cfggameplay.json")

def baixareventsviaftp(clientid, mapa):
    remotedir = EVENTS_REMOTE_PATHS.get(mapa)
    if not remotedir:
        return False, None, f"Caminho remoto não configurado para o mapa {mapa}"
    return baixar_arquivo_via_ftp(clientid, remotedir, "events.xml")

def baixarmessagesviaftp(clientid, mapa):
    remotedir = MESSAGES_REMOTE_PATHS.get(mapa)
    if not remotedir:
        return False, None, f"Caminho remoto não configurado para o mapa {mapa}"
    return baixar_arquivo_via_ftp(clientid, remotedir, "messages.xml")

def baixarcfgeventspawnsviaftp(clientid, mapa):
    remotedir = CFGEVENTSPAWNS_REMOTE_PATHS.get(mapa)
    if not remotedir:
        return False, None, f"Caminho remoto não configurado para o mapa {mapa}"
    return baixar_arquivo_via_ftp(clientid, remotedir, "cfgeventspawns.xml")

# ---------- HELPERS CFGEVENTSPAWNS.XML ----------

def parse_cfgeventspawns_xml(xml_bytes):
    """
    Recebe bytes de um cfgeventspawns.xml e devolve:
    - tree: objeto ET.ElementTree
    - root: elemento raiz
    - eventos_map: dict {nome_evento: DataFrame com colunas x, z, a, y}
    """
    tree = ET.ElementTree(ET.fromstring(xml_bytes))
    root = tree.getroot()

    eventos_map = {}

    for event_elem in root.findall("event"):
        event_name = event_elem.get("name", "SemNome")
        rows = []

        for pos in event_elem.findall("pos"):
            def to_float(v, default=None):
                if v is None or str(v).strip() == "":
                    return default
                try:
                    return float(str(v).strip())
                except Exception:
                    return default

            rows.append({
                "x": to_float(pos.get("x"), 0.0),
                "z": to_float(pos.get("z"), 0.0),
                "a": to_float(pos.get("a"), None),
                "y": to_float(pos.get("y"), None),
            })

        eventos_map[event_name] = pd.DataFrame(rows, columns=["x", "z", "a", "y"])

    return tree, root, eventos_map


def apply_df_to_cfgeventspawns_xml(tree, root, event_name, df_evento):
    """
    Atualiza apenas um bloco <event name='...'> no XML original,
    preservando os demais eventos e atributos existentes.
    Retorna bytes do novo cfgeventspawns.xml.
    """
    target_event = None
    for event_elem in root.findall("event"):
        if event_elem.get("name") == event_name:
            target_event = event_elem
            break

    if target_event is None:
        raise ValueError(f"Evento '{event_name}' não encontrado no cfgeventspawns.xml.")

    for pos_elem in list(target_event.findall("pos")):
        target_event.remove(pos_elem)

    df_clean = df_evento.copy()

    for _, row in df_clean.iterrows():
        pos_attrs = {
            "x": str(float(row["x"])) if pd.notna(row["x"]) else "0.0",
            "z": str(float(row["z"])) if pd.notna(row["z"]) else "0.0",
        }

        if "a" in row and pd.notna(row["a"]):
            pos_attrs["a"] = str(float(row["a"]))

        if "y" in row and pd.notna(row["y"]):
            pos_attrs["y"] = str(float(row["y"]))

        ET.SubElement(target_event, "pos", pos_attrs)

    xml_bytes = ET.tostring(root, encoding="utf-8", method="xml")
    header = b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    return header + xml_bytes


def aplicar_eventos_map_no_cfgeventspawns(tree, root, eventos_map):
    """
    Aplica todos os DataFrames do dict eventos_map ao XML inteiro
    e devolve os bytes finais.
    """
    for event_name, df_evento in eventos_map.items():
        apply_df_to_cfgeventspawns_xml(tree, root, event_name, df_evento)

    xml_bytes = ET.tostring(root, encoding="utf-8", method="xml")
    header = b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    return header + xml_bytes



# ---------- HELPERS EVENTS.XML ----------

def parse_events_xml(xml_bytes):
    """
    Recebe bytes de um events.xml e devolve:
    - tree: objeto ET.ElementTree
    - root: elemento raiz
    - df: DataFrame com os principais campos editáveis de cada <event>
    """
    tree = ET.ElementTree(ET.fromstring(xml_bytes))
    root = tree.getroot()
    rows = []

    def to_int(value, default=0):
        try:
            if value is None or str(value).strip() == "":
                return default
            return int(float(str(value).strip()))
        except Exception:
            return default

    def to_bool(value, default=False):
        try:
            if value is None or str(value).strip() == "":
                return default
            value_str = str(value).strip().lower()
            return value_str in ["1", "true", "yes"]
        except Exception:
            return default

    for event_elem in root.findall("event"):
        name = event_elem.get("name", "")

        nominal = to_int(event_elem.findtext("nominal"), 0)
        minimum = to_int(event_elem.findtext("min"), 0)
        maximum = to_int(event_elem.findtext("max"), 0)
        lifetime = to_int(event_elem.findtext("lifetime"), 0)
        restock = to_int(event_elem.findtext("restock"), 0)
        saferadius = to_int(event_elem.findtext("saferadius"), 0)
        distanceradius = to_int(event_elem.findtext("distanceradius"), 0)
        cleanupradius = to_int(event_elem.findtext("cleanupradius"), 0)

        flags_elem = event_elem.find("flags")
        active = False
        if flags_elem is not None:
            active = to_bool(flags_elem.get("active", "0"), False)

        rows.append(
            {
                "name": name,
                "nominal": nominal,
                "min": minimum,
                "max": maximum,
                "lifetime": lifetime,
                "restock": restock,
                "saferadius": saferadius,
                "distanceradius": distanceradius,
                "cleanupradius": cleanupradius,
                "active": active,
            }
        )

    df = pd.DataFrame(
        rows,
        columns=[
            "name",
            "nominal",
            "min",
            "max",
            "lifetime",
            "restock",
            "saferadius",
            "distanceradius",
            "cleanupradius",
            "active",
        ],
    )

    return tree, root, df

def apply_df_to_events_xml(tree, root, df_events):
    """
    Aplica as alterações do DataFrame de volta no events.xml
    e devolve bytes do novo XML.
    """
    df_indexed = df_events.set_index("name")

    def set_text(parent, tag, value):
        elem = parent.find(tag)
        if elem is None:
            elem = ET.SubElement(parent, tag)
        elem.text = str(int(value)) if pd.notna(value) else "0"

    for event_elem in root.findall("event"):
        name = event_elem.get("name", "")
        if name not in df_indexed.index:
            continue

        row = df_indexed.loc[name]

        set_text(event_elem, "nominal", row.get("nominal", 0))
        set_text(event_elem, "min", row.get("min", 0))
        set_text(event_elem, "max", row.get("max", 0))
        set_text(event_elem, "lifetime", row.get("lifetime", 0))
        set_text(event_elem, "restock", row.get("restock", 0))
        set_text(event_elem, "saferadius", row.get("saferadius", 0))
        set_text(event_elem, "distanceradius", row.get("distanceradius", 0))
        set_text(event_elem, "cleanupradius", row.get("cleanupradius", 0))

        flags_elem = event_elem.find("flags")
        if flags_elem is None:
            flags_elem = ET.SubElement(event_elem, "flags")

        active_value = bool(row.get("active", False))
        flags_elem.set("active", "1" if active_value else "0")

    xml_bytes = ET.tostring(root, encoding="utf-8", method="xml")
    header = b'<?xml version="1.0" encoding="utf-8"?>\n'
    return header + xml_bytes

# ---------- HELPERS MESSAGES.XML ----------

def parse_messages_xml(xml_bytes):
    """
    Lê o messages.xml e expõe cada <message> em DataFrame editável.
    Tenta capturar tanto atributos quanto sub-tags comuns.
    """
    tree = ET.ElementTree(ET.fromstring(xml_bytes))
    root = tree.getroot()
    rows = []

    def to_int(value, default=0):
        try:
            if value is None or str(value).strip() == "":
                return default
            return int(float(str(value).strip()))
        except Exception:
            return default

    def first_text(elem, tag_names):
        for tag in tag_names:
            child = elem.find(tag)
            if child is not None and child.text is not None and str(child.text).strip() != "":
                return str(child.text).strip()
        return ""

    def first_int(elem, tag_names, default=0):
        for tag in tag_names:
            child = elem.find(tag)
            if child is not None:
                return to_int(child.text, default)
        return default

    message_nodes = root.findall(".//message")

    for idx, msg_elem in enumerate(message_nodes, start=1):
        text_value = (
            first_text(msg_elem, ["text", "message", "content", "body"])
            or (msg_elem.text or "").strip()
        )

        time_value = msg_elem.get("time", None)
        if time_value in [None, ""]:
            time_value = msg_elem.get("delay", None)
        if time_value in [None, ""]:
            time_value = first_int(msg_elem, ["time", "delay"], 0)
        else:
            time_value = to_int(time_value, 0)

        priority_value = msg_elem.get("priority", None)
        if priority_value in [None, ""]:
            priority_value = first_int(msg_elem, ["priority", "order"], 0)
        else:
            priority_value = to_int(priority_value, 0)

        row = {
            "ordem": idx,
            "id": str(msg_elem.get("id", "")).strip(),
            "name": str(msg_elem.get("name", "")).strip(),
            "time": time_value,
            "priority": priority_value,
            "color": str(msg_elem.get("color", "")).strip(),
            "icon": str(msg_elem.get("icon", "")).strip(),
            "text": text_value,
            "_elem": msg_elem,
        }
        rows.append(row)

    df = pd.DataFrame(
        rows,
        columns=["ordem", "id", "name", "time", "priority", "color", "icon", "text", "_elem"]
    )

    return tree, root, df


def apply_df_to_messages_xml(tree, root, df_messages):
    """
    Aplica as alterações do DataFrame ao XML original,
    preservando ao máximo a estrutura já existente.
    Também permite adicionar novas mensagens.
    """
    if df_messages.empty:
        xml_bytes = ET.tostring(root, encoding="utf-8", method="xml")
        return b"\n" + xml_bytes

    def ensure_child(parent, tag_name):
        child = parent.find(tag_name)
        if child is None:
            child = ET.SubElement(parent, tag_name)
        return child

    df_work = df_messages.copy()

    for _, row in df_work.iterrows():
        msg_elem = row.get("_elem")

        id_val = str(row.get("id", "")).strip()
        name_val = str(row.get("name", "")).strip()
        color_val = str(row.get("color", "")).strip()
        icon_val = str(row.get("icon", "")).strip()
        text_val = str(row.get("text", "") or "").strip()
        time_val = row.get("time", 0)
        priority_val = row.get("priority", 0)

        if msg_elem is None:
            msg_elem = ET.SubElement(root, "message")

        if id_val:
            msg_elem.set("id", id_val)
        elif "id" in msg_elem.attrib:
            del msg_elem.attrib["id"]

        if name_val:
            msg_elem.set("name", name_val)
        elif "name" in msg_elem.attrib:
            del msg_elem.attrib["name"]

        if color_val:
            msg_elem.set("color", color_val)
        elif "color" in msg_elem.attrib:
            del msg_elem.attrib["color"]

        if icon_val:
            msg_elem.set("icon", icon_val)
        elif "icon" in msg_elem.attrib:
            del msg_elem.attrib["icon"]

        if pd.notna(time_val):
            try:
                time_int = int(float(time_val))
            except Exception:
                time_int = 0

            if "time" in msg_elem.attrib:
                msg_elem.set("time", str(time_int))
            elif "delay" in msg_elem.attrib:
                msg_elem.set("delay", str(time_int))
            elif msg_elem.find("time") is not None:
                ensure_child(msg_elem, "time").text = str(time_int)
            elif msg_elem.find("delay") is not None:
                ensure_child(msg_elem, "delay").text = str(time_int)
            else:
                msg_elem.set("time", str(time_int))

        if pd.notna(priority_val):
            try:
                priority_int = int(float(priority_val))
            except Exception:
                priority_int = 0

            if "priority" in msg_elem.attrib:
                msg_elem.set("priority", str(priority_int))
            elif msg_elem.find("priority") is not None:
                ensure_child(msg_elem, "priority").text = str(priority_int)
            elif msg_elem.find("order") is not None:
                ensure_child(msg_elem, "order").text = str(priority_int)

        if msg_elem.find("text") is not None:
            ensure_child(msg_elem, "text").text = text_val
        elif msg_elem.find("message") is not None:
            ensure_child(msg_elem, "message").text = text_val
        elif msg_elem.find("content") is not None:
            ensure_child(msg_elem, "content").text = text_val
        elif msg_elem.find("body") is not None:
            ensure_child(msg_elem, "body").text = text_val
        else:
            msg_elem.text = text_val

    xml_bytes = ET.tostring(root, encoding="utf-8", method="xml")
    return b"\n" + xml_bytes

# ---------- HELPER GENÉRICO DE FTP ----------

def enviar_arquivo_via_ftp(clientid, localpath, remotedir, remotefilename):
    """
    Envia um arquivo local para um diretório remoto específico via FTP.
    """
    dbatual = load_db(DB_CLIENTS, {})
    if clientid not in dbatual:
        return False, "Cliente não encontrado"

    conf = dbatual[clientid].get("ftp", {})
    if not conf or not conf.get("host"):
        return False, "Configuração FTP não encontrada"

    try:
        ftp = ftplib.FTP()
        ftp.connect(conf["host"], int(conf.get("port", 21)), timeout=15)
        ftp.login(conf["user"], conf["pass"])
        ftp.cwd(remotedir)

        with open(localpath, "rb") as f:
            ftp.storbinary(f"STOR {remotefilename}", f)

        ftp.quit()
        return True, "Sucesso"

    except Exception as e:
        return False, str(e)


# ---------- WRAPPERS ESPECÍFICOS ----------

def enviar_types_via_ftp(clientid, localpath, mapa):
    """
    Envia o arquivo types.xml já salvo em localpath
    para o caminho correto no servidor, de acordo com o mapa.
    """
    remotedir = TYPES_REMOTE_PATHS.get(mapa)
    if not remotedir:
        return False, f"Caminho remoto não configurado para o mapa {mapa}"

    return enviar_arquivo_via_ftp(
        clientid=clientid,
        localpath=localpath,
        remotedir=remotedir,
        remotefilename="types.xml",
    )

def enviar_globals_via_ftp(clientid, localpath, mapa):
    """
    Envia o arquivo globals.xml já salvo em localpath
    para o caminho correto no servidor, de acordo com o mapa.
    """
    remotedir = TYPES_REMOTE_PATHS.get(mapa)
    if not remotedir:
        return False, f"Caminho remoto não configurado para o mapa {mapa}"

    return enviar_arquivo_via_ftp(
        clientid=clientid,
        localpath=localpath,
        remotedir=remotedir,
        remotefilename="globals.xml",
    )

def enviar_cfggameplay_via_ftp(clientid, localpath, mapa):
    """
    Envia o arquivo cfggameplay.json já salvo em localpath
    para o caminho correto no servidor, de acordo com o mapa.
    """
    remotedir = CFGGAMEPLAY_REMOTE_PATHS.get(mapa)
    if not remotedir:
        return False, f"Caminho remoto não configurado para o mapa {mapa}"

    return enviar_arquivo_via_ftp(
        clientid=clientid,
        localpath=localpath,
        remotedir=remotedir,
        remotefilename="cfggameplay.json",
    )

def enviareventsviaftp(clientid, localpath, mapa):
    """
    Envia o arquivo events.xml para o diretório correto do mapa.
    """
    remotedir = EVENTS_REMOTE_PATHS.get(mapa)
    if not remotedir:
        return False, f"Caminho remoto não configurado para o mapa {mapa}"

    return enviar_arquivo_via_ftp(
        clientid=clientid,
        localpath=localpath,
        remotedir=remotedir,
        remotefilename="events.xml",
    )

def enviarmessagesviaftp(clientid, localpath, mapa):
    MESSAGESREMOTEPATHS = {
        "Chernarus": "dayzxb_missions/dayzOffline.chernarusplus/db",
        "Livonia": "dayzxb_missions/dayzOffline.enoch/db",
    }

    dbatual = load_db(DB_CLIENTS, {})
    if clientid not in dbatual:
        return False, "Cliente não encontrado"

    conf = dbatual[clientid]["ftp"]
    remotedir = MESSAGESREMOTEPATHS.get(mapa)
    if not remotedir:
        return False, f"Caminho remoto não configurado para o mapa {mapa}"

    try:
        ftp = ftplib.FTP()
        ftp.connect(conf["host"], int(conf["port"]), timeout=15)
        ftp.login(conf["user"], conf["pass"])
        ftp.cwd(remotedir)

        with open(localpath, "rb") as f:
            ftp.storbinary("STOR messages.xml", f)

        ftp.quit()
        return True, "Sucesso"
    except Exception as e:
        return False, str(e)


def enviar_cfgeventspawns_via_ftp(clientid, localpath, mapa):
    """
    Envia o arquivo cfgeventspawns.xml para a raiz da missão do mapa.
    """
    remotedir = CFGEVENTSPAWNS_REMOTE_PATHS.get(mapa)
    if not remotedir:
        return False, f"Caminho remoto não configurado para o mapa {mapa}"

    return enviar_arquivo_via_ftp(
        clientid=clientid,
        localpath=localpath,
        remotedir=remotedir,
        remotefilename="cfgeventspawns.xml",
    )

def get_server_status_nitrado(client_id: str, nitrado_id: str) -> str:
    """
    Obtém o status do servidor via API Nitrado.
    Retorna: "stopped", "restarting", "online", ou "unknown"
    """
    try:
        NITRADO_TOKEN = os.environ.get("NITRADO_TOKEN", "")
        NITRADO_API = "https://api.nitrado.net"
        
        if not NITRADO_TOKEN or not nitrado_id:
            return "unknown"
        
        headers = {"Authorization": f"Bearer {NITRADO_TOKEN}"}
        url = f"{NITRADO_API}/services/{nitrado_id}/gameservers"
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            gs = data.get("data", {}).get("gameserver", {})
            status = gs.get("status", "unknown")
            return status  # "stopped", "starting", "online", etc
        return "unknown"
    except Exception:
        return "unknown"


def enviar_pedidos_via_ftp(client_id: str, pedidos: list, mapa: str = "Chernarus") -> bool:
    """
    Envia um arquivo JSON com os pedidos para o servidor via FTP.
    Arquivo é enviado para: dayzxb_missions/dayzOffline.{mapa}/custom/loja_pedidos.json
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


def worker_processar_pedidos():
    """
    Worker que processa pedidos de loja quando servidor está em restart/stopped.
    Executa a cada 30 segundos.
    """
    while True:
        try:
            db_all = load_db(DB_CLIENTS, {})
            mudou = False
            
            for client_id, client_info in db_all.items():
                # Obtém dados do cliente
                pedidos_list = client_info.get("pedidos", [])
                nitrado_id = client_info.get("nitrado_id", "")
                mapa = client_info.get("loja", {}).get("mapa_padrao", "Chernarus")
                ftp_config = client_info.get("ftp", {})
                
                if not pedidos_list or not ftp_config or not ftp_config.get("host"):
                    continue
                
                # Filtra apenas pedidos aguardando reset
                pedidos_pendentes = [p for p in pedidos_list if p.get("status") == "Aguardando Reset"]
                
                if not pedidos_pendentes:
                    continue
                
                # Obtém status do servidor
                server_status = get_server_status_nitrado(client_id, nitrado_id)
                
                # Se servidor está stopped ou restarting, processa pedidos
                if server_status in ["stopped", "restarting", "restart"]:
                    # Tenta enviar pedidos
                    success = enviar_pedidos_via_ftp(client_id, pedidos_pendentes, mapa)
                    
                    if success:
                        # Marca todos os pedidos como entregues
                        for pedido in pedidos_pendentes:
                            pedido["status"] = "Entregue"
                            pedido["data_entrega"] = get_hora_brasilia().strftime("%d/%m/%Y %H:%M")
                        
                        client_info["pedidos"] = pedidos_list
                        mudou = True
                        print(f"✅ Pedidos do cliente {client_id} entregues com sucesso via FTP")
            
            if mudou:
                save_db(DB_CLIENTS, db_all)
        
        except Exception as e:
            print(f"Erro no worker de pedidos: {e}")
        
        time.sleep(30)

# ---------- HELPERS GLOBALS.XML (AMBIENTE) ----------

# Lista das variáveis que vamos expor na UI primeiro
GLOBALS_KEYS_FOCO = [
    "AnimalMaxCount",
    "ZombieMaxCount",
    "CleanupLifetimeDeadPlayer",
    "IdleModeCountdown",
    "TimeLogin",
    "TimeLogout",
    # Se quiser, podemos adicionar mais depois
]


def parse_globals_xml(xml_bytes):
    """
    Lê o globals.xml e devolve:
    - tree, root (ElementTree)
    - vars_dict: dicionário {nome_var: (type, valor)}
    """
    tree = ET.ElementTree(ET.fromstring(xml_bytes))
    root = tree.getroot()
    vars_dict = {}

    for v in root.findall("var"):
        name = v.get("name")
        v_type = v.get("type")
        value_raw = v.get("value")
        if name is None:
            continue
        # Tentamos converter para int/float, mantendo string se falhar
        try:
            if "." in str(value_raw):
                value = float(value_raw)
            else:
                value = int(value_raw)
        except Exception:
            value = value_raw
        vars_dict[name] = {"type": v_type, "value": value, "elem": v}

    return tree, root, vars_dict


def apply_globals_changes(tree, root, vars_dict):
    """
    Aplica o dicionário vars_dict no XML e devolve bytes do novo globals.xml.
    Espera vars_dict no formato {name: {"type": "0", "value": valor_num}}.
    """
    for v in root.findall("var"):
        name = v.get("name")
        if name in vars_dict:
            info = vars_dict[name]
            v.set("type", str(info.get("type", v.get("type", "0"))))
            v.set("value", str(info.get("value")))

    xml_bytes = ET.tostring(root, encoding="utf-8", method="xml")
    header = b'<?xml version="1.0" encoding="utf-8"?>\n'
    return header + xml_bytes

# ---------- HELPERS LOJA / TRADER (JSON) ----------

LOJA_DEFAULT = {
    "mapa_padrao": "Chernarus",
    "posicao_padrao": "",
    "itens": []  # cada item: {id, nome, classe, categoria, preco, quantidade, ativo}
}


def load_loja_for_client(client_data_obj):
    """
    Garante que exista a estrutura de loja dentro do client_data.
    """
    if "loja" not in client_data_obj:
        client_data_obj["loja"] = LOJA_DEFAULT.copy()
    else:
        # Garante chaves básicas
        if "mapa_padrao" not in client_data_obj["loja"]:
            client_data_obj["loja"]["mapa_padrao"] = "Chernarus"
        if "posicao_padrao" not in client_data_obj["loja"]:
            client_data_obj["loja"]["posicao_padrao"] = ""
        if "itens" not in client_data_obj["loja"]:
            client_data_obj["loja"]["itens"] = []
    return client_data_obj["loja"]


def loja_itens_to_df(loja):
    """
    Converte a lista de itens da loja em DataFrame para edição no Streamlit.
    """
    import pandas as pd

    rows = loja.get("itens", [])
    if not rows:
        return pd.DataFrame(
            columns=["id", "nome", "classe", "categoria", "preco", "quantidade", "ativo"]
        )
    return pd.DataFrame(rows)


def df_to_loja_itens(df):
    """
    Converte o DataFrame editado de volta para lista de dicts.
    """
    itens = []
    for _, row in df.iterrows():
        if not row.get("nome") and not row.get("classe"):
            # ignora linhas totalmente vazias
            continue
        itens.append(
            {
                "id": int(row.get("id", 0)),
                "nome": str(row.get("nome", "")),
                "classe": str(row.get("classe", "")),
                "categoria": str(row.get("categoria", "")),
                "preco": int(row.get("preco", 0)),
                "quantidade": int(row.get("quantidade", 1)),
                "ativo": bool(row.get("ativo", True)),
            }
        )
    # ordena por id para manter catálogo organizado
    itens.sort(key=lambda x: x["id"])
    return itens

# ---------- HELPERS PLAYERS / VÍNCULOS ----------

PLAYERS_DEFAULT = {}  # dict: {gamertag: {...dados...}}


def load_players_for_client(client_data_obj):
    """
    Garante que exista a estrutura de players dentro do client_data.
    """
    if "players" not in client_data_obj:
        client_data_obj["players"] = PLAYERS_DEFAULT.copy()
    return client_data_obj["players"]


def players_to_df(players_dict):
    """
    Converte dict de players para DataFrame editável.
    """
    import pandas as pd

    rows = []
    for gamertag, info in players_dict.items():
        rows.append(
            {
                "gamertag": gamertag,
                "apelido": info.get("apelido", ""),
                "discord_id": info.get("discord_id", ""),
                "observacoes": info.get("observacoes", ""),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["gamertag", "apelido", "discord_id", "observacoes"])
    return pd.DataFrame(rows)


def df_to_players(df):
    """
    Converte DataFrame de volta para dict de players.
    """
    players = {}
    for _, row in df.iterrows():
        gamertag = str(row.get("gamertag", "")).strip()
        if not gamertag:
            continue
        players[gamertag] = {
            "gamertag": gamertag,
            "apelido": str(row.get("apelido", "")).strip(),
            "discord_id": str(row.get("discord_id", "")).strip(),
            "observacoes": str(row.get("observacoes", "")).strip(),
        }
    return players

# =========================================================
# 3. INICIALIZAÇÃO DE ESTADO
# =========================================================

if "db_users" not in st.session_state:
    st.session_state.db_users = load_db(
        DB_USERS,
        {
            "admin_key": "ALEX_ADMIN",
            "keys": {},
        },
    )

# Garante estrutura mínima em db_users, sem duplicar config_planos
st.session_state.db_users.setdefault("admin_key", "ALEX_ADMIN")
st.session_state.db_users.setdefault("keys", {})
st.session_state.db_users.setdefault("admin_email", "")
st.session_state.db_users.setdefault("mfa_code", "")
st.session_state.db_users.setdefault("mfa_expiry", "")

if "db_clients" not in st.session_state:
    st.session_state.db_clients = load_db(DB_CLIENTS, {})

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "user_key" not in st.session_state:
    st.session_state.user_key = None
if "view_mode" not in st.session_state:
    st.session_state.view_mode = "admin"


# =========================================================
# 4. SIDEBAR — TITAN CLOUD PRO
# =========================================================

with st.sidebar:
    st.subheader("Titan Cloud Pro")


# =========================================================
# 5. TELA DE LOGIN (APENAS PARA PORTAL DO ADMIN)
# =========================================================

if not st.session_state.get("authenticated"):
    st.title("🔑 Titan Cloud - Login (Admin)")

    dados_geo = buscar_localizacao_cliente()
    login_key = st.text_input("Insira sua KeyUser de administrador", type="password")

    with st.expander("🔑 Esqueci minha senha"):
        db_users_rec = load_db(DB_USERS, {"admin_key": "ALEX_ADMIN", "keys": {}})
        admin_email_rec = db_users_rec.get("admin_email", "")

        if not admin_email_rec:
            st.warning("Nenhum e-mail de recuperação cadastrado. Acesse o painel e cadastre em **Segurança da Conta**.")
        else:
            st.info(f"Um código será enviado para: **{admin_email_rec[:3]}***@{admin_email_rec.split('@')[-1]}")
            if st.button("📧 Enviar código de recuperação", use_container_width=True):
                import random
                # Bloqueia novo envio se já foi enviado há menos de 2 minutos
                ultimo_envio = db_users_rec.get("mfa_last_sent", "")
                if ultimo_envio:
                    try:
                        ultimo_dt = datetime.strptime(ultimo_envio, "%d/%m/%Y %H:%M:%S").replace(tzinfo=FUSO_BR)
                        segundos_passados = (get_hora_brasilia() - ultimo_dt).total_seconds()
                        if segundos_passados < 120:
                            st.warning(f"Aguarde {int(120 - segundos_passados)}s antes de solicitar outro código.")
                            st.stop()
                    except Exception:
                        pass
                codigo = str(random.randint(100000, 999999))
                db_users_rec["mfa_last_sent"] = get_hora_brasilia().strftime("%d/%m/%Y %H:%M:%S")
                expiry = (get_hora_brasilia() + timedelta(minutes=10)).strftime("%d/%m/%Y %H:%M")
                db_users_rec["mfa_code"] = codigo
                db_users_rec["mfa_expiry"] = expiry
                save_db(DB_USERS, db_users_rec)
                ok_email = enviar_email(
                    admin_email_rec,
                    "Titan Cloud - Código de recuperação",
                    f"Seu código de acesso é: {codigo}\nVálido por 10 minutos.",
                )
                if ok_email:
                    st.success("Código enviado! Verifique seu e-mail.")
                    st.session_state["mfa_recovery_mode"] = True
                else:
                    st.error("Falha ao enviar e-mail. Verifique as credenciais EMAIL_USER e EMAIL_PASS no ambiente.")

        if st.session_state.get("mfa_recovery_mode"):
            codigo_input = st.text_input("Digite o código recebido", max_chars=6)
            nova_senha = st.text_input("Nova senha (KeyUser)", type="password")
            confirmar_senha = st.text_input("Confirmar nova senha", type="password")

            if st.button("✅ Redefinir senha", use_container_width=True):
                db_users_rec2 = load_db(DB_USERS, {"admin_key": "ALEX_ADMIN", "keys": {}})
                codigo_salvo = db_users_rec2.get("mfa_code", "")
                expiry_str = db_users_rec2.get("mfa_expiry", "")

                try:
                    expiry_dt = datetime.strptime(expiry_str, "%d/%m/%Y %H:%M").replace(tzinfo=FUSO_BR)
                    ainda_valido = get_hora_brasilia() <= expiry_dt
                except Exception:
                    ainda_valido = False

                tentativas = db_users_rec2.get("mfa_tentativas", 0)
                if tentativas >= 5:
                    st.error("Muitas tentativas inválidas. Solicite um novo código.")
                    db_users_rec2["mfa_code"] = ""
                    db_users_rec2["mfa_tentativas"] = 0
                    save_db(DB_USERS, db_users_rec2)
                elif not codigo_salvo or codigo_input != codigo_salvo:
                    db_users_rec2["mfa_tentativas"] = tentativas + 1
                    save_db(DB_USERS, db_users_rec2)
                    st.error(f"Código inválido. Tentativa {tentativas + 1}/5.")
                elif not ainda_valido:
                    st.error("Código expirado. Solicite um novo.")
                elif nova_senha != confirmar_senha:
                    st.error("As senhas não coincidem.")
                elif len(nova_senha) < 6:
                    st.error("A nova senha deve ter pelo menos 6 caracteres.")
                else:
                    db_users_rec2["admin_key"] = nova_senha
                    db_users_rec2["mfa_code"] = ""
                    db_users_rec2["mfa_expiry"] = ""
                    db_users_rec2["mfa_tentativas"] = 0
                    db_users_rec2["mfa_last_sent"] = ""
                    save_db(DB_USERS, db_users_rec2)
                    st.session_state.db_users = db_users_rec2
                    st.session_state["mfa_recovery_mode"] = False
                    st.success("Senha redefinida com sucesso! Faça login com a nova senha.")

    if st.button("Entrar no Painel", use_container_width=True):
        ok, cargo = validar_acesso(login_key)

        if ok and cargo == "admin":
            token_sessao = secrets.token_hex(8)

            if dados_geo:
                local_final = f"{dados_geo['cidade']} - {dados_geo['estado']}"
            else:
                local_final = "Localização não capturada"

            db_users = st.session_state.db_users
            db_users["admin_last_session"] = token_sessao
            db_users["admin_local"] = local_final
            db_users["admin_last_login"] = get_hora_brasilia().strftime("%d/%m/%Y %H:%M:%S")
            save_db(DB_USERS, db_users)

            st.session_state.authenticated = True
            st.session_state.user_key = login_key
            st.session_state.role = "admin"
            st.session_state.session_token = token_sessao
            st.session_state.view_mode = "admin"

            st.rerun()
        elif ok and cargo == "client":
            token_sessao = secrets.token_hex(8)

            st.session_state.authenticated = True
            st.session_state.user_key = login_key
            st.session_state.role = "client"
            st.session_state.session_token = token_sessao
            st.session_state.view_mode = "client"

            user_info_login = st.session_state.db_users["keys"][login_key]
            user_info_login["last_session"] = token_sessao
            user_info_login["last_login"] = get_hora_brasilia().strftime("%d/%m/%Y %H:%M:%S")
            save_db(DB_USERS, st.session_state.db_users)

            start_worker_once()
            st.rerun()
        else:
            st.error(cargo)

    st.stop()


# =========================================================
# 6. ÁREA DO ADMINISTRADOR
# =========================================================

if st.session_state.role == "admin" and st.session_state.view_mode == "admin":
    with st.sidebar:
        st.subheader("🛡️ Menu Admin")
        if st.button("🚀 Usar Sistema (Modo Teste)", use_container_width=True):
            st.session_state.view_mode = "client"
            st.rerun()
        if st.button("🔴 Logout (Admin)", use_container_width=True):
            for k in ["authenticated", "role", "view_mode", "user_key", "session_token"]:
                st.session_state.pop(k, None)
            st.rerun()

    st.title("🛡️ Painel de Controle - Administrador")

    # -------------------------------------------------------
    # SEGURANÇA DA CONTA (troca de senha + cadastro de email)
    # -------------------------------------------------------
    with st.expander("🔐 Segurança da Conta", expanded=False):
        db_seg = load_db(DB_USERS, {"admin_key": "ALEX_ADMIN", "keys": {}})

        st.markdown("#### 📧 E-mail de recuperação")
        email_atual = db_seg.get("admin_email", "")
        novo_email = st.text_input("E-mail para recuperação de senha", value=email_atual, key="seg_email_input")
        if st.button("💾 Salvar e-mail", key="btn_salvar_email", use_container_width=True):
            if "@" not in novo_email or "." not in novo_email:
                st.error("E-mail inválido.")
            else:
                db_seg["admin_email"] = novo_email
                save_db(DB_USERS, db_seg)
                st.session_state.db_users["admin_email"] = novo_email
                st.success(f"E-mail de recuperação salvo: {novo_email}")

        st.divider()
        st.markdown("#### 🔑 Trocar senha (KeyUser Admin)")
        senha_atual_input = st.text_input("Senha atual", type="password", key="seg_senha_atual")
        nova_senha_input = st.text_input("Nova senha", type="password", key="seg_nova_senha")
        confirmar_nova = st.text_input("Confirmar nova senha", type="password", key="seg_confirmar_nova")

        if st.button("🔄 Alterar senha", key="btn_alterar_senha", use_container_width=True):
            db_seg2 = load_db(DB_USERS, {"admin_key": "ALEX_ADMIN", "keys": {}})
            if senha_atual_input != db_seg2.get("admin_key", ""):
                st.error("Senha atual incorreta.")
            elif nova_senha_input != confirmar_nova:
                st.error("As novas senhas não coincidem.")
            elif len(nova_senha_input) < 6:
                st.error("A nova senha deve ter pelo menos 6 caracteres.")
            else:
                db_seg2["admin_key"] = nova_senha_input
                save_db(DB_USERS, db_seg2)
                st.session_state.db_users["admin_key"] = nova_senha_input
                st.success("Senha alterada com sucesso! Use a nova senha no próximo login.")

    tab_adm1, tab_adm2, tab_adm3, tab_adm4, tab_adm5 = st.tabs(
        [
            "➕ Gerar Chaves",
            "👥 Gestão de Clientes",
            "⚙️ Configurar Planos",
            "💾 Backup/Restore",
            "📢 Comunicados",
        ]
    )

    # --- TAB 1: GERAR CHAVES ---
    with tab_adm1:
        with st.expander("Gerador de Chaves", expanded=True):
            col_gen1, col_gen2 = st.columns([2, 1])

            # Coluna esquerda: dados do cliente/servidor + KeyUser
            with col_gen1:
                srv_name = st.text_input("Nome do Servidor / Cliente")
                nitrado_id = st.text_input(
                    "ID do Servidor na Nitrado (opcional, ex.: 18927875)",
                    placeholder="Se preencher, será usado como ID interno do servidor",
                )
                
                nitrado_token_input = st.text_input(
                    "Token Nitrado do Cliente",
                    placeholder="Token gerado na conta Nitrado do cliente",
                    type="password",
                )
                
                discord_guild_id_input = st.text_input(
                    "ID do Servidor Discord (Guild ID)",
                    placeholder="Ex.: 1234567890123456789",
                    help=(
                        "ID numérico do servidor Discord do administrador. "
                        "Discord > Configurações > Avançado > Modo desenvolvedor > "
                        "Botão direito no servidor > Copiar ID do servidor."
                    ),
                )
                plano_sel = st.selectbox("Escolha o Plano", list(PLANOS.keys()))

                if "temp_key" not in st.session_state:
                    st.session_state.temp_key = ""
                ck1, ck2 = st.columns([3, 1])
                new_k = ck1.text_input(
                    "KeyUser (chave de acesso)",
                    value=st.session_state.temp_key,
                )

                if ck2.button("🎲 Gerar"):
                    st.session_state.temp_key = "".join(
                        secrets.choice(string.ascii_uppercase + string.digits)
                        for _ in range(12)
                    )
                    st.rerun()

            # Coluna direita: validade e gravação
            with col_gen2:
                dias_v = st.number_input("Dias de validade", min_value=1, value=30)

                if st.button("🚀 Registrar e Ativar", use_container_width=True):
                    if not srv_name or not new_k:
                        st.error("Preencha o nome do servidor/cliente e a KeyUser.")
                    else:
                        # 1) Definir ID interno do servidor (server_id)
                        if nitrado_id.strip():
                            server_id = nitrado_id.strip()
                        else:
                            server_id = "".join(
                                secrets.choice(string.ascii_uppercase + string.digits)
                                for _ in range(12)
                            )

                        # 2) Calcular data de expiração
                        data_exp = (
                            get_hora_brasilia() + timedelta(days=dias_v)
                        ).strftime("%d/%m/%Y")

                        # 3) Registrar no users_db: KeyUser -> dados + server_id
                        st.session_state.db_users["keys"][new_k] = {
                            "server": srv_name,
                            "server_id": server_id,
                            "expires": data_exp,
                            "plano": plano_sel,
                            "discord_guild_id": discord_guild_id_input.strip(),
                            "nitrado_token": nitrado_token_input.strip(),
                        }
                        save_db(DB_USERS, st.session_state.db_users)

                        # 4) Inicializar estrutura em db_clients para esse server_id
                        if server_id not in st.session_state.db_clients:
                            st.session_state.db_clients[server_id] = {
                                "ftp": {
                                    "host": "",
                                    "user": "",
                                    "pass": "",
                                    "port": "21",
                                },
                                "agendas": [],
                                "logs": [],
                                "comunicados": [],
                                "players": {},
                            }
                            save_db(DB_CLIENTS, st.session_state.db_clients)

                        st.session_state.temp_key = ""
                        st.success(
                            f"Chave para '{srv_name}' ativada!\n\n"
                            f"- KeyUser (login do cliente): {new_k}\n"
                            f"- ID interno do servidor: {server_id}\n"
                            f"- Guild Discord: {discord_guild_id_input.strip() or '(não informado)'}"
                        )
                        st.rerun()

    # --- TAB 2: GESTÃO DE CLIENTES ---
    with tab_adm2:
        st.subheader("👥 Gestão de Clientes Ativos")
        if not st.session_state.db_users["keys"]:
            st.info("Nenhum cliente cadastrado no momento.")

        for k, v in list(st.session_state.db_users["keys"].items()):
            dt_exp_check = datetime.strptime(v["expires"], "%d/%m/%Y").date()
            dias_rest = (dt_exp_check - get_hora_brasilia().date()).days
            cor_status = "🟢" if dias_rest > 0 else "🔴"

            limites_globais = st.session_state.db_users.get("config_planos", PLANOS)
            uso_atual = len(st.session_state.db_clients.get(k, {}).get("agendas", []))
            limite_padrao = limites_globais.get(v.get("plano", "Starter"), 2)
            limite_final = v.get("limite_extra", limite_padrao)

            with st.expander(
                f"{cor_status} {v['server']} | {v.get('plano', 'Starter')} ({uso_atual}/{limite_final})"
            ):
                st.markdown("### 🔑 Credenciais de Acesso")
                st.code(k)
                st.divider()

                st.markdown("#### 🌐 Monitoramento e Segurança")
                col_mon1, col_mon2 = st.columns(2)
                with col_mon1:
                    st.write(f"**📍 Localização:** {v.get('local', 'Nenhum acesso registrado')}")
                    st.write(f"**🖥️ IP:** {v.get('last_ip', '0.0.0.0')}")
                with col_mon2:
                    st.write(f"**🕒 Último Login:** {v.get('last_login', '---')}")
                    if st.button("🚫 Banir Acesso (Expirar Key)", key=f"ban_{k}", type="primary", use_container_width=True):
                        v["expires"] = (get_hora_brasilia() - timedelta(days=1)).strftime("%d/%m/%Y")
                        save_db(DB_USERS, st.session_state.db_users)
                        st.warning(f"O acesso de {v['server']} foi bloqueado.")
                        st.rerun()

                st.divider()

                st.markdown("#### ⚙️ Configurações do Servidor")
                client_cfg = st.session_state.db_clients.get(k, {})
                ftp_cfg = client_cfg.get("ftp", {})
                col_cfg1, col_cfg2 = st.columns(2)
                with col_cfg1:
                    st.write(f"**🎮 Nitrado ID:** `{v.get('server_id', 'Não configurado')}`")
                    st.write(f"**💬 Discord Guild ID:** `{v.get('discord_guild_id', 'Não configurado')}`")
                    nitrado_token_salvo = v.get('nitrado_token', '')
                    nitrado_token_masked = nitrado_token_salvo[:4] + "*" * (len(nitrado_token_salvo) - 4) if len(nitrado_token_salvo) > 4 else "Não configurado"
                    st.write(f"**🔑 Nitrado Token:** `{nitrado_token_masked}`")
                    novo_token = st.text_input("Atualizar Nitrado Token", value=nitrado_token_salvo, type="password", key=f"ntoken_{k}")
                    if st.button("💾 Salvar Token", key=f"save_ntoken_{k}", use_container_width=True):
                        st.session_state.db_users["keys"][k]["nitrado_token"] = novo_token.strip()
                        save_db(DB_USERS, st.session_state.db_users)
                        st.success("Token Nitrado atualizado!")
                        st.rerun()
                with col_cfg2:
                    st.write(f"**🖥️ FTP Host:** `{ftp_cfg.get('host', 'Não configurado')}`")
                    st.write(f"**👤 FTP User:** `{ftp_cfg.get('user', 'Não configurado')}`")
                    ftp_pass = ftp_cfg.get('pass', '')
                    ftp_pass_masked = ftp_pass[:2] + "*" * (len(ftp_pass) - 2) if len(ftp_pass) > 2 else "Não configurado"
                    st.write(f"**🔒 FTP Pass:** `{ftp_pass_masked}`")
                    st.write(f"**🔌 FTP Port:** `{ftp_cfg.get('port', '21')}`")

                st.divider()

                c_edit1, c_edit2 = st.columns(2)
                with c_edit1:
                    st.markdown("#### 📝 Informações e Plano")
                    new_n = st.text_input("Editar Nome", value=v["server"], key=f"n_{k}")
                    new_p = st.selectbox("Trocar Plano", list(PLANOS.keys()), index=list(PLANOS.keys()).index(v.get("plano", "Starter")), key=f"p_{k}")
                    new_lim = st.number_input("Ajustar Limite", min_value=1, value=int(limite_final), key=f"lim_{k}")

                    if st.button("💾 Salvar Alterações", key=f"bn_{k}", use_container_width=True):
                        st.session_state.db_users["keys"][k]["server"] = new_n
                        st.session_state.db_users["keys"][k]["plano"] = new_p
                        st.session_state.db_users["keys"][k]["limite_extra"] = new_lim
                        save_db(DB_USERS, st.session_state.db_users)
                        st.success("Dados atualizados!")
                        st.rerun()

                with c_edit2:
                    st.markdown("#### 📅 Validade do Acesso")
                    st.write(f"**Expira em:** {v['expires']} ({dias_rest} dias)")
                    add_d = st.number_input("Adicionar dias", min_value=1, value=30, key=f"d_{k}")
                    if st.button("➕ Estender/Renovar", key=f"bd_{k}", use_container_width=True):
                        nova_data = (dt_exp_check + timedelta(days=add_d)).strftime("%d/%m/%Y")
                        st.session_state.db_users["keys"][k]["expires"] = nova_data
                        save_db(DB_USERS, st.session_state.db_users)
                        st.success(f"Estendido para {nova_data}!")
                        st.rerun()

                st.divider()
                st.markdown("#### 🛒 Ações Administrativas")
                
                if st.button("💾 Salvar/Sincronizar Loja", key=f"save_loja_{k}", use_container_width=True):
                    db_completo = load_db(DB_CLIENTS, {})
                    if k not in db_completo:
                        db_completo[k] = {}
                    
                    # Busca o DataFrame editado que foi salvo com a chave única do cliente
                    df_loja_key = f"df_loja_{k}"
                    df_editado = st.session_state.get(df_loja_key, pd.DataFrame())
                    itens_atualizados = df_to_loja_itens(df_editado)
                    
                    db_completo[k]["loja"] = {
                        "mapa_padrao": "Chernarus",
                        "posicao_padrao": "",
                        "itens": itens_atualizados
                    }
                    save_db(DB_CLIENTS, db_completo)
                    st.session_state.db_clients = db_completo
                    st.success(f"Loja do servidor {v['server']} sincronizada com sucesso!")

                st.divider()
                if st.button("🗑️ EXCLUIR CLIENTE PERMANENTEMENTE", key=f"del_{k}", type="primary", use_container_width=True):
                    # Relê do disco para garantir dados atualizados
                    db_users_fresh = load_db(DB_USERS, {"admin_key": "ALEX_ADMIN", "keys": {}})
                    db_clients_fresh = load_db(DB_CLIENTS, {})

                    if k in db_users_fresh["keys"]:
                        del db_users_fresh["keys"][k]
                    if k in db_clients_fresh:
                        del db_clients_fresh[k]

                    save_db(DB_USERS, db_users_fresh)
                    save_db(DB_CLIENTS, db_clients_fresh)

                    st.session_state.db_users = db_users_fresh
                    st.session_state.db_clients = db_clients_fresh
                    st.rerun()

        # --- TAB 3: CONFIG PLANOS ---
        with tab_adm3:
            st.subheader("⚙️ Configuração Global de Limites")
            if "config_planos" not in st.session_state.db_users:
                st.session_state.db_users["config_planos"] = PLANOS.copy()
            conf_planos = st.session_state.db_users["config_planos"]

            col_p1, col_p2, col_p3 = st.columns(3)
            with col_p1:
                novo_starter = st.number_input(
                    "Starter",
                    min_value=1,
                    value=conf_planos.get("Starter", 2),
                    key="conf_starter",
                )
            with col_p2:
                novo_pro = st.number_input(
                    "Pro",
                    min_value=1,
                    value=conf_planos.get("Pro", 10),
                    key="conf_pro",
                )
            with col_p3:
                novo_ent = st.number_input(
                    "Enterprise",
                    min_value=1,
                    value=conf_planos.get("Enterprise", 999),
                    key="conf_ent",
                )

            if st.button("🚀 Aplicar Limites Globais", use_container_width=True):
                st.session_state.db_users["config_planos"] = {
                    "Starter": novo_starter,
                    "Pro": novo_pro,
                    "Enterprise": novo_ent,
                }
                save_db(DB_USERS, st.session_state.db_users)
                st.success("Limites globais atualizados!")
                time.sleep(1)
                st.rerun()

        # --- TAB 4: BACKUP / RESTORE ---
        with tab_adm4:
            st.subheader("📦 Central de Migração de Dados")
            st.info("Faça backup antes de atualizar e restaure logo após o deploy.")

            def get_default_db_users():
                return {
                    "admin_key": "ALEXADMIN",
                    "keys": {},
                    "config_planos": {
                        "Starter": 2,
                        "Pro": 8,
                        "Enterprise": 16,
                    },
                    "admin_email": "",
                    "mfa_code": "",
                    "mfa_expiry": "",
                    "mfa_last_sent": "",
                    "mfa_tentativas": 0,
                    "admin_last_session": "",
                    "admin_local": "",
                    "admin_last_login": "",
                }

            def get_default_client_data():
                return {
                    "ftp": {
                        "host": "",
                        "user": "",
                        "pass": "",
                        "port": 21,
                    },
                    "agendas": [],
                    "agendas_raid": [],
                    "logs": [],
                    "comunicados": [],
                    "players": {},
                    "tracking_acoes": {},
                    "wallets": {},
                    "bank": {},
                    "pedidos": [],
                    "xpstats": {},
                    "webhooks_config": [],
                    "heatmap_data": [],
                    "loja": {
                        "mapa_padrao": "Chernarus",
                        "posicao_padrao": "",
                        "itens": [],
                    },
                    "dzcoins_config": {
                        "ativo": False,
                        "quantidade_dzcoins": 10,
                        "intervalo_minutos": 60,
                    },
                    "feeds_config": {
                        "coordenadas_killfeed": True,
                        "feed_conexao": True,
                        "feed_construcao": True,
                        "combatlog": True,
                        "ping_adm": True,
                        "loja_automatica": True,
                        "glitch_subsolo": True,
                        "glitch_fogueiras": True,
                        "glitch_hortas": True,
                        "ranking": True,
                        "ranking_auto": True,
                        "baixar_logs": True,
                        "mod_pve": False,
                        "zona_pvp": False,
                        "mapa_calor": True,
                        "playersonlineauto": True,
                        "webhookplayersonline": "",
                        "webhook_admin_logs": "",
                    },
                }

            def merge_dict_structure(base_dict, default_dict):
                for key, value in default_dict.items():
                    if key not in base_dict:
                        if isinstance(value, dict):
                            base_dict[key] = value.copy()
                        elif isinstance(value, list):
                            base_dict[key] = value.copy()
                        else:
                            base_dict[key] = value
                    else:
                        if isinstance(value, dict) and isinstance(base_dict.get(key), dict):
                            merge_dict_structure(base_dict[key], value)
                return base_dict

            col_back, col_rest = st.columns(2)

            with col_back:
                st.markdown("### ⬇️ Exportar Backup")
                dados_totais = {
                    "users": st.session_state.db_users,
                    "clients": st.session_state.db_clients,
                }
                json_string = json.dumps(dados_totais, indent=4, ensure_ascii=False)
                st.download_button(
                    label="💾 Baixar Backup Geral (JSON)",
                    data=json_string,
                    file_name=f"backup_titan_{get_hora_brasilia().strftime('%d_%m_%Y')}.json",
                    mime="application/json",
                    use_container_width=True,
                )

            with col_rest:
                st.markdown("### ⬆️ Importar/Restaurar")
                arquivo_upload = st.file_uploader(
                    "Selecione o arquivo de backup", type="json"
                )
                if st.button(
                    "🚀 Restaurar Dados Agora",
                    use_container_width=True,
                    type="primary",
                ):
                    if arquivo_upload is not None:
                        try:
                            backup_data = json.load(arquivo_upload)
                            if "users" in backup_data and "clients" in backup_data:
                                st.session_state.db_users = backup_data["users"]
                                st.session_state.db_clients = backup_data["clients"]
                                save_db(DB_USERS, st.session_state.db_users)
                                save_db(DB_CLIENTS, st.session_state.db_clients)
                                st.success("✅ Restauração concluída!")
                                time.sleep(2)
                                st.rerun()
                            else:
                                st.error("❌ Arquivo inválido!")
                        except Exception as e:
                            st.error(f"❌ Erro: {e}")

            st.divider()
            st.markdown("### 🛠️ Manutenção da Base")
            st.caption(
                "Use a correção estrutural para completar campos ausentes sem apagar dados. "
                "Use o reset total apenas se quiser começar tudo do zero."
            )

            col_fix, col_reset = st.columns(2)

            with col_fix:
                if st.button(
                    "🧩 Corrigir Estrutura dos Bancos",
                    use_container_width=True,
                ):
                    try:
                        db_users_atual = load_db(DB_USERS, {})
                        db_clients_atual = load_db(DB_CLIENTS, {})

                        db_users_corrigido = merge_dict_structure(
                            db_users_atual, get_default_db_users()
                        )

                        if "keys" not in db_users_corrigido or not isinstance(
                            db_users_corrigido["keys"], dict
                        ):
                            db_users_corrigido["keys"] = {}

                        if "config_planos" not in db_users_corrigido or not isinstance(
                            db_users_corrigido["config_planos"], dict
                        ):
                            db_users_corrigido["config_planos"] = {
                                "Starter": 2,
                                "Pro": 8,
                                "Enterprise": 16,
                            }

                        if not isinstance(db_clients_atual, dict):
                            db_clients_atual = {}

                        for server_id, client_data in db_clients_atual.items():
                            if not isinstance(client_data, dict):
                                db_clients_atual[server_id] = get_default_client_data()
                            else:
                                db_clients_atual[server_id] = merge_dict_structure(
                                    client_data, get_default_client_data()
                                )

                        save_db(DB_USERS, db_users_corrigido)
                        save_db(DB_CLIENTS, db_clients_atual)

                        st.session_state.db_users = db_users_corrigido
                        st.session_state.db_clients = db_clients_atual

                        st.success("✅ Estrutura dos bancos corrigida com sucesso!")
                        time.sleep(2)
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Erro ao corrigir estrutura: {e}")

            with col_reset:
                if "confirmar_reset_base" not in st.session_state:
                    st.session_state.confirmar_reset_base = False

                if not st.session_state.confirmar_reset_base:
                    if st.button(
                        "🗑️ Resetar Base Completa",
                        use_container_width=True,
                        type="primary",
                    ):
                        st.session_state.confirmar_reset_base = True
                        st.warning(
                            "⚠️ Clique novamente para confirmar o reset total da base."
                        )
                        st.rerun()
                else:
                    st.error(
                        "⚠️ Atenção: isso apagará todos os clientes e acessos cadastrados."
                    )

                    col_confirma, col_cancela = st.columns(2)

                    with col_confirma:
                        if st.button(
                            "✅ Confirmar Reset Total",
                            use_container_width=True,
                            type="primary",
                        ):
                            try:
                                db_users_atual = load_db(DB_USERS, {})
                                admin_key_atual = db_users_atual.get("admin_key", "ALEXADMIN")
                                admin_email_atual = db_users_atual.get("admin_email", "")

                                novo_db_users = get_default_db_users()
                                novo_db_users["admin_key"] = admin_key_atual
                                novo_db_users["admin_email"] = admin_email_atual

                                novo_db_clients = {}

                                save_db(DB_USERS, novo_db_users)
                                save_db(DB_CLIENTS, novo_db_clients)

                                st.session_state.db_users = novo_db_users
                                st.session_state.db_clients = novo_db_clients
                                st.session_state.confirmar_reset_base = False

                                st.success("✅ Base resetada com sucesso!")
                                time.sleep(2)
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Erro ao resetar base: {e}")

                    with col_cancela:
                        if st.button(
                            "❎ Cancelar Reset",
                            use_container_width=True,
                        ):
                            st.session_state.confirmar_reset_base = False
                            st.info("Reset cancelado.")
                            time.sleep(1)
                            st.rerun()

        # --- TAB 5: COMUNICADOS ---
        with tab_adm5:
            st.subheader("📢 Enviar Comunicado Oficial")
            col_c1, col_c2 = st.columns([1, 2])

            with col_c1:
                opcoes_clientes = {
                    v["server"]: k for k, v in st.session_state.db_users["keys"].items()
                }
                alvos = st.multiselect(
                    "Enviar para:", options=["Todos"] + list(opcoes_clientes.keys()), default="Todos"
                )

                st.write("**Enviar via:**")
                send_sys = st.checkbox("Painel (Sistema)", value=True, disabled=True)
                send_mail = st.checkbox("E-mail")
                send_wa = st.checkbox("WhatsApp")
                send_disc = st.checkbox("Discord (Webhook do Cliente)")

            with col_c2:
                titulo_com = st.text_input(
                    "Título do Comunicado",
                    placeholder="Ex: Manutenção Programada",
                    key="input_tit_com",
                )
                corpo_com = st.text_area(
                    "Mensagem",
                    height=200,
                    placeholder="Escreva aqui os detalhes...",
                    key="input_msg_com",
                )

                if st.button(
                    "🚀 Disparar Comunicado", use_container_width=True, type="primary"
                ):
                    if titulo_com and corpo_com:
                        st.session_state.db_users = load_db(
                            DB_USERS, {"admin_key": "ALEX_ADMIN", "keys": {}}
                        )
                        st.session_state.db_clients = load_db(DB_CLIENTS, {})

                        if "Todos" in alvos:
                            destinatarios = list(st.session_state.db_users["keys"].keys())
                        else:
                            destinatarios = [
                                opcoes_clientes[nome] for nome in alvos
                            ]

                        comunicado_obj = {
                            "id": str(time.time()),
                            "data": get_hora_brasilia().strftime("%d/%m/%Y %H:%M"),
                            "titulo": titulo_com,
                            "mensagem": corpo_com,
                            "lido": False,
                        }

                        for d_id in destinatarios:
                            if d_id not in st.session_state.db_clients:
                                st.session_state.db_clients[d_id] = {
                                    "ftp": {
                                        "host": "",
                                        "user": "",
                                        "pass": "",
                                        "port": "21",
                                    },
                                    "agendas": [],
                                    "logs": [],
                                    "comunicados": [],
                                }
                            if "comunicados" not in st.session_state.db_clients[d_id]:
                                st.session_state.db_clients[d_id]["comunicados"] = []

                            st.session_state.db_clients[d_id]["comunicados"].insert(
                                0, comunicado_obj
                            )

                            if send_disc:
                                webhook_url = st.session_state.db_clients.get(d_id, {}).get(
                                    "discord_webhook"
                                )
                                if webhook_url:
                                    try:
                                        payload = {
                                            "embeds": [
                                                {
                                                    "title": f"📢 {titulo_com}",
                                                    "description": corpo_com,
                                                    "color": 16711680,
                                                }
                                            ]
                                        }
                                        requests.post(
                                            webhook_url, json=payload, timeout=5
                                        )
                                    except Exception:
                                        pass

                            if send_mail:
                                email_cli = (
                                    st.session_state.db_users["keys"]
                                    .get(d_id, {})
                                    .get("email")
                                )
                                if email_cli:
                                    enviar_email(email_cli, titulo_com, corpo_com)

                            if send_wa:
                                wpp_cli = (
                                    st.session_state.db_users["keys"]
                                    .get(d_id, {})
                                    .get("whatsapp")
                                )
                                if wpp_cli:
                                    enviar_whatsapp(wpp_cli, corpo_com)

                        save_db(DB_CLIENTS, st.session_state.db_clients)

                        if "input_tit_com" in st.session_state:
                            del st.session_state["input_tit_com"]
                        if "input_msg_com" in st.session_state:
                            del st.session_state["input_msg_com"]

                        st.success(f"✅ Enviado para {len(destinatarios)} clientes!")
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.error("Preencha o título e a mensagem.")

    st.stop()  # Admin viu o painel, para aqui

elif st.session_state.get("view_mode") == "client":
    pass  # continua para a Área do Cliente abaixo

# =========================================================
# 7. ÁREA DO CLIENTE
# =========================================================

user_id = st.session_state.user_key

db_disco_clients = load_db(DB_CLIENTS, {})
db_disco_users = load_db(DB_USERS, {"admin_key": "ALEX_ADMIN", "keys": {}})
st.session_state.db_clients = db_disco_clients
st.session_state.db_users = db_disco_users

# Admin em modo teste: usa o primeiro cliente disponível como referência
# Cliente normal: usa sua própria user_key
if st.session_state.get("role") == "admin":
    chaves_clientes = list(st.session_state.db_users.get("keys", {}).keys())
    if not chaves_clientes:
        st.warning("Nenhum cliente cadastrado ainda. Cadastre um cliente primeiro no Painel Admin.")
        if st.button("⚙️ Voltar ao Painel Admin"):
            st.session_state.view_mode = "admin"
            st.rerun()
        st.stop()

    with st.sidebar:
        st.subheader("🔍 Modo Teste — Selecione o Cliente")
        nomes_clientes = {
            v["server"]: k
            for k, v in st.session_state.db_users["keys"].items()
        }
        cliente_sel_nome = st.selectbox(
            "Visualizar como cliente:",
            list(nomes_clientes.keys()),
            key="admin_cliente_sel",
        )
        user_id = nomes_clientes[cliente_sel_nome]
else:
    user_id = st.session_state.user_key

if user_id not in st.session_state.db_clients:
    st.session_state.db_clients[user_id] = {}
client_data = st.session_state.db_clients[user_id]

# --- PASSO 1: INICIALIZAÇÃO DA ESTRUTURA FTP ---
if "ftp" not in client_data:
    client_data["ftp"] = {"host": "", "user": "", "pass": "", "port": "21"}
    save_db(DB_CLIENTS, st.session_state.db_clients)

# --- PASSO 1.1: INICIALIZAÇÃO DAS LISTAS ESSENCIAIS ---
if "agendas" not in client_data:
    client_data["agendas"] = []
    save_db(DB_CLIENTS, st.session_state.db_clients)

if "agendas_raid" not in client_data:
    client_data["agendas_raid"] = []
    save_db(DB_CLIENTS, st.session_state.db_clients)

if "logs" not in client_data:
    client_data["logs"] = []
    save_db(DB_CLIENTS, st.session_state.db_clients)

if "comunicados" not in client_data:
    client_data["comunicados"] = []
    save_db(DB_CLIENTS, st.session_state.db_clients)

# --- PASSO 2: INICIALIZAÇÃO DA ESTRUTURA DE FEEDS (GRC/GOVERNANÇA) ---
if "feeds_config" not in client_data:
    client_data["feeds_config"] = {
        "coordenadas_killfeed": True,
        "feed_conexao": True,
        "feed_construcao": True,
        "combatlog": True,
        "ping_adm": True,
        "loja_automatica": True,
        "glitch_subsolo": True,
        "ranking": True,
        "baixar_logs": True,
        "mod_pve": False,
        "zona_pvp": False
    }
    save_db(DB_CLIENTS, st.session_state.db_clients)

if "ranking_config" not in client_data:
    client_data["ranking_config"] = {
        "ativo": True,
        "datainicial": "",
        "modoexibicao": "cumulativo",
        "tipojanela": "temporada",
        "permitirreprocessamento": True,
        "ultimareconfiguracao": "",
    }
    save_db(DB_CLIENTS, st.session_state.db_clients)

if "rankingstats" not in client_data:
    client_data["rankingstats"] = {
        "ultimaatualizacao": "",
        "periodoatual": "",
        "acumulado": {},
        "diario": {},
        "semanal": {},
        "mensal": {},
    }
    save_db(DB_CLIENTS, st.session_state.db_clients)
    
# --- PASSO 7: ESTRUTURA PARA DETECÇÃO DE SPAM DE OBJETOS (GRC) ---
if "tracking_acoes" not in client_data:
    client_data["tracking_acoes"] = {} 
    save_db(DB_CLIENTS, st.session_state.db_clients)

# Sempre relê do disco para garantir que alterações do admin sejam refletidas
_db_users_fresh = load_db(DB_USERS, {"admin_key": "ALEX_ADMIN", "keys": {}})

user_info = _db_users_fresh["keys"].get(
    user_id,
    {
        "server": "Servidor (Admin Teste)",
        "plano": "Admin",
        "expires": "31/12/2099",
    }
)

# Atualiza o session_state para manter consistência
st.session_state.db_users = _db_users_fresh

if st.session_state.role == "client":
    token_valido = user_info.get("last_session")
    if st.session_state.get("session_token") != token_valido:
        st.error("⚠️ Sessão Finalizada: Esta conta foi conectada em outro local.")
        if st.button(
            "Fazer Login Novamente",
            use_container_width=True,
            key="relogin_btn",
        ):
            st.session_state.authenticated = False
            st.rerun()
        st.stop()

# Leitura segura do plano com fallback para Starter
plano_atual = user_info.get("plano", "Starter")

# Admin sempre tem acesso total
if st.session_state.get("role") == "admin" and st.session_state.get("view_mode") == "admin":
    plano_atual = "Enterprise"

limite_agendas = int(
    user_info.get(
        "limite_extra",
        st.session_state.db_users.get("config_planos", PLANOS).get(plano_atual, 2),
    )
)
total_agendas = len(client_data.get("agendas", []))

if st.session_state.role == "admin":
    exp_status = "Ilimitado (Admin)"
else:
    try:
        dt_exp_obj = datetime.strptime(user_info["expires"], "%d/%m/%Y").date()
        dias_restantes = (dt_exp_obj - get_hora_brasilia().date()).days
        exp_status = f"{max(0, dias_restantes)} dias"
    except Exception:
        exp_status = "Erro na data"

# --- SIDEBAR CLIENTE ---
with st.sidebar:
    st.title("👤 Minha Conta")

    if st.session_state.role == "admin":
        if st.button(
            "⚙️ VOLTAR AO PAINEL ADMIN",
            type="primary",
            use_container_width=True,
            key="back_to_adm_main",
        ):
            st.session_state.view_mode = "admin"
            st.rerun()

    st.write(f"Servidor: **{user_info['server']}**")

    # --- Badge do plano ---
    cor_plano = (
        "#FFD700" if plano_atual == "Enterprise"
        else "#00d4ff" if plano_atual == "Pro"
        else "#aaaaaa"
    )
    # Cores de texto para melhor legibilidade no fundo escuro
    cor_texto_plano = (
        "#FFD700" if plano_atual == "Enterprise"
        else "#00d4ff" if plano_atual == "Pro"
        else "#aaaaaa"
    )
    cor_label_escura = (
        "#FFD700" if plano_atual == "Enterprise"
        else "#00d4ff" if plano_atual == "Pro"
        else "#aaaaaa"
    )
    icone_plano = (
        "👑" if plano_atual == "Enterprise"
        else "⭐" if plano_atual == "Pro"
        else "🔹"
    )

    st.markdown(
        f"""
        <div style="
            background:#1a1a2e;
            border:2px solid {cor_plano};
            border-radius:8px;
            padding:14px 12px;
            margin-bottom:10px;
            text-align:center;
        ">
            <div style="font-size:24px;">{icone_plano}</div>
            <div style="font-size:15px; font-weight:bold;
                        color:#ffffff; margin-top:6px;">
                Plano {plano_atual}
            </div>
            <div style="font-size:12px; color:{cor_label_escura}; margin-top:3px; font-weight:600;">
                Expira em: {exp_status}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # --- Funcionalidades do plano ---
    st.markdown(
        "<div style='font-size:12px; color:#222222; margin-bottom:4px;'>"
        "Funcionalidades do seu plano:</div>",
        unsafe_allow_html=True,
    )

    funcionalidades_exibir = [
        ("editor_types",          "🧬 Editor Loot (types.xml)"),
        ("editor_globals",        "🌍 Editor Ambiente (globals.xml)"),
        ("editor_cfggameplay",    "⚙️ Editor Gameplay"),
        ("editor_events",         "📅 Editor Eventos"),
        ("editor_messages",       "💬 Editor Mensagens"),
        ("editor_cfgeventspawns", "📍 Editor Spawns"),
        ("ranking_semanal",       "🏆 Ranking Semanal"),
        ("transferencia_jogador", "🔁 Transferência DzCoins"),
    ]

    for chave, label in funcionalidades_exibir:
        permitido = plano_permite(plano_atual, chave)
        icone = "✅" if permitido else "🔒"
        cor_label = "#000000" if permitido else "#333333"
        st.markdown(
            f"""
            <div style="
                font-size:11px;
                color:{cor_label};
                padding:3px 0;
                border-bottom:1px solid #1e2535;
            ">
                {icone} {label}
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.divider()

    progresso = min(total_agendas / limite_agendas, 1.0) if limite_agendas > 0 else 0
    st.progress(progresso, text=f"Uso agendamentos: {total_agendas}/{limite_agendas}")

    st.divider()

    st.subheader("⚙️ Configurações FTP")

    if "ftp" not in client_data:
        client_data["ftp"] = {"host": "", "user": "", "pass": "", "port": "21"}

    client_data["ftp"]["host"] = st.text_input(
        "Host", value=client_data.get("ftp", {}).get("host", ""), key="f_host_main"
    )
    client_data["ftp"]["user"] = st.text_input(
        "Usuário", value=client_data.get("ftp", {}).get("user", ""), key="f_user_main"
    )
    client_data["ftp"]["pass"] = st.text_input(
        "Senha", type="password", value=client_data.get("ftp", {}).get("pass", ""), key="f_pass_main"
    )
    client_data["ftp"]["port"] = st.text_input(
        "Porta", value=client_data.get("ftp", {}).get("port", "21"), key="f_port_main"
    )

    col_f1, col_f2 = st.columns(2)
    if col_f1.button("💾 Salvar Dados", use_container_width=True, key="f_save_main"):
        save_db(DB_CLIENTS, st.session_state.db_clients)
        st.success("Salvo!")
        registrar_log(user_id, "Configurações FTP atualizadas.")

    if col_f2.button("⚡ Testar", use_container_width=True, key="f_test_main"):
        try:
            ftp_t = ftplib.FTP()
            ftp_t.connect(
                client_data["ftp"]["host"],
                int(client_data["ftp"]["port"]),
                timeout=10,
            )
            ftp_t.login(client_data["ftp"]["user"], client_data["ftp"]["pass"])
            ftp_t.quit()
            registrar_log(user_id, "Teste FTP: Sucesso", "sucesso")
            st.success("✅ Conexão OK!")
        except Exception as e:
            registrar_log(user_id, f"Teste FTP: Falha ({str(e)})", "erro")
            st.error("❌ Erro FTP")

    st.divider()

    if st.button("🚪 Sair do Sistema", use_container_width=True, key="logout_btn_final"):
        st.session_state.authenticated = False
        st.rerun()

    @st.fragment(run_every="1s")
    def sidebar_clock():
        st.metric(label="🕒 Brasília", value=get_hora_brasilia().strftime("%H:%M:%S"))

    sidebar_clock()

# --- TABS PRINCIPAIS CLIENTE ---
st.title(f"🎮 {user_info['server']}")

# Inclusão da "⚙️ Feeds / Bot" entre "🏦 Banco / Carteira" e "💎 Planos"
tab1, tab2, tab3, tab4, tab5, tabcfggameplay, tabevents, tabmessages, tabcfgeventspawns, tabraid, tab6, tab7, tab_analytics, tab_ranking, tab8, tab_feeds, tab_planos = st.tabs([
    "📅 Eventos Agendados", 
    "📋 Histórico Logs", 
    "📢 Comunicados", 
    "🧬 Loot / types.xml",
    "🌍 Ambiente / globals.xml", 
    "⚙️ Gameplay / cfggameplay.json", 
    "📅 Eventos / events.xml",
    "💬 Mensagens / messages.xml", 
    "📍 Spawns / cfgeventspawns.xml",
    "🛡️ Agenda de RAID",
    "🛒 Loja / Trader", 
    "👥 Jogadores", 
    "📊 Analytics",
    "🏆 Ranking",
    "🏦 Banco / Carteira", 
    "⚙️ Feeds / Bot",
    "💎 Planos",
])

with tab1:
    c1, c2 = st.columns([1, 1.5])

    with c1:
        st.subheader("🚀 Novo Evento")

        if total_agendas >= limite_agendas:
            st.error(f"Limite do plano atingido ({limite_agendas}).")
        else:
            upload_widget_key = f"uploader_agendamento_{user_id}"
            upload_session_key = f"agendamento_upload_{user_id}"

            up_file = st.file_uploader(
                "Arquivo",
                type=["xml", "json"],
                key=upload_widget_key,
            )

            if up_file is not None:
                file_bytes = up_file.getvalue()
                st.session_state[upload_session_key] = {
                    "name": up_file.name,
                    "bytes": file_bytes,
                    "b64": base64.b64encode(file_bytes).decode("utf-8"),
                }
                st.success(f"Arquivo carregado: {up_file.name}")

            arquivo_em_sessao = st.session_state.get(upload_session_key)

            if arquivo_em_sessao:
                st.info(f"Arquivo pronto para agendar: {arquivo_em_sessao['name']}")

            mapa = st.selectbox(
                "Mapa",
                ["Chernarus", "Livonia"],
                key=f"map_sel_main_{user_id}",
            )

            dt_ev = st.date_input(
                "Data",
                min_value=get_hora_brasilia().date(),
                key=f"date_sel_main_{user_id}",
            )

            h_in = st.text_input(
                "Entrada",
                "19:55",
                key=f"h_in_main_{user_id}",
            )

            h_out = st.text_input(
                "Saída",
                "21:55",
                key=f"h_out_main_{user_id}",
            )

            rec = st.selectbox(
                "Recorrência",
                ["Único", "Diário", "Semanal"],
                key=f"rec_sel_main_{user_id}",
            )

            if st.button(
                "Confirmar Agendamento",
                use_container_width=True,
                key=f"conf_btn_main_{user_id}",
            ):
                if arquivo_em_sessao:
                    safe_fn = f"{user_id[:5]}_{arquivo_em_sessao['name']}"
                    path = os.path.join(UPLOAD_DIR, safe_fn)

                    try:
                        with open(path, "wb") as f:
                            f.write(arquivo_em_sessao["bytes"])
                    except Exception as e:
                        st.error(f"Erro ao salvar arquivo localmente: {e}")
                        st.stop()

                    nova_agenda = {
                        "id": str(time.time()),
                        "file": arquivo_em_sessao["name"],
                        "localpath": path,
                        "filecontent": arquivo_em_sessao["b64"],
                        "mapa": mapa,
                        "path": "/dayzxb_missions/dayzOffline.chernarusplus/custom"
                        if mapa == "Chernarus"
                        else "/dayzxb_missions/dayzOffline.enoch/custom",
                        "data": dt_ev.strftime("%d/%m/%Y"),
                        "in": h_in,
                        "out": h_out,
                        "rec": rec,
                        "status": "Aguardando",
                    }

                    client_data["agendas"].append(nova_agenda)
                    save_db(DB_CLIENTS, st.session_state.db_clients)
                    registrar_log(
                        user_id,
                        f"Agendado: {arquivo_em_sessao['name']} ({mapa})",
                        "info",
                    )

                    st.session_state.pop(upload_session_key, None)

                    st.success("Evento agendado com sucesso!")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.warning("Selecione um arquivo antes de confirmar.")

    with c2:
        st.subheader("📋 Lista de Execução")
        agendas_lista = client_data.get("agendas", [])

        if not agendas_lista:
            st.info("Nenhum evento agendado.")
        else:
            for agenda in agendas_lista:
                status_atual = agenda.get("status", "Aguardando")
                cor = {
                    "Aguardando": "🔵",
                    "Ativo": "🟢",
                    "Finalizado": "⚪",
                }.get(status_atual, "🔴")

                titulo_expander = (
                    f"{cor} {agenda['file']} | 📅 {agenda['data']} | 🗺️ {agenda['mapa']}"
                )

                with st.expander(titulo_expander):
                    inf1, inf2 = st.columns(2)

                    with inf1:
                        st.write(f"**📄 Arquivo:** `{agenda['file']}`")
                        st.write(f"**🗺️ Mapa:** {agenda['mapa']}")
                        st.write(f"**🔄 Recorrência:** {agenda.get('rec', 'Único')}")

                    with inf2:
                        st.write(f"**⏰ Janela:** {agenda['in']} > {agenda['out']}")
                        st.write(f"**📌 Status:** {status_atual}")

                    st.divider()

                    if st.button(
                        "Remover Agendamento",
                        key=f"rem_main_{agenda['id']}",
                        use_container_width=True,
                        type="secondary",
                    ):
                        nome_arquivo = agenda["file"]
                        client_data["agendas"] = [
                            a for a in client_data["agendas"] if a["id"] != agenda["id"]
                        ]

                        save_db(DB_CLIENTS, st.session_state.db_clients)
                        registrar_log(
                            user_id,
                            f"Removido: {nome_arquivo}",
                            "info",
                        )

                        st.toast(f"Evento {nome_arquivo} removido!")
                        st.rerun()

with tab2:
    st.subheader("📜 Histórico de Atividades")
    db_fresco = load_db(DB_CLIENTS, {})
    logs_frescos = db_fresco.get(user_id, {}).get("logs", [])
    if not logs_frescos:
        st.info("Sem logs registrados.")
    else:
        if st.button("Limpar Histórico", key="clear_logs_btn"):
            db_fresco[user_id]["logs"] = []
            save_db(DB_CLIENTS, db_fresco)
            st.rerun()
        for log in logs_frescos:
            if "🔴" in log:
                st.error(log)
            elif "🟢" in log:
                st.success(log)
            elif "📡" in log:
                st.warning(log)
            else:
                st.info(log)

with tab3:
    st.subheader("📢 Comunicados Oficiais")
    comunicados = client_data.get("comunicados", [])

    if not comunicados:
        st.info("Nenhum comunicado disponível.")
    else:
        col_c, col_btn = st.columns([2.5, 1])
        with col_btn:
            if st.button(
                "🗑️ Limpar Histórico",
                use_container_width=True,
                help="Apaga permanentemente todas as mensagens desta conta",
            ):
                client_data["comunicados"] = []
                save_db(DB_CLIENTS, st.session_state.db_clients)
                st.toast("Histórico limpo com sucesso!")
                st.rerun()

        st.divider()

        for idx, m in enumerate(comunicados):
            with st.expander(f"📌 {m['titulo']} - {m['data']}"):
                st.write(m["mensagem"])

                st.divider()
                if st.button(
                    "Remover aviso", key=f"del_msg_{idx}", type="secondary"
                ):
                    client_data["comunicados"].pop(idx)
                    save_db(DB_CLIENTS, st.session_state.db_clients)
                    st.rerun()
                    
with tab4:
    if not plano_permite(plano_atual, "editor_types"):
        bloquear_funcionalidade(plano_atual, "🧬 Editor de Loot (types.xml)")
    else:
        st.subheader("⚙️ Editor de Loot (types.xml)")
        st.info("Você pode enviar o types.xml manualmente ou carregar direto do servidor via FTP.")
    
        mapa_types = st.selectbox(
            "Mapa do types.xml",
            ["Chernarus", "Livonia"],
            key=f"mapa_types_ftp_{user_id}"
        )
    
        colftp1, colftp2 = st.columns([1, 1])
    
        with colftp1:
            if st.button(
                "📥 Carregar types.xml do servidor via FTP",
                use_container_width=True,
                key=f"btn_load_types_ftp_{user_id}"
            ):
                ok, xml_bytes, msg = baixartypesviaftp(user_id, mapa_types)
    
                if ok:
                    try:
                        tree, root, df_types = parse_types_xml(xml_bytes)
    
                        st.session_state[f"types_xml_tree_{user_id}"] = tree
                        st.session_state[f"types_xml_root_{user_id}"] = root
                        st.session_state[f"types_xml_df_{user_id}"] = df_types
    
                        st.success(f"types.xml carregado do servidor com sucesso! ({len(df_types)} itens)")
                    except Exception as e:
                        st.error(f"Arquivo baixado, mas houve erro ao interpretar o XML: {e}")
                else:
                    st.error(f"Erro ao baixar types.xml via FTP: {msg}")
    
        with colftp2:
            up_types = st.file_uploader(
                "Enviar types.xml",
                type=["xml"],
                key="up_types_xml_client"
            )
    
        if up_types is not None:
            try:
                xml_bytes = up_types.read()
                tree, root, df_types = parse_types_xml(xml_bytes)
    
                st.session_state[f"types_xml_tree_{user_id}"] = tree
                st.session_state[f"types_xml_root_{user_id}"] = root
                st.session_state[f"types_xml_df_{user_id}"] = df_types
    
                st.success(f"Arquivo carregado: {up_types.name} ({len(df_types)} itens)")
            except Exception as e:
                st.error(f"Erro ao ler types.xml: {e}")
    
        # daqui para baixo continua o restante da lógica da tab4
        # que usa st.session_state[f"types_xml_df_{user_id}"]
    
        # 2) Se já temos algo carregado na sessão, mostra a interface
        key_df = f"types_xml_df_{user_id}"
        if key_df in st.session_state:
            df_types = st.session_state[key_df]
    
            st.markdown("### 🔍 Filtros rápidos")
    
            col_f1, col_f2, col_f3 = st.columns(3)
            with col_f1:
                categoria_sel = st.selectbox(
                    "Categoria",
                    options=["Todas"] + sorted(
                        [c for c in df_types["category"].dropna().unique().tolist()]
                    ),
                    index=0,
                )
            with col_f2:
                only_nom_zero = st.checkbox("Mostrar apenas itens sem spawn (nominal = 0)")
            with col_f3:
                nome_busca = st.text_input("Buscar por nome (contém)", "")
    
            df_view = df_types.copy()
    
            if categoria_sel != "Todas":
                df_view = df_view[df_view["category"] == categoria_sel]
    
            if only_nom_zero:
                df_view = df_view[df_view["nominal"] == 0]
    
            if nome_busca.strip():
                df_view = df_view[df_view["name"].str.contains(nome_busca.strip(), case=False)]
    
            st.markdown("### ✏️ Ajuste de parâmetros")
    
            edited_df = st.data_editor(
                df_view,
                num_rows="fixed",
                hide_index=True,
                column_config={
                    "name": "Classe",
                    "category": "Categoria",
                    "nominal": st.column_config.NumberColumn(
                        "Nominal",
                        help="Quantidade alvo do item no mapa.",
                        min_value=0,
                        step=1,
                    ),
                    "min": st.column_config.NumberColumn(
                        "Min",
                        help="Quantidade mínima a manter.",
                        min_value=0,
                        step=1,
                    ),
                    "lifetime": st.column_config.NumberColumn(
                        "Lifetime (s)",
                        help="Tempo, em segundos, que o item fica no mundo.",
                        min_value=0,
                        step=60,
                    ),
                },
                disabled=["name", "category"],
            )  # [web:67][web:61]
    
            st.markdown("### 💾 Salvar alterações no types.xml")
    
            col_s1, col_s2 = st.columns(2)
            with col_s1:
                if st.button("Aplicar alterações na sessão", use_container_width=True):
                    # Atualiza apenas as linhas filtradas, de volta no df original
                    df_merged = df_types.set_index("name")
                    edited_indexed = edited_df.set_index("name")
    
                    for idx in edited_indexed.index:
                        if idx in df_merged.index:
                            for col in ["nominal", "min", "lifetime"]:
                                df_merged.loc[idx, col] = edited_indexed.loc[idx, col]
    
                    st.session_state[key_df] = df_merged.reset_index()
                    st.success("Alterações aplicadas internamente (ainda não gerou novo XML).")
    
            with col_s2:
                if st.button("⬇️ Baixar types.xml ajustado", use_container_width=True):
                    tree = st.session_state.get(f"types_xml_tree_{user_id}")
                    root = st.session_state.get(f"types_xml_root_{user_id}")
                    df_full = st.session_state.get(key_df)
    
                    if tree is None or root is None or df_full is None:
                        st.error("Dados do XML não encontrados na sessão. Reenvie o arquivo.")
                    else:
                        new_xml_bytes = apply_df_to_types_xml(tree, root, df_full)
                        st.download_button(
                            label="Baixar types.xml",
                            data=new_xml_bytes,
                            file_name="types_editado.xml",
                            mime="application/xml",
                            use_container_width=True,
                        )
                        st.success("types.xml atualizado gerado com sucesso!")
    
            # --- AÇÃO EXTRA: Salvar no Titan Cloud + aplicar via FTP ---
            st.markdown("### 🚀 Salvar e aplicar no servidor DayZ")
    
            mapa_dest = st.selectbox(
                "Mapa de destino (onde o types.xml será aplicado)",
                ["Chernarus", "Livonia"],
                key="types_mapa_dest",
            )
    
            if st.button("Salvar no Titan Cloud e enviar via FTP", use_container_width=True):
                tree = st.session_state.get(f"types_xml_tree_{user_id}")
                root = st.session_state.get(f"types_xml_root_{user_id}")
                df_full = st.session_state.get(key_df)
    
                if tree is None or root is None or df_full is None:
                    st.error("Dados do XML não encontrados na sessão. Reenvie o arquivo.")
                else:
                    # 1) Gerar XML em memória
                    new_xml_bytes = apply_df_to_types_xml(tree, root, df_full)
    
                    # 2) Salvar em disco persistente do Titan Cloud
                    safe_name = f"{user_id[:5]}_types_{mapa_dest.lower()}.xml"
                    local_types_path = os.path.join(UPLOAD_DIR, safe_name)
                    try:
                        with open(local_types_path, "wb") as f:
                            f.write(new_xml_bytes)
    
                        # 3) Enviar via FTP para o servidor, no caminho correto
                        ok, msg = enviar_types_via_ftp(user_id, local_types_path, mapa_dest)
    
                        if ok:
                            registrar_log(
                                user_id,
                                f"types.xml atualizado e enviado via FTP para {mapa_dest}.",
                                "sucesso",
                            )
                            st.success("types.xml enviado e aplicado via FTP com sucesso!")
                        else:
                            registrar_log(
                                user_id,
                                f"Falha ao enviar types.xml via FTP ({msg})",
                                "erro",
                            )
                            st.error(f"Erro ao enviar via FTP: {msg}")
                    except Exception as e:
                        registrar_log(
                            user_id,
                            f"Erro ao salvar/enviar types.xml ({str(e)})",
                            "erro",
                        )
                        st.error(f"Erro ao salvar/enviar types.xml: {e}")
    
            st.divider()
            st.markdown("#### ℹ️ Dicas rápidas")
            st.write(
                "- Nominal define a **quantidade alvo** de cada item no mapa; "
                "valores muito altos criam excesso de loot, muito baixos deixam o servidor vazio."
            )  # [web:61][web:65]
            st.write(
                "- Lifetime é o tempo em segundos antes do item ser limpo; "
                "itens de base costumam ter lifetime mais alto que loot comum."
            )  # [web:65]
    
with tab5:
    if not plano_permite(plano_atual, "editor_globals"):
        bloquear_funcionalidade(plano_atual, "🌍 Editor de Ambiente (globals.xml)")
    else:
        st.subheader("🌍 Ambiente / globals.xml")
        st.info("Você pode enviar o globals.xml manualmente ou carregar direto do servidor via FTP.")
    
        mapa_globals = st.selectbox(
            "Mapa do globals.xml",
            ["Chernarus", "Livonia"],
            key=f"mapa_globals_ftp_{user_id}"
        )
    
        colg1, colg2 = st.columns([1, 1])
    
        with colg1:
            if st.button(
                "📥 Carregar globals.xml do servidor via FTP",
                use_container_width=True,
                key=f"btn_load_globals_ftp_{user_id}"
            ):
                ok, xml_bytes, msg = baixarglobalsviaftp(user_id, mapa_globals)
    
                if ok:
                    try:
                        tree, root, vars_dict = parse_globals_xml(xml_bytes)
                        st.session_state[f"globals_tree_{user_id}"] = tree
                        st.session_state[f"globals_root_{user_id}"] = root
                        st.session_state[f"globals_vars_{user_id}"] = vars_dict
                        st.success(f"globals.xml carregado do servidor com sucesso! ({len(vars_dict)} variáveis)")
                    except Exception as e:
                        st.error(f"Arquivo baixado, mas houve erro ao interpretar o XML: {e}")
                else:
                    st.error(f"Erro ao baixar globals.xml via FTP: {msg}")
    
        with colg2:
            up_globals = st.file_uploader(
                "Enviar globals.xml",
                type=["xml"],
                key=f"up_globalsxml_{user_id}"
            )
    
        if up_globals is not None:
            try:
                xml_bytes = up_globals.read()
                tree, root, vars_dict = parse_globals_xml(xml_bytes)
    
                st.session_state[f"globals_tree_{user_id}"] = tree
                st.session_state[f"globals_root_{user_id}"] = root
                st.session_state[f"globals_vars_{user_id}"] = vars_dict
    
                st.success(f"Arquivo carregado: {up_globals.name} ({len(vars_dict)} variáveis)")
            except Exception as e:
                st.error(f"Erro ao ler globals.xml: {e}")
    
        # daqui para baixo continua o restante da lógica da tab5
    
        key_gvars = f"globals_vars_{user_id}"
        if key_gvars in st.session_state:
            g_vars = st.session_state[key_gvars]
    
            st.markdown("### 🧩 Parâmetros principais")
    
            def get_val(name, default):
                info = g_vars.get(name, None)
                if info is None:
                    return default
                return info.get("value", default)
    
            col1, col2 = st.columns(2)
            with col1:
                animal_max = st.slider("AnimalMaxCount (máx. animais no mapa)", 0, 1000, int(get_val("AnimalMaxCount", 200)), 10)
                zombie_max = st.slider("ZombieMaxCount (máx. zumbis no mapa)", 0, 5000, int(get_val("ZombieMaxCount", 1000)), 50)
                cleanup_dead = st.slider("CleanupLifetimeDeadPlayer (limpeza corpo jogador, seg.)", 300, 21600, int(get_val("CleanupLifetimeDeadPlayer", 3600)), 300)
            with col2:
                idle_mode = st.slider("IdleModeCountdown (seg. até idle em servidor vazio)", 0, 86400, int(get_val("IdleModeCountdown", 60)), 60)
                time_login = st.slider("TimeLogin (tempo de login, seg.)", 5, 120, int(get_val("TimeLogin", 15)), 1)
                time_logout = st.slider("TimeLogout (tempo de logout, seg.)", 5, 120, int(get_val("TimeLogout", 15)), 1)
    
            st.markdown("### 🧹 Limpeza e ambiente")
            col3, col4 = st.columns(2)
            with col3:
                cleanup_animal = st.slider("CleanupLifetimeDeadAnimal (corpo animal, seg.)", 60, 7200, int(get_val("CleanupLifetimeDeadAnimal", 1200)), 60)
                cleanup_infected = st.slider("CleanupLifetimeDeadInfected (corpo zumbi, seg.)", 60, 3600, int(get_val("CleanupLifetimeDeadInfected", 330)), 30)
                cleanup_default = st.slider("CleanupLifetimeDefault (limpeza padrão, seg.)", 10, 300, int(get_val("CleanupLifetimeDefault", 45)), 5)
                cleanup_ruined = st.slider("CleanupLifetimeRuined (item destruído, seg.)", 60, 3600, int(get_val("CleanupLifetimeRuined", 330)), 30)
            with col4:
                cleanup_avoidance = st.slider("CleanupAvoidance (distância evitar limpeza, m)", 0, 500, int(get_val("CleanupAvoidance", 100)), 10)
                cleanup_limit = st.slider("CleanupLifetimeLimit (limite limpeza)", 10, 200, int(get_val("CleanupLifetimeLimit", 50)), 5)
                food_decay = st.slider("FoodDecay (deterioração de comida: 0=off, 1=on)", 0, 1, int(get_val("FoodDecay", 1)), 1)
                world_wet = st.slider("WorldWetTempUpdate (atualização temperatura/molhado: 0=off, 1=on)", 0, 1, int(get_val("WorldWetTempUpdate", 1)), 1)
    
            st.markdown("### 🎯 Spawn e loot")
            col5, col6 = st.columns(2)
            with col5:
                initial_spawn = st.slider("InitialSpawn (% spawn inicial de loot)", 0, 100, int(get_val("InitialSpawn", 100)), 5)
                spawn_initial = st.slider("SpawnInitial (tempo inicial spawn CE, seg.)", 0, 3600, int(get_val("SpawnInitial", 1200)), 60)
                respawn_attempt = st.slider("RespawnAttempt (tentativas de respawn CE)", 1, 20, int(get_val("RespawnAttempt", 2)), 1)
                respawn_limit = st.slider("RespawnLimit (limite de respawn CE)", 1, 100, int(get_val("RespawnLimit", 20)), 1)
                respawn_types = st.slider("RespawnTypes (tipos de respawn CE)", 1, 50, int(get_val("RespawnTypes", 12)), 1)
            with col6:
                restart_spawn = st.slider("RestartSpawn (respawn no restart: 0=off, 1=on)", 0, 1, int(get_val("RestartSpawn", 0)), 1)
                loot_proxy = st.slider("LootProxyPlacement (loot em proxies: 0=off, 1=on)", 0, 1, int(get_val("LootProxyPlacement", 1)), 1)
                loot_spawn_avoidance = st.slider("LootSpawnAvoidance (distância evitar loot, m)", 0, 500, int(get_val("LootSpawnAvoidance", 100)), 10)
                loot_dmg_min = st.slider("LootDamageMin (dano mín. loot ao spawnar)", 0.0, 1.0, float(get_val("LootDamageMin", 0.0)), 0.01)
                loot_dmg_max = st.slider("LootDamageMax (dano máx. loot ao spawnar)", 0.0, 1.0, float(get_val("LootDamageMax", 0.82)), 0.01)
    
            st.markdown("### ⏱️ Tempo e penalidades")
            col7, col8 = st.columns(2)
            with col7:
                time_hopping = st.slider("TimeHopping (penalidade server hop, seg.)", 0, 600, int(get_val("TimeHopping", 60)), 10)
                time_penalty = st.slider("TimePenalty (tempo de penalidade geral, seg.)", 0, 300, int(get_val("TimePenalty", 20)), 5)
                zone_spawn_dist = st.slider("ZoneSpawnDist (distância zona de spawn, m)", 0, 1000, int(get_val("ZoneSpawnDist", 300)), 10)
            with col8:
                flag_refresh_freq = st.slider("FlagRefreshFrequency (frequência refresh bandeira, seg.)", 3600, 864000, int(get_val("FlagRefreshFrequency", 432000)), 3600)
                flag_refresh_max = st.slider("FlagRefreshMaxDuration (duração máx. bandeira, seg.)", 3600, 8640000, int(get_val("FlagRefreshMaxDuration", 3456000)), 3600)
                idle_startup = st.slider("IdleModeStartup (iniciar em idle: 0=off, 1=on)", 0, 1, int(get_val("IdleModeStartup", 1)), 1)
    
            st.markdown("### 📝 Resumo do ambiente")
    
            idle_min = idle_mode // 60
            cleanup_player_min = cleanup_dead // 60
            cleanup_animal_min = cleanup_animal // 60
            cleanup_infected_min = cleanup_infected // 60
            cleanup_ruined_min = cleanup_ruined // 60
            flag_refresh_dias = round(flag_refresh_freq / 86400, 1)
            flag_refresh_max_dias = round(flag_refresh_max / 86400, 1)
    
            st.write(f"- Máx. **{zombie_max}** zumbis e **{animal_max}** animais no mapa.")
            st.write(f"- Corpos de jogadores somem em ~**{cleanup_player_min} min** | animais em ~**{cleanup_animal_min} min** | zumbis em ~**{cleanup_infected_min} min** | itens destruídos em ~**{cleanup_ruined_min} min**.")
            st.write(f"- Servidor entra em idle após **{idle_min} min** sem jogadores. Iniciar em idle: **{'Sim' if idle_startup else 'Não'}**.")
            st.write(f"- Tempo de login: **{time_login} s** | logout: **{time_logout} s** | penalidade: **{time_penalty} s** | server hop: **{time_hopping} s**.")
            st.write(f"- Loot spawna com dano entre **{loot_dmg_min:.2f}** e **{loot_dmg_max:.2f}** | Loot em proxies: **{'Sim' if loot_proxy else 'Não'}** | Avoidance: **{loot_spawn_avoidance} m**.")
            st.write(f"- Spawn inicial de loot: **{initial_spawn}%** | Tempo CE inicial: **{spawn_initial} s** | Respawn: **{respawn_attempt}** tentativas, limite **{respawn_limit}**, tipos **{respawn_types}**.")
            st.write(f"- Deterioração de comida: **{'Ativada' if food_decay else 'Desativada'}** | Temperatura/molhado: **{'Ativado' if world_wet else 'Desativado'}**.")
            st.write(f"- Bandeira de território: refresh a cada **{flag_refresh_dias} dias**, duração máx. **{flag_refresh_max_dias} dias**.")
            st.write(f"- Zona de spawn: **{zone_spawn_dist} m** | Avoidance de limpeza: **{cleanup_avoidance} m** | Limite de limpeza: **{cleanup_limit}**.")
    
            st.markdown("### 💾 Salvar alterações no globals.xml")
    
            if st.button("Aplicar alterações na sessão (globals.xml)", use_container_width=True):
                # Atualiza o dicionário g_vars em memória
                def set_val(name, value):
                    if name not in g_vars:
                        g_vars[name] = {"type": "0", "value": value, "elem": None}
                    else:
                        g_vars[name]["value"] = value
    
                set_val("AnimalMaxCount", animal_max)
                set_val("ZombieMaxCount", zombie_max)
                set_val("CleanupLifetimeDeadPlayer", cleanup_dead)
                set_val("IdleModeCountdown", idle_mode)
                set_val("TimeLogin", time_login)
                set_val("TimeLogout", time_logout)
                set_val("CleanupLifetimeDeadAnimal", cleanup_animal)
                set_val("CleanupLifetimeDeadInfected", cleanup_infected)
                set_val("CleanupLifetimeDefault", cleanup_default)
                set_val("CleanupLifetimeRuined", cleanup_ruined)
                set_val("CleanupAvoidance", cleanup_avoidance)
                set_val("CleanupLifetimeLimit", cleanup_limit)
                set_val("FoodDecay", food_decay)
                set_val("WorldWetTempUpdate", world_wet)
                set_val("InitialSpawn", initial_spawn)
                set_val("SpawnInitial", spawn_initial)
                set_val("RespawnAttempt", respawn_attempt)
                set_val("RespawnLimit", respawn_limit)
                set_val("RespawnTypes", respawn_types)
                set_val("RestartSpawn", restart_spawn)
                set_val("LootProxyPlacement", loot_proxy)
                set_val("LootSpawnAvoidance", loot_spawn_avoidance)
                set_val("LootDamageMin", loot_dmg_min)
                set_val("LootDamageMax", loot_dmg_max)
                set_val("TimeHopping", time_hopping)
                set_val("TimePenalty", time_penalty)
                set_val("ZoneSpawnDist", zone_spawn_dist)
                set_val("FlagRefreshFrequency", flag_refresh_freq)
                set_val("FlagRefreshMaxDuration", flag_refresh_max)
                set_val("IdleModeStartup", idle_startup)
    
                st.session_state[key_gvars] = g_vars
                st.success("Alterações aplicadas internamente ao globals.xml (sessão).")
    
            if st.button("⬇️ Baixar globals.xml ajustado", use_container_width=True):
                g_tree = st.session_state.get(f"globals_tree_{user_id}")
                g_root = st.session_state.get(f"globals_root_{user_id}")
                g_vars_full = st.session_state.get(key_gvars)
    
                if g_tree is None or g_root is None or g_vars_full is None:
                    st.error("Dados do globals.xml não encontrados na sessão. Reenvie o arquivo.")
                else:
                    new_globals_bytes = apply_globals_changes(g_tree, g_root, g_vars_full)
                    st.download_button(
                        label="Baixar globals.xml",
                        data=new_globals_bytes,
                        file_name="globals_editado.xml",
                        mime="application/xml",
                        use_container_width=True,
                    )
                    st.success("globals.xml atualizado gerado com sucesso!")
    
            st.markdown("### 🚀 Salvar e aplicar no servidor DayZ")
    
            mapa_globals = st.selectbox(
                "Mapa de destino (onde o globals.xml será aplicado)",
                ["Chernarus", "Livonia"],
                key="globals_mapa_dest",
            )
    
            if st.button("Salvar no Titan Cloud e enviar via FTP (globals)", use_container_width=True):
                g_tree = st.session_state.get(f"globals_tree_{user_id}")
                g_root = st.session_state.get(f"globals_root_{user_id}")
                g_vars_full = st.session_state.get(key_gvars)
    
                if g_tree is None or g_root is None or g_vars_full is None:
                    st.error("Dados do globals.xml não encontrados na sessão. Reenvie o arquivo.")
                else:
                    # 1) Gerar XML em memória com as alterações
                    new_globals_bytes = apply_globals_changes(g_tree, g_root, g_vars_full)
    
                    # 2) Salvar em disco persistente do Titan Cloud
                    safe_name_g = f"{user_id[:5]}_globals_{mapa_globals.lower()}.xml"
                    local_globals_path = os.path.join(UPLOAD_DIR, safe_name_g)
    
                    try:
                        with open(local_globals_path, "wb") as f:
                            f.write(new_globals_bytes)
    
                        # 3) Enviar via FTP para o servidor, no caminho correto
                        ok_g, msg_g = enviar_globals_via_ftp(user_id, local_globals_path, mapa_globals)
    
                        if ok_g:
                            registrar_log(
                                user_id,
                                f"globals.xml atualizado e enviado via FTP para {mapa_globals}.",
                                "sucesso",
                            )
                            st.success("globals.xml enviado e aplicado via FTP com sucesso!")
                        else:
                            registrar_log(
                                user_id,
                                f"Falha ao enviar globals.xml via FTP ({msg_g})",
                                "erro",
                            )
                            st.error(f"Erro ao enviar via FTP: {msg_g}")
                    except Exception as e:
                        registrar_log(
                            user_id,
                            f"Erro ao salvar/enviar globals.xml ({str(e)})",
                            "erro",
                        )
                        st.error(f"Erro ao salvar/enviar globals.xml: {e}")
                        
with tabcfggameplay:
    if not plano_permite(plano_atual, "editor_cfggameplay"):
        bloquear_funcionalidade(plano_atual, "⚙️ Editor de Gameplay (cfggameplay.json)")
    else:
        st.subheader("⚙️ Gameplay / cfggameplay.json")
        # ... resto dentro do else
        st.info("Você pode enviar o cfggameplay.json manualmente ou carregar direto do servidor via FTP.")
    
        # chave única de sessão para este usuário
        cfg_session_key = f"cfggameplay_data_{user_id}"
    
        mapa_cfggameplay = st.selectbox(
            "Mapa do cfggameplay.json",
            ["Chernarus", "Livonia"],
            key=f"mapa_cfggameplay_ftp_{user_id}"
        )
    
        colcg1, colcg2 = st.columns([1, 1])
    
        # 1) Carregar do servidor via FTP
        with colcg1:
            if st.button(
                "📥 Carregar cfggameplay.json do servidor via FTP",
                use_container_width=True,
                key=f"btn_load_cfggameplay_ftp_{user_id}"
            ):
                ok, json_bytes, msg = baixarcfggameplayviaftp(user_id, mapa_cfggameplay)
    
                if ok:
                    try:
                        cfg_data = json.loads(json_bytes.decode("utf-8"))
                        st.session_state[cfg_session_key] = cfg_data
                        st.success("cfggameplay.json carregado do servidor com sucesso!")
                    except Exception as e:
                        st.error(f"Arquivo baixado, mas houve erro ao interpretar o JSON: {e}")
                else:
                    st.error(f"Erro ao baixar cfggameplay.json via FTP: {msg}")
    
        # 2) Upload manual do arquivo
        with colcg2:
            up_cfggameplay = st.file_uploader(
                "Enviar cfggameplay.json",
                type=["json"],
                key=f"up_cfggameplay_{user_id}"
            )
    
        if up_cfggameplay is not None:
            try:
                json_bytes = up_cfggameplay.read()
                cfg_data = json.loads(json_bytes.decode("utf-8"))
                st.session_state[cfg_session_key] = cfg_data
                st.success("cfggameplay.json carregado com sucesso!")
            except Exception as e:
                st.error(f"Erro ao ler cfggameplay.json: {e}")
    
        # daqui para baixo continua o restante da lógica da aba
    
        # 2) Se já temos cfg em sessão, mostra a interface
        if cfg_session_key in st.session_state:
            cfg = st.session_state[cfg_session_key]
    
            # Garante estruturas principais
            general = cfg.get("GeneralData", {})
            player = cfg.get("PlayerData", {})
            stamina = player.get("StaminaData", {})
            shock = player.get("ShockHandlingData", {})
            movement = player.get("MovementData", {})
            worlds = cfg.get("WorldsData", {})
            map_data = cfg.get("MapData", {})
            ui_data = cfg.get("UIData", {})
            vehicle_data = cfg.get("VehicleData", {})
    
            # -------------------------------
            # Geral
            # -------------------------------
            st.markdown("### ⚙️ Geral")
    
            col_g1, col_g2 = st.columns(2)
            with col_g1:
                disable_base_damage = st.checkbox(
                    "Desativar dano em bases (disableBaseDamage)",
                    value=general.get("disableBaseDamage", False),
                )
                disable_container_damage = st.checkbox(
                    "Desativar dano em containers (disableContainerDamage)",
                    value=general.get("disableContainerDamage", False),
                )
            with col_g2:
                disable_respawn_dialog = st.checkbox(
                    "Desativar tela de respawn (disableRespawnDialog)",
                    value=general.get("disableRespawnDialog", False),
                )
                disable_respawn_unconscious = st.checkbox(
                    "Bloquear respawn inconsciente (disableRespawnInUnconsciousness)",
                    value=general.get("disableRespawnInUnconsciousness", False),
                )
    
            # -------------------------------
            # Stamina
            # -------------------------------
            st.markdown("### 💪 Jogador - Stamina")
    
            col_s1, col_s2 = st.columns(2)
            with col_s1:
                stamina_max = st.number_input(
                    "Stamina máxima (staminaMax)",
                    min_value=10.0,
                    max_value=50000.0,
                    step=5.0,
                    value=float(stamina.get("staminaMax", 100.0)),
                )
                stamina_min_cap = st.number_input(
                    "Stamina mínima (staminaMinCap)",
                    min_value=0.0,
                    max_value=50.0,
                    step=1.0,
                    value=float(stamina.get("staminaMinCap", 5.0)),
                )
                stamina_weight_threshold = st.number_input(
                    "Peso limite stamina (staminaWeightLimitThreshold)",
                    min_value=0.0,
                    max_value=20000.0,
                    step=100.0,
                    value=float(stamina.get("staminaWeightLimitThreshold", 6000.0)),
                )
            with col_s2:
                stamina_penalty = st.number_input("Penalidade kg → % (staminaKgToStaminaPercentPenalty)", min_value=0.0, max_value=10.0, step=0.05, value=float(stamina.get("staminaKgToStaminaPercentPenalty", 1.75)))
                sprint_sta_mod_erc = st.number_input("Sprint em pé (sprintStaminaModifierErc)", min_value=0.1, max_value=5.0, step=0.1, value=float(stamina.get("sprintStaminaModifierErc", 1.0)))
                sprint_sta_mod_cro = st.number_input("Sprint abaixado (sprintStaminaModifierCro)", min_value=0.1, max_value=5.0, step=0.1, value=float(stamina.get("sprintStaminaModifierCro", 1.0)))
    
            col_s3, col_s4 = st.columns(2)
            with col_s3:
                sprint_swim_mod = st.number_input("Sprint nadando (sprintSwimmingStaminaModifier)", min_value=0.1, max_value=5.0, step=0.1, value=float(stamina.get("sprintSwimmingStaminaModifier", 1.0)))
                sprint_ladder_mod = st.number_input("Sprint em escada (sprintLadderStaminaModifier)", min_value=0.1, max_value=5.0, step=0.1, value=float(stamina.get("sprintLadderStaminaModifier", 1.0)))
                melee_sta_mod = st.number_input("Stamina corpo a corpo (meleeStaminaModifier)", min_value=0.1, max_value=5.0, step=0.1, value=float(stamina.get("meleeStaminaModifier", 1.0)))
            with col_s4:
                obstacle_sta_mod = st.number_input("Stamina obstáculos (obstacleTraversalStaminaModifier)", min_value=0.1, max_value=5.0, step=0.1, value=float(stamina.get("obstacleTraversalStaminaModifier", 1.0)))
                hold_breath_mod = st.number_input("Segurar respiração (holdBreathStaminaModifier)", min_value=0.1, max_value=5.0, step=0.1, value=float(stamina.get("holdBreathStaminaModifier", 1.0)))
            
            disable_personal_light = st.checkbox("Desativar luz pessoal (disablePersonalLight)", value=player.get("disablePersonalLight", False))
    
            # -------------------------------
            # Shock / Movimento
            # -------------------------------
            st.markdown("### 🧠 Jogador - Shock e Movimento")
    
            col_sh1, col_sh2 = st.columns(2)
            with col_sh1:
                shock_refill_con = st.number_input(
                    "Refill choque consciente (shockRefillSpeedConscious)",
                    min_value=0.1,
                    max_value=50.0,
                    step=0.5,
                    value=float(shock.get("shockRefillSpeedConscious", 5.0)),
                )
                shock_refill_uncon = st.number_input(
                    "Refill choque inconsciente (shockRefillSpeedUnconscious)",
                    min_value=0.1,
                    max_value=50.0,
                    step=0.5,
                    value=float(shock.get("shockRefillSpeedUnconscious", 1.0)),
                )
            with col_sh2:
                allow_refill_mod = st.checkbox(
                    "Permitir modificador de refill (allowRefillSpeedModifier)",
                    value=shock.get("allowRefillSpeedModifier", True),
                )
    
            col_m1, col_m2 = st.columns(2)
            with col_m1:
                time_to_sprint = st.number_input("Tempo para sprint (timeToSprint)", min_value=0.0, max_value=5.0, step=0.05, value=float(movement.get("timeToSprint", 0.45)))
                time_to_strafe_jog = st.number_input("Tempo para strafe correndo (timeToStrafeJog)", min_value=0.0, max_value=5.0, step=0.05, value=float(movement.get("timeToStrafeJog", 0.1)))
                time_to_strafe_sprint = st.number_input("Tempo para strafe sprint (timeToStrafeSprint)", min_value=0.0, max_value=5.0, step=0.05, value=float(movement.get("timeToStrafeSprint", 0.3)))
            with col_m2:
                rot_speed_jog = st.number_input("Rotação correndo (rotationSpeedJog)", min_value=0.0, max_value=2.0, step=0.05, value=float(movement.get("rotationSpeedJog", 0.3)))
                rot_speed_sprint = st.number_input("Rotação sprint (rotationSpeedSprint)", min_value=0.0, max_value=2.0, step=0.05, value=float(movement.get("rotationSpeedSprint", 0.15)))
                allow_sta_inertia = st.checkbox("Stamina afeta inércia (allowStaminaAffectInertia)", value=movement.get("allowStaminaAffectInertia", True))
    
            # -------------------------------
            # Drowning
            # -------------------------------
            st.markdown("### 🌊 Afogamento")
            drowning = player.get("DrowningData", {})
            col_d1, col_d2, col_d3 = st.columns(3)
            with col_d1:
                drown_stamina = st.number_input("Depleção stamina (staminaDepletionSpeed)", min_value=0.0, max_value=100.0, step=1.0, value=float(drowning.get("staminaDepletionSpeed", 10.0)))
            with col_d2:
                drown_health = st.number_input("Depleção vida (healthDepletionSpeed)", min_value=0.0, max_value=100.0, step=1.0, value=float(drowning.get("healthDepletionSpeed", 10.0)))
            with col_d3:
                drown_shock = st.number_input("Depleção choque (shockDepletionSpeed)", min_value=0.0, max_value=100.0, step=1.0, value=float(drowning.get("shockDepletionSpeed", 10.0)))
    
            # -------------------------------
            # Weapon Obstruction
            # -------------------------------
            st.markdown("### 🔫 Obstrução de Armas")
            weapon_obs = player.get("WeaponObstructionData", {})
            col_w1, col_w2 = st.columns(2)
            with col_w1:
                weapon_static_mode = st.selectbox("Modo estático (staticMode)", options=[0, 1], index=int(weapon_obs.get("staticMode", 1)), help="0 = desativado, 1 = ativado")
            with col_w2:
                weapon_dynamic_mode = st.selectbox("Modo dinâmico (dynamicMode)", options=[0, 1], index=int(weapon_obs.get("dynamicMode", 1)), help="0 = desativado, 1 = ativado")
    
            # -------------------------------
            # Base Building
            # -------------------------------
            st.markdown("### 🏗️ Construção de Bases")
            base = cfg.get("BaseBuildingData", {})
            hologram = base.get("HologramData", {})
            construction = base.get("ConstructionData", {})
    
            st.markdown("**Hologram (verificações ao posicionar)**")
            col_h1, col_h2 = st.columns(2)
            with col_h1:
                dis_bbox = st.checkbox("Desativar colisão BBox (disableIsCollidingBBoxCheck)", value=hologram.get("disableIsCollidingBBoxCheck", False))
                dis_player = st.checkbox("Desativar colisão jogador (disableIsCollidingPlayerCheck)", value=hologram.get("disableIsCollidingPlayerCheck", False))
                dis_roof = st.checkbox("Desativar verificação teto (disableIsClippingRoofCheck)", value=hologram.get("disableIsClippingRoofCheck", False))
                dis_base_viable = st.checkbox("Desativar base viável (disableIsBaseViableCheck)", value=hologram.get("disableIsBaseViableCheck", False))
                dis_gplot = st.checkbox("Desativar colisão GPlot (disableIsCollidingGPlotCheck)", value=hologram.get("disableIsCollidingGPlotCheck", False))
                dis_angle = st.checkbox("Desativar verificação ângulo (disableIsCollidingAngleCheck)", value=hologram.get("disableIsCollidingAngleCheck", False))
            with col_h2:
                dis_placement = st.checkbox("Desativar permissão de colocação (disableIsPlacementPermittedCheck)", value=hologram.get("disableIsPlacementPermittedCheck", False))
                dis_height = st.checkbox("Desativar verificação altura (disableHeightPlacementCheck)", value=hologram.get("disableHeightPlacementCheck", False))
                dis_underwater = st.checkbox("Desativar verificação subaquática (disableIsUnderwaterCheck)", value=hologram.get("disableIsUnderwaterCheck", False))
                dis_terrain = st.checkbox("Desativar verificação terreno (disableIsInTerrainCheck)", value=hologram.get("disableIsInTerrainCheck", False))
                dis_cold = st.checkbox("Desativar verificação área fria (disableColdAreaBuildingCheck)", value=hologram.get("disableColdAreaBuildingCheck", False))
    
            st.markdown("**Construção**")
            col_c1, col_c2, col_c3 = st.columns(3)
            with col_c1:
                dis_roof_check = st.checkbox("Desativar check teto (disablePerformRoofCheck)", value=construction.get("disablePerformRoofCheck", False))
            with col_c2:
                dis_colliding_check = st.checkbox("Desativar check colisão (disableIsCollidingCheck)", value=construction.get("disableIsCollidingCheck", False))
            with col_c3:
                dis_distance_check = st.checkbox("Desativar check distância (disableDistanceCheck)", value=construction.get("disableDistanceCheck", False))
    
            # -------------------------------
            # Mundo / Clima
            # -------------------------------
            st.markdown("### 🌍 Mundo / Clima")
    
            lighting_config = st.selectbox(
                "Preset de iluminação (lightingConfig)",
                options=[0, 1, 2],
                index=[0, 1, 2].index(int(worlds.get("lightingConfig", 0))),
                help="Controla presets de iluminação do servidor.",
            )
    
            st.info(
                "As listas environmentMinTemps e environmentMaxTemps definem temperaturas por mês. "
                "Manteremos edição avançada para uma próxima versão."
            )
    
            # -------------------------------
            # Mapa / UI
            # -------------------------------
            st.markdown("### 🗺️ Mapa e Interface")
    
            col_map1, col_map2 = st.columns(2)
            with col_map1:
                display_player_pos = st.checkbox(
                    "Mostrar posição do jogador no mapa (displayPlayerPosition)",
                    value=map_data.get("displayPlayerPosition", False),
                )
                display_nav_info = st.checkbox(
                    "Mostrar infos de navegação (displayNavInfo)",
                    value=map_data.get("displayNavInfo", True),
                )
            with col_map2:
                use_3d_map = st.checkbox("Usar mapa 3D (use3DMap)", value=ui_data.get("use3DMap", False))
                ignore_map_ownership = st.checkbox("Ignorar posse do mapa (ignoreMapOwnership)", value=map_data.get("ignoreMapOwnership", False))
                ignore_nav_ownership = st.checkbox("Ignorar posse de nav items (ignoreNavItemsOwnership)", value=map_data.get("ignoreNavItemsOwnership", False))
    
            st.markdown("**Indicação de Hit**")
            hit = ui_data.get("HitIndicationData", {})
            col_hi1, col_hi2 = st.columns(2)
            with col_hi1:
                hit_dir_override = st.checkbox("Override direção hit (hitDirectionOverrideEnabled)", value=hit.get("hitDirectionOverrideEnabled", False))
                hit_dir_behaviour = st.selectbox("Comportamento direção hit (hitDirectionBehaviour)", options=[0, 1, 2], index=min(int(hit.get("hitDirectionBehaviour", 1)), 2))
                hit_dir_style = st.selectbox("Estilo indicador hit (hitDirectionStyle)", options=[0, 1, 2], index=min(int(hit.get("hitDirectionStyle", 0)), 2))
                hit_post_process = st.checkbox("Post process indicação hit (hitIndicationPostProcessEnabled)", value=hit.get("hitIndicationPostProcessEnabled", True))
            with col_hi2:
                hit_max_duration = st.number_input("Duração máx. indicador (hitDirectionMaxDuration, seg.)", min_value=0.1, max_value=10.0, step=0.1, value=float(hit.get("hitDirectionMaxDuration", 2.0)))
                hit_breakpoint = st.number_input("Breakpoint relativo (hitDirectionBreakPointRelative)", min_value=0.0, max_value=1.0, step=0.05, value=float(hit.get("hitDirectionBreakPointRelative", 0.2)))
                hit_scatter = st.number_input("Dispersão indicador (hitDirectionScatter)", min_value=0.0, max_value=90.0, step=1.0, value=float(hit.get("hitDirectionScatter", 10.0)))
                hit_color = st.text_input("Cor indicador hex (hitDirectionIndicatorColorStr)", value=hit.get("hitDirectionIndicatorColorStr", "0xffbb0a1e"))
    
            # -------------------------------
            # Veículos
            # -------------------------------
            st.markdown("### 🚗 Veículos")
    
            boat_decay_multiplier = st.number_input(
                "Multiplicador de decay de barcos (boatDecayMultiplier)",
                min_value=0.0,
                max_value=10.0,
                step=0.1,
                value=float(vehicle_data.get("boatDecayMultiplier", 1)),
            )
    
            st.markdown("### 💾 Gerar cfggameplay.json ajustado")
    
            col_save1, col_save2 = st.columns(2)
    
            # 3) Aplicar alterações na sessão (atualiza cfg em memória)
            with col_save1:
                if st.button(
                    "Aplicar alterações na sessão (cfggameplay)",
                    use_container_width=True,
                ):
                    # General
                    cfg["GeneralData"] = {
                        **general,
                        "disableBaseDamage": disable_base_damage,
                        "disableContainerDamage": disable_container_damage,
                        "disableRespawnDialog": disable_respawn_dialog,
                        "disableRespawnInUnconsciousness": disable_respawn_unconscious,
                    }
    
                    # Stamina
                    stamina.update({
                        "staminaMax": stamina_max,
                        "staminaMinCap": stamina_min_cap,
                        "staminaWeightLimitThreshold": stamina_weight_threshold,
                        "staminaKgToStaminaPercentPenalty": stamina_penalty,
                        "sprintStaminaModifierErc": sprint_sta_mod_erc,
                        "sprintStaminaModifierCro": sprint_sta_mod_cro,
                        "sprintSwimmingStaminaModifier": sprint_swim_mod,
                        "sprintLadderStaminaModifier": sprint_ladder_mod,
                        "meleeStaminaModifier": melee_sta_mod,
                        "obstacleTraversalStaminaModifier": obstacle_sta_mod,
                        "holdBreathStaminaModifier": hold_breath_mod,
                    })
                    player["StaminaData"] = stamina
    
                    # Shock
                    shock.update({
                        "shockRefillSpeedConscious": shock_refill_con,
                        "shockRefillSpeedUnconscious": shock_refill_uncon,
                        "allowRefillSpeedModifier": allow_refill_mod,
                    })
                    player["ShockHandlingData"] = shock
    
                    # Movement
                    movement.update({
                        "timeToSprint": time_to_sprint,
                        "timeToStrafeJog": time_to_strafe_jog,
                        "timeToStrafeSprint": time_to_strafe_sprint,
                        "rotationSpeedJog": rot_speed_jog,
                        "rotationSpeedSprint": rot_speed_sprint,
                        "allowStaminaAffectInertia": allow_sta_inertia,
                    })
                    player["MovementData"] = movement
    
                    # Drowning
                    player["DrowningData"] = {
                        "staminaDepletionSpeed": drown_stamina,
                        "healthDepletionSpeed": drown_health,
                        "shockDepletionSpeed": drown_shock,
                    }
    
                    # Weapon Obstruction
                    player["WeaponObstructionData"] = {
                        "staticMode": weapon_static_mode,
                        "dynamicMode": weapon_dynamic_mode,
                    }
    
                    player["disablePersonalLight"] = disable_personal_light
                    cfg["PlayerData"] = player
    
                    # Base Building
                    cfg["BaseBuildingData"] = {
                        "HologramData": {
                            "disableIsCollidingBBoxCheck": dis_bbox,
                            "disableIsCollidingPlayerCheck": dis_player,
                            "disableIsClippingRoofCheck": dis_roof,
                            "disableIsBaseViableCheck": dis_base_viable,
                            "disableIsCollidingGPlotCheck": dis_gplot,
                            "disableIsCollidingAngleCheck": dis_angle,
                            "disableIsPlacementPermittedCheck": dis_placement,
                            "disableHeightPlacementCheck": dis_height,
                            "disableIsUnderwaterCheck": dis_underwater,
                            "disableIsInTerrainCheck": dis_terrain,
                            "disableColdAreaBuildingCheck": dis_cold,
                            "disallowedTypesInUnderground": hologram.get("disallowedTypesInUnderground", ["FenceKit","TerritoryFlagKit","WatchtowerKit"]),
                        },
                        "ConstructionData": {
                            "disablePerformRoofCheck": dis_roof_check,
                            "disableIsCollidingCheck": dis_colliding_check,
                            "disableDistanceCheck": dis_distance_check,
                        }
                    }
    
                    # Mapa
                    map_data["displayPlayerPosition"] = display_player_pos
                    map_data["displayNavInfo"] = display_nav_info
                    map_data["ignoreMapOwnership"] = ignore_map_ownership
                    map_data["ignoreNavItemsOwnership"] = ignore_nav_ownership
                    cfg["MapData"] = map_data
    
                    # UI + HitIndication
                    ui_data["use3DMap"] = use_3d_map
                    ui_data["HitIndicationData"] = {
                        "hitDirectionOverrideEnabled": hit_dir_override,
                        "hitDirectionBehaviour": hit_dir_behaviour,
                        "hitDirectionStyle": hit_dir_style,
                        "hitDirectionIndicatorColorStr": hit_color,
                        "hitDirectionMaxDuration": hit_max_duration,
                        "hitDirectionBreakPointRelative": hit_breakpoint,
                        "hitDirectionScatter": hit_scatter,
                        "hitIndicationPostProcessEnabled": hit_post_process,
                    }
                    cfg["UIData"] = ui_data
    
                    # Mundo
                    worlds["lightingConfig"] = lighting_config
                    cfg["WorldsData"] = worlds
    
                    # Mapa / UI
                    map_data["displayPlayerPosition"] = display_player_pos
                    map_data["displayNavInfo"] = display_nav_info
                    cfg["MapData"] = map_data
    
                    ui_data["use3DMap"] = use_3d_map
                    cfg["UIData"] = ui_data
    
                    # Veículos
                    vehicle_data["boatDecayMultiplier"] = boat_decay_multiplier
                    cfg["VehicleData"] = vehicle_data
    
                    st.session_state[cfg_session_key] = cfg
                    st.success("Alterações aplicadas internamente ao cfggameplay.json.")
    
            # 4) Download do arquivo ajustado
            with col_save2:
                if st.button("⬇️ Baixar cfggameplay.json ajustado", use_container_width=True):
                    try:
                        cfg_bytes = json.dumps(
                            st.session_state[cfg_session_key],
                            ensure_ascii=False,
                            indent=4,
                        ).encode("utf-8")
    
                        st.download_button(
                            label="Baixar cfggameplay.json",
                            data=cfg_bytes,
                            file_name="cfggameplay_editado.json",
                            mime="application/json",
                            use_container_width=True,
                        )
                    except Exception as e:
                        st.error(f"Erro ao gerar cfggameplay.json: {e}")
    
            # (Opcional) 5) Salvar em disco + enviar via FTP
            st.markdown("### 🚀 Salvar no Titan Cloud e enviar via FTP (opcional)")
    
            mapa_cfggameplay = st.selectbox(
                "Mapa de destino (onde o cfggameplay.json será aplicado)",
                ["Chernarus", "Livonia"],
                key="cfggameplay_mapa_dest",
            )
    
            if st.button(
                "Salvar no Titan Cloud e enviar via FTP (cfggameplay)",
                use_container_width=True,
            ):
                try:
                    cfg_bytes = json.dumps(
                        st.session_state[cfg_session_key],
                        ensure_ascii=False,
                        indent=4,
                    ).encode("utf-8")
                except Exception as e:
                    st.error(f"Erro ao serializar cfggameplay.json: {e}")
                    st.stop()
    
                safe_name_cfg = f"{user_id[:5]}_cfggameplay_{mapa_cfggameplay.lower()}.json"
                local_cfggameplay_path = os.path.join(UPLOAD_DIR, safe_name_cfg)
    
                try:
                    with open(local_cfggameplay_path, "wb") as f:
                        f.write(cfg_bytes)
                except Exception as e:
                    registrar_log(
                        user_id,
                        f"Erro ao salvar cfggameplay.json localmente ({str(e)})",
                        "erro",
                    )
                    st.error(f"Erro ao salvar cfggameplay.json localmente: {e}")
                    st.stop()
    
                # Você implementa essa função seguindo o padrão types/globals:
                ok_cfg, msg_cfg = enviar_cfggameplay_via_ftp(
                    user_id,
                    local_cfggameplay_path,
                    mapa_cfggameplay,
                )
    
                if ok_cfg:
                    registrar_log(
                        user_id,
                        f"cfggameplay.json atualizado e enviado via FTP para {mapa_cfggameplay}.",
                        "sucesso",
                    )
                    st.success("cfggameplay.json enviado e aplicado via FTP com sucesso!")
                else:
                    registrar_log(
                        user_id,
                        f"Falha ao enviar cfggameplay.json via FTP ({msg_cfg})",
                        "erro",
                    )
                    st.error(f"Erro ao enviar cfggameplay via FTP: {msg_cfg}")
        else:
            st.info("Envie o cfggameplay.json do seu servidor para começar a editar.")
    
with tabevents:
    if not plano_permite(plano_atual, "editor_events"):
        bloquear_funcionalidade(plano_atual, "📅 Editor de Eventos (events.xml)")
    else:
        st.subheader("🎪 Eventos / events.xml")
        st.info("Você pode enviar o events.xml manualmente ou carregar direto do servidor via FTP.")
    
        # chaves únicas da sessão para este usuário
        key_tree = f"events_tree_{user_id}"
        key_root = f"events_root_{user_id}"
        key_df = f"events_df_{user_id}"
    
        mapa_events = st.selectbox(
            "Mapa do events.xml",
            ["Chernarus", "Livonia"],
            key=f"mapa_events_ftp_{user_id}"
        )
    
        cole1, cole2 = st.columns([1, 1])
    
        with cole1:
            if st.button(
                "📥 Carregar events.xml do servidor via FTP",
                use_container_width=True,
                key=f"btn_load_events_ftp_{user_id}"
            ):
                ok, xml_bytes, msg = baixareventsviaftp(user_id, mapa_events)
    
                if ok:
                    try:
                        tree, root, df_events = parse_events_xml(xml_bytes)
    
                        st.session_state[key_tree] = tree
                        st.session_state[key_root] = root
                        st.session_state[key_df] = df_events
    
                        st.success(f"events.xml carregado do servidor com sucesso! ({len(df_events)} eventos)")
                    except Exception as e:
                        st.error(f"Arquivo baixado, mas houve erro ao interpretar o XML: {e}")
                else:
                    st.error(f"Erro ao baixar events.xml via FTP: {msg}")
    
        with cole2:
            up_events = st.file_uploader(
                "Enviar events.xml",
                type=["xml"],
                key=f"up_eventsxml_{user_id}"
            )
    
        if up_events is not None:
            try:
                xml_bytes = up_events.read()
                tree, root, df_events = parse_events_xml(xml_bytes)
    
                st.session_state[key_tree] = tree
                st.session_state[key_root] = root
                st.session_state[key_df] = df_events
    
                st.success(f"Arquivo carregado: {up_events.name} ({len(df_events)} eventos)")
            except Exception as e:
                st.error(f"Erro ao ler events.xml: {e}")
    
        mapaevents = st.selectbox(
            "Mapa de destino onde o events.xml será aplicado",
            ["Chernarus", "Livonia"],
            key=f"mapaevents_{user_id}"
        )
    
        if key_df in st.session_state:
            df_events = st.session_state[key_df].copy()
    
            if "active" in df_events.columns:
                df_events["active"] = df_events["active"].astype(bool)
    
            st.markdown("### 🔍 Filtros rápidos")
    
            colf1, colf2 = st.columns([2, 1])
    
            with colf1:
                busca_evento = st.text_input(
                    "Buscar evento por nome",
                    "",
                    key=f"busca_events_{user_id}"
                )
    
            with colf2:
                somente_ativos = st.checkbox(
                    "Mostrar apenas ativos",
                    key=f"ativos_events_{user_id}"
                )
    
            df_view = df_events.copy()
    
            if busca_evento.strip():
                df_view = df_view[
                    df_view["name"].astype(str).str.contains(busca_evento.strip(), case=False, na=False)
                ]
    
            if somente_ativos and "active" in df_view.columns:
                df_view = df_view[df_view["active"]]
    
            st.markdown("### ✏️ Ajuste de parâmetros")
    
            edited_df = st.data_editor(
                df_view,
                num_rows="fixed",
                hide_index=True,
                use_container_width=True,
                column_config={
                    "name": st.column_config.TextColumn(
                        "Evento",
                        help="Nome interno do evento no events.xml."
                    ),
                    "nominal": st.column_config.NumberColumn(
                        "Nominal",
                        min_value=0,
                        step=1
                    ),
                    "min": st.column_config.NumberColumn(
                        "Min",
                        min_value=0,
                        step=1
                    ),
                    "max": st.column_config.NumberColumn(
                        "Max",
                        min_value=0,
                        step=1
                    ),
                    "lifetime": st.column_config.NumberColumn(
                        "Lifetime",
                        min_value=0,
                        step=1
                    ),
                    "restock": st.column_config.NumberColumn(
                        "Restock",
                        min_value=0,
                        step=1
                    ),
                    "saferadius": st.column_config.NumberColumn(
                        "SafeRadius",
                        min_value=0,
                        step=1
                    ),
                    "distanceradius": st.column_config.NumberColumn(
                        "DistanceRadius",
                        min_value=0,
                        step=1
                    ),
                    "cleanupradius": st.column_config.NumberColumn(
                        "CleanupRadius",
                        min_value=0,
                        step=1
                    ),
                    "active": st.column_config.CheckboxColumn(
                        "Ativo",
                        help="Define se o evento está ativo no XML."
                    ),
                },
                disabled=["name"],
                key=f"editor_events_{user_id}"
            )
    
            st.markdown("### 💾 Gerar, baixar e aplicar")
    
            colb1, colb2, colb3 = st.columns(3)
    
            with colb1:
                if st.button(
                    "Aplicar alterações na sessão",
                    use_container_width=True,
                    key=f"btn_apply_events_{user_id}"
                ):
                    df_merged = df_events.copy().set_index("name")
                    edited_indexed = edited_df.copy().set_index("name")
    
                    for idx in edited_indexed.index:
                        if idx in df_merged.index:
                            for col in [
                                "nominal",
                                "min",
                                "max",
                                "lifetime",
                                "restock",
                                "saferadius",
                                "distanceradius",
                                "cleanupradius",
                                "active",
                            ]:
                                if col in edited_indexed.columns and col in df_merged.columns:
                                    df_merged.loc[idx, col] = edited_indexed.loc[idx, col]
    
                    st.session_state[key_df] = df_merged.reset_index()
                    st.success("Alterações aplicadas internamente ao events.xml (sessão).")
    
            with colb2:
                tree = st.session_state.get(key_tree)
                root = st.session_state.get(key_root)
                df_full = st.session_state.get(key_df)
    
                if tree is not None and root is not None and df_full is not None:
                    try:
                        new_xml_bytes = apply_df_to_events_xml(tree, root, df_full)
    
                        st.download_button(
                            label="⬇️ Baixar events.xml ajustado",
                            data=new_xml_bytes,
                            file_name="events_editado.xml",
                            mime="application/xml",
                            use_container_width=True,
                            key=f"download_events_editado_{user_id}"
                        )
                    except Exception as e:
                        st.error(f"Erro ao gerar events.xml ajustado: {e}")
    
            with colb3:
                if st.button(
                    "Salvar no Titan Cloud e enviar via FTP",
                    use_container_width=True,
                    key=f"btn_events_save_{user_id}"
                ):
                    tree = st.session_state.get(key_tree)
                    root = st.session_state.get(key_root)
                    df_full = st.session_state.get(key_df)
    
                    if tree is None or root is None or df_full is None:
                        st.error("Dados do XML não encontrados na sessão. Reenvie o arquivo.")
                    else:
                        try:
                            new_xml_bytes = apply_df_to_events_xml(tree, root, df_full)
    
                            safe_name = f"{user_id[:5]}_events_{mapaevents.lower()}.xml"
                            localpath = os.path.join(UPLOAD_DIR, safe_name)
    
                            with open(localpath, "wb") as f:
                                f.write(new_xml_bytes)
    
                            ok, msg = enviareventsviaftp(user_id, localpath, mapaevents)
    
                            if ok:
                                registrar_log(
                                    user_id,
                                    f"events.xml atualizado e enviado via FTP para {mapaevents}.",
                                    "sucesso"
                                )
                                st.success("events.xml enviado e aplicado via FTP com sucesso!")
                            else:
                                registrar_log(
                                    user_id,
                                    f"Falha ao enviar events.xml via FTP: {msg}",
                                    "erro"
                                )
                                st.error(f"Erro ao enviar events.xml via FTP: {msg}")
    
                        except Exception as e:
                            registrar_log(
                                user_id,
                                f"Erro ao salvar/enviar events.xml: {str(e)}",
                                "erro"
                            )
                            st.error(f"Erro ao salvar/enviar events.xml: {e}")
    
            st.divider()
            st.markdown("### ℹ️ Observações rápidas")
            st.write("- Use primeiro o botão Aplicar alterações na sessão antes de baixar ou enviar.")
            st.write("- O download e o FTP sempre usam o DataFrame salvo na sessão.")
        else:
            st.info("Envie o events.xml do seu servidor para começar a editar.")
    
    
with tabmessages:
    if not plano_permite(plano_atual, "editor_messages"):
        bloquear_funcionalidade(plano_atual, "💬 Editor de Mensagens (messages.xml)")
    else:
        st.subheader("💬 Mensagens / messages.xml")
        st.info("Você pode enviar o messages.xml manualmente ou carregar direto do servidor via FTP.")
    
        # chaves únicas da sessão para este usuário
        key_tree = f"messages_tree_{user_id}"
        key_root = f"messages_root_{user_id}"
        key_df = f"messages_df_{user_id}"
    
        mapa_messages = st.selectbox(
            "Mapa do messages.xml",
            ["Chernarus", "Livonia"],
            key=f"mapa_messages_ftp_{user_id}"
        )
    
        colm1, colm2 = st.columns([1, 1])
    
        with colm1:
            if st.button(
                "📥 Carregar messages.xml do servidor via FTP",
                use_container_width=True,
                key=f"btn_load_messages_ftp_{user_id}"
            ):
                ok, xml_bytes, msg = baixarmessagesviaftp(user_id, mapa_messages)
    
                if ok:
                    try:
                        tree, root, df_messages = parse_messages_xml(xml_bytes)
    
                        st.session_state[key_tree] = tree
                        st.session_state[key_root] = root
                        st.session_state[key_df] = df_messages
    
                        st.success(f"messages.xml carregado do servidor com sucesso! ({len(df_messages)} mensagens)")
                    except Exception as e:
                        st.error(f"Arquivo baixado, mas houve erro ao interpretar o XML: {e}")
                else:
                    st.error(f"Erro ao baixar messages.xml via FTP: {msg}")
    
        with colm2:
            up_messages = st.file_uploader(
                "Enviar messages.xml",
                type=["xml"],
                key=f"up_messagesxml_{user_id}"
            )
    
        if up_messages is not None:
            try:
                xml_bytes = up_messages.read()
                tree, root, df_messages = parse_messages_xml(xml_bytes)
    
                st.session_state[key_tree] = tree
                st.session_state[key_root] = root
                st.session_state[key_df] = df_messages
    
                st.success(f"Arquivo carregado: {up_messages.name} ({len(df_messages)} mensagens)")
            except Exception as e:
                st.error(f"Erro ao ler messages.xml: {e}")
    
        mapamessages = st.selectbox(
            "Mapa de destino onde o messages.xml será aplicado",
            ["Chernarus", "Livonia"],
            key=f"mapamessages_{user_id}"
        )
    
        if key_df in st.session_state:
            df_messages = st.session_state[key_df].copy()
    
            if "text" not in df_messages.columns:
                df_messages["text"] = ""
    
            if "time" not in df_messages.columns:
                df_messages["time"] = 0
    
            if "ordem" not in df_messages.columns:
                df_messages["ordem"] = range(1, len(df_messages) + 1)
    
            if "_elem" not in df_messages.columns:
                df_messages["_elem"] = None
    
            st.markdown("### 🔍 Mensagens atualmente aplicadas")
    
            busca_msg = st.text_input(
                "Buscar por texto da mensagem",
                "",
                key=f"busca_messages_{user_id}"
            )
    
            with st.expander("🔎 Diagnóstico do messages.xml carregado", expanded=False):
                st.write(f"Total de mensagens detectadas: {len(df_messages)}")
                if not df_messages.empty:
                    cols_diag = [c for c in ["ordem", "time", "text"] if c in df_messages.columns]
                    st.dataframe(
                        df_messages[cols_diag].copy(),
                        use_container_width=True
                    )
    
            st.markdown("### ➕ Inclusão rápida")
    
            col_new1, col_new2 = st.columns([3, 1])
    
            with col_new1:
                nova_msg_texto = st.text_input(
                    "Texto da nova mensagem",
                    key=f"nova_msg_texto_{user_id}",
                    placeholder="Ex: Reinício do servidor em 10 minutos."
                )
    
            with col_new2:
                nova_msg_tempo = st.number_input(
                    "Tempo",
                    min_value=0,
                    step=1,
                    value=30,
                    key=f"nova_msg_tempo_{user_id}"
                )
    
            if st.button("Adicionar nova mensagem à sessão", use_container_width=True, key=f"btn_add_message_{user_id}"):
                if not nova_msg_texto.strip():
                    st.warning("Digite o texto da nova mensagem antes de adicionar.")
                else:
                    nova_ordem = int(df_messages["ordem"].max()) + 1 if not df_messages.empty else 1
    
                    nova_linha = {
                        "ordem": nova_ordem,
                        "id": "",
                        "name": "",
                        "time": int(nova_msg_tempo),
                        "priority": 0,
                        "color": "",
                        "icon": "",
                        "text": nova_msg_texto.strip(),
                        "_elem": None,
                    }
    
                    df_messages = pd.concat([df_messages, pd.DataFrame([nova_linha])], ignore_index=True)
                    st.session_state[key_df] = df_messages
                    st.success("Nova mensagem adicionada à sessão. Agora você pode revisar, baixar ou enviar.")
    
            df_view = df_messages.copy()
    
            if busca_msg.strip():
                df_view = df_view[
                    df_view["text"].astype(str).str.contains(busca_msg.strip(), case=False, na=False)
                ]
    
            st.markdown("### ✏️ Ajuste de mensagens")
    
            cols_editor = [c for c in ["ordem", "time", "text"] if c in df_view.columns]
    
            edited_df = st.data_editor(
                df_view[cols_editor],
                num_rows="dynamic",
                hide_index=True,
                use_container_width=True,
                column_config={
                    "ordem": st.column_config.NumberColumn("Ordem", disabled=True),
                    "time": st.column_config.NumberColumn("Tempo", min_value=0, step=1),
                    "text": st.column_config.TextColumn("Mensagem", width="large"),
                },
                disabled=["ordem"],
                key=f"editor_messages_{user_id}"
            )
    
            st.markdown("### 💾 Gerar, baixar e aplicar")
    
            colmsg1, colmsg2, colmsg3 = st.columns(3)
    
            with colmsg1:
                if st.button(
                    "Aplicar alterações na sessão",
                    use_container_width=True,
                    key=f"btn_apply_messages_{user_id}"
                ):
                    df_full = df_messages.copy().set_index("ordem")
                    df_edit = edited_df.copy().set_index("ordem")
    
                    for idx in df_edit.index:
                        if idx in df_full.index:
                            for col in ["time", "text"]:
                                if col in df_edit.columns and col in df_full.columns:
                                    df_full.loc[idx, col] = df_edit.loc[idx, col]
    
                    novos_indices = [idx for idx in df_edit.index if idx not in df_full.index]
                    if novos_indices:
                        novas_linhas = df_edit.loc[novos_indices].reset_index()
                        novas_linhas["id"] = ""
                        novas_linhas["name"] = ""
                        novas_linhas["priority"] = 0
                        novas_linhas["color"] = ""
                        novas_linhas["icon"] = ""
                        novas_linhas["_elem"] = None
                        df_full = pd.concat([df_full.reset_index(), novas_linhas], ignore_index=True).set_index("ordem")
    
                    st.session_state[key_df] = df_full.reset_index().sort_values("ordem").reset_index(drop=True)
                    st.success("Alterações aplicadas internamente ao messages.xml (sessão).")
    
            with colmsg2:
                tree = st.session_state.get(key_tree)
                root = st.session_state.get(key_root)
                df_full = st.session_state.get(key_df)
    
                if tree is not None and root is not None and df_full is not None:
                    try:
                        new_xml_bytes = apply_df_to_messages_xml(tree, root, df_full)
    
                        st.download_button(
                            label="⬇️ Baixar messages.xml ajustado",
                            data=new_xml_bytes,
                            file_name="messages_editado.xml",
                            mime="application/xml",
                            use_container_width=True,
                            key=f"download_messages_editado_{user_id}"
                        )
                    except Exception as e:
                        st.error(f"Erro ao gerar messages.xml ajustado: {e}")
    
            with colmsg3:
                if st.button(
                    "Salvar no Titan Cloud e enviar via FTP",
                    use_container_width=True,
                    key=f"btn_messages_save_{user_id}"
                ):
                    tree = st.session_state.get(key_tree)
                    root = st.session_state.get(key_root)
                    df_full = st.session_state.get(key_df)
    
                    if tree is None or root is None or df_full is None:
                        st.error("Dados do XML não encontrados na sessão. Reenvie o arquivo.")
                    else:
                        try:
                            new_xml_bytes = apply_df_to_messages_xml(tree, root, df_full)
    
                            safename = f"{user_id[:5]}_messages_{mapamessages.lower()}.xml"
                            localpath = os.path.join(UPLOAD_DIR, safename)
    
                            with open(localpath, "wb") as f:
                                f.write(new_xml_bytes)
    
                            ok, msg = enviarmessagesviaftp(user_id, localpath, mapamessages)
    
                            if ok:
                                registrar_log(
                                    user_id,
                                    f"messages.xml atualizado e enviado via FTP para {mapamessages}.",
                                    "sucesso"
                                )
                                st.success("messages.xml enviado e aplicado via FTP com sucesso!")
                            else:
                                registrar_log(
                                    user_id,
                                    f"Falha ao enviar messages.xml via FTP: {msg}",
                                    "erro"
                                )
                                st.error(f"Erro ao enviar messages.xml via FTP: {msg}")
    
                        except Exception as e:
                            registrar_log(
                                user_id,
                                f"Erro ao salvar/enviar messages.xml: {str(e)}",
                                "erro"
                            )
                            st.error(f"Erro ao salvar/enviar messages.xml: {e}")
    
            st.divider()
            st.markdown("### ℹ️ Observações rápidas")
            st.write("- A interface mostra apenas os campos realmente úteis para o seu messages.xml atual.")
            st.write("- Você pode adicionar novas mensagens direto pela área de inclusão rápida.")
            st.write("- Use primeiro o botão Aplicar alterações na sessão antes de baixar ou enviar.")
            st.write("- O download e o FTP sempre usam o DataFrame salvo na sessão.")
            st.write("- O bloco de diagnóstico ajuda a validar se o parser leu corretamente o schema do seu XML.")
        else:
            st.info("Envie o messages.xml do seu servidor para começar a visualizar, editar e incluir novas mensagens.")
    
    
with tabcfgeventspawns:
    if not plano_permite(plano_atual, "editor_cfgeventspawns"):
        bloquear_funcionalidade(plano_atual, "📍 Editor de Spawns (cfgeventspawns.xml)")
    else:
        st.subheader("📍 Spawns / cfgeventspawns.xml")
        st.info("Você pode enviar o cfgeventspawns.xml manualmente ou carregar direto do servidor via FTP.")
    
        # chaves únicas da sessão para este usuário
        key_tree = f"cfgeventspawns_tree_{user_id}"
        key_root = f"cfgeventspawns_root_{user_id}"
        key_map = f"cfgeventspawns_map_{user_id}"
    
        mapa_spawns = st.selectbox(
            "Mapa do cfgeventspawns.xml",
            ["Chernarus", "Livonia"],
            key=f"mapa_cfgeventspawns_ftp_{user_id}"
        )
    
        cols1, cols2 = st.columns([1, 1])
    
        with cols1:
            if st.button(
                "📥 Carregar cfgeventspawns.xml do servidor via FTP",
                use_container_width=True,
                key=f"btn_load_cfgeventspawns_ftp_{user_id}"
            ):
                ok, xml_bytes, msg = baixarcfgeventspawnsviaftp(user_id, mapa_spawns)
    
                if ok:
                    try:
                        tree, root, eventos_map = parse_cfgeventspawns_xml(xml_bytes)
    
                        st.session_state[key_tree] = tree
                        st.session_state[key_root] = root
                        st.session_state[key_map] = eventos_map
    
                        st.success(f"cfgeventspawns.xml carregado do servidor com sucesso! ({len(eventos_map)} eventos)")
                    except Exception as e:
                        st.error(f"Arquivo baixado, mas houve erro ao interpretar o XML: {e}")
                else:
                    st.error(f"Erro ao baixar cfgeventspawns.xml via FTP: {msg}")
    
        with cols2:
            up_cfgeventspawns = st.file_uploader(
                "Enviar cfgeventspawns.xml",
                type=["xml"],
                key=f"up_cfgeventspawnsxml_{user_id}"
            )
    
        if up_cfgeventspawns is not None:
            try:
                xml_bytes = up_cfgeventspawns.read()
                tree, root, eventos_map = parse_cfgeventspawns_xml(xml_bytes)
    
                st.session_state[key_tree] = tree
                st.session_state[key_root] = root
                st.session_state[key_map] = eventos_map
    
                st.success(f"Arquivo carregado: {up_cfgeventspawns.name} ({len(eventos_map)} eventos)")
            except Exception as e:
                st.error(f"Erro ao ler cfgeventspawns.xml: {e}")
    
        if key_map in st.session_state:
            eventos_map = st.session_state[key_map]
            nomes_eventos = sorted(eventos_map.keys())
    
            if not nomes_eventos:
                st.warning("Nenhum evento encontrado no cfgeventspawns.xml.")
                st.stop()
    
            colsel1, colsel2 = st.columns([2, 1])
    
            with colsel1:
                evento_sel = st.selectbox(
                    "Selecione o evento para editar",
                    options=nomes_eventos,
                    key=f"cfgeventspawns_evento_sel_{user_id}"
                )
    
            with colsel2:
                mapa_dest = st.selectbox(
                    "Mapa de destino",
                    ["Chernarus", "Livonia"],
                    key=f"cfgeventspawns_mapa_{user_id}"
                )
    
            df_evento = eventos_map[evento_sel].copy()
    
            st.markdown("### Posições do evento")
            st.caption("Edite coordenadas X/Z, ângulo A e altura Y quando existir. Você também pode adicionar novas linhas.")
    
            edited_df = st.data_editor(
                df_evento,
                num_rows="dynamic",
                hide_index=True,
                use_container_width=True,
                column_config={
                    "x": st.column_config.NumberColumn("X", step=0.1),
                    "z": st.column_config.NumberColumn("Z", step=0.1),
                    "a": st.column_config.NumberColumn("Ângulo A", step=0.1),
                    "y": st.column_config.NumberColumn("Altura Y", step=0.1),
                },
                key=f"editor_cfgeventspawns_{user_id}_{evento_sel}"
            )
    
            c1, c2, c3 = st.columns(3)
    
            with c1:
                if st.button(
                    "Aplicar alterações na sessão",
                    use_container_width=True,
                    key=f"btn_apply_cfgeventspawns_{user_id}"
                ):
                    eventos_map[evento_sel] = edited_df
                    st.session_state[key_map] = eventos_map
                    st.success(f"Alterações aplicadas ao evento '{evento_sel}' na sessão.")
    
            with c2:
                if st.button(
                    "Baixar cfgeventspawns.xml ajustado",
                    use_container_width=True,
                    key=f"btn_download_cfgeventspawns_{user_id}"
                ):
                    try:
                        tree = st.session_state.get(key_tree)
                        root = st.session_state.get(key_root)
                        eventos_map = st.session_state.get(key_map)
    
                        if tree is None or root is None or eventos_map is None:
                            st.error("Dados do XML não encontrados na sessão. Reenvie o arquivo.")
                        else:
                            eventos_map[evento_sel] = edited_df
                            newxmlbytes = aplicar_eventos_map_no_cfgeventspawns(tree, root, eventos_map)
    
                            st.download_button(
                                label="Clique para baixar",
                                data=newxmlbytes,
                                file_name="cfgeventspawns_editado.xml",
                                mime="application/xml",
                                use_container_width=True
                            )
                    except Exception as e:
                        st.error(f"Erro ao gerar cfgeventspawns.xml: {e}")
    
            with c3:
                if st.button(
                    "Salvar no Titan Cloud e enviar via FTP",
                    use_container_width=True,
                    key=f"btn_ftp_cfgeventspawns_{user_id}"
                ):
                    try:
                        tree = st.session_state.get(key_tree)
                        root = st.session_state.get(key_root)
                        eventos_map = st.session_state.get(key_map)
    
                        if tree is None or root is None or eventos_map is None:
                            st.error("Dados do XML não encontrados na sessão. Reenvie o arquivo.")
                        else:
                            eventos_map[evento_sel] = edited_df
                            newxmlbytes = aplicar_eventos_map_no_cfgeventspawns(tree, root, eventos_map)
    
                            safe_name = f"{user_id}_cfgeventspawns_{mapa_dest.lower()}.xml"
                            localpath = os.path.join(UPLOAD_DIR, safe_name)
    
                            with open(localpath, "wb") as f:
                                f.write(newxmlbytes)
    
                            ok, msg = enviar_cfgeventspawns_via_ftp(user_id, localpath, mapa_dest)
    
                            if ok:
                                registrar_log(user_id, f"cfgeventspawns.xml atualizado e enviado via FTP para {mapa_dest}.", "sucesso")
                                st.success("cfgeventspawns.xml enviado e aplicado via FTP com sucesso!")
                            else:
                                registrar_log(user_id, f"Falha ao enviar cfgeventspawns.xml via FTP: {msg}", "erro")
                                st.error(f"Erro ao enviar via FTP: {msg}")
    
                    except Exception as e:
                        registrar_log(user_id, f"Erro ao salvar/enviar cfgeventspawns.xml: {str(e)}", "erro")
                        st.error(f"Erro ao salvar/enviar cfgeventspawns.xml: {e}")
    
            st.divider()
            st.markdown("### Resumo do evento selecionado")
            st.write(f"- Evento: {evento_sel}")
            st.write(f"- Total de posições carregadas: {len(edited_df)}")

with tab_raid:
    st.subheader("🛡️ Gestão de Horários de RAID")
    st.info("O RAID automatizado altera o arquivo `cfggameplay.json`. No início do RAID o dano em bases é ativado, e no fim é desativado automaticamente.")

    # Formulário de Agendamento
    with st.expander("➕ Agendar Nova Janela de RAID", expanded=True):
        col_r1, col_r2 = st.columns(2)
        with col_r1:
            data_r = st.date_input("Data do RAID", min_value=get_hora_brasilia().date(), key=f"date_raid_{user_id}")
            h_ini_r = st.text_input("Hora Início (ex: 20:00)", "20:00", key=f"h_ini_raid_{user_id}")
        with col_r2:
            h_fim_r = st.text_input("Hora Fim (ex: 23:59)", "23:59", key=f"h_fim_raid_{user_id}")
            mapa_r = st.selectbox("Mapa do RAID", ["Chernarus", "Livonia"], key=f"mapa_raid_{user_id}")
            rec_r = st.selectbox("Recorrência", ["Único", "Diário", "Semanal"], key=f"rec_raid_{user_id}")
        
        # BOTÃO ÚNICO COM KEY PARA EVITAR DUPLICIDADE
        if st.button("🚀 Confirmar Agendamento de RAID", use_container_width=True, key=f"btn_confirmar_raid_final_{user_id}"):
            novo_raid = {
                "id": str(time.time()),
                "data": data_r.strftime("%d/%m/%Y"),
                "in": h_ini_r,
                "out": h_fim_r,
                "mapa": mapa_r,
                "rec": rec_r,
                "status": "Aguardando"
            }
            client_data.setdefault("agendas_raid", []).append(novo_raid)
            save_db(DB_CLIENTS, st.session_state.db_clients)
            registrar_log(user_id, f"RAID Agendado ({rec_r}): {data_r.strftime('%d/%m/%Y')} às {h_ini_r}", "info")
            st.success(f"RAID {rec_r} agendado com sucesso!")
            st.rerun()

    # Lista de RAIDs Agendados
    st.markdown("### 📋 RAIDs Programados")
    raids_lista = client_data.get("agendas_raid", [])
    if not raids_lista:
        st.info("Nenhum RAID agendado no momento.")
    else:
        for r in raids_lista:
            cor_r = "🔵" if r["status"] == "Aguardando" else "🟢" if r["status"] == "Ativo" else "⚪"
            with st.expander(f"{cor_r} RAID {r['mapa']} | {r['data']} | {r['in']} às {r['out']}"):
                st.write(f"**Recorrência:** {r.get('rec', 'Único')}")
                st.write(f"**Status:** {r['status']}")
                if st.button("Excluir RAID", key=f"del_raid_{r['id']}", type="secondary"):
                    client_data["agendas_raid"] = [i for i in client_data["agendas_raid"] if i["id"] != r["id"]]
                    save_db(DB_CLIENTS, st.session_state.db_clients)
                    st.rerun()
    
with tab6:
    st.subheader("🛒 Loja / Trader & Gestão de Vendas")
    
    # --- SUB-NAVEGAÇÃO INTERNA GRC ---
    # Permite alternar funções sem poluir o topo do site com mais abas
    menu_interno_loja = st.radio(
        "Selecione a operação:",
        ["📦 Gestão de Pedidos", "🛠️ Configurar Catálogo"],
        horizontal=True,
        label_visibility="collapsed",
        key=f"menu_interno_loja_{user_id}"
    )
    
    st.divider()

    # --- 1. TELA DE GESTÃO E ESTORNO (NOVO) ---
    if menu_interno_loja == "📦 Gestão de Pedidos":
        # Chama a função de auditoria declarada anteriormente
        # Certifique-se de que a função 'render_gestao_pedidos' foi inserida no Passo 1 anterior
        nitrado_id_loja = st.session_state.db_users.get("keys", {}).get(user_id, {}).get("server_id", "")
        server_id_loja = user_id  # ← índice correto é sempre a KeyUser
        client_data_loja = st.session_state.db_clients.get(server_id_loja, {})
        render_gestao_pedidos(client_data_loja, server_id_loja)

    # --- 2. TELA DE CONFIGURAÇÃO DE ITENS (CÓDIGO ORIGINAL) ---
    else:
        st.info("Configure aqui o catálogo de itens da loja do seu servidor.")

        # Descobre o server_id real vinculado ao usuário logado
        server_id_loja = st.session_state.db_users.get("keys", {}).get(user_id, {}).get("server_id", user_id)

        # Recarrega a base mais atual do disco para garantir integridade
        db_completo = load_db(DB_CLIENTS, {})

        # Garante estrutura mínima do servidor
        if server_id_loja not in db_completo:
            db_completo[server_id_loja] = {
                "ftp": {"host": "", "user": "", "pass": "", "port": "21"},
                "agendas": [],
                "logs": [],
                "comunicados": [],
                "players": {},
                "loja": {"mapa_padrao": "Chernarus", "posicao_padrao": "", "itens": []},
            }

        client_data_loja = db_completo[server_id_loja]

        if "loja" not in client_data_loja:
            client_data_loja["loja"] = {"mapa_padrao": "Chernarus", "posicao_padrao": "", "itens": []}

        loja = client_data_loja["loja"]
        loja.setdefault("mapa_padrao", "Chernarus")
        loja.setdefault("posicao_padrao", "")
        loja.setdefault("itens", [])

        st.markdown("### ⚙️ Configurações gerais da Loja")

        col_conf1, col_conf2 = st.columns(2)
        with col_conf1:
            loja_mapa_padrao = st.selectbox(
                "Mapa padrão da Loja",
                ["Chernarus", "Livonia"],
                index=["Chernarus", "Livonia"].index(loja.get("mapa_padrao", "Chernarus")),
                key=f"loja_mapa_padrao_{user_id}",
            )
        with col_conf2:
            loja_posicao_padrao = st.text_input(
                "Coordenadas padrão de entrega (opcional)",
                value=loja.get("posicao_padrao", ""),
                help="Opcional. Coordenadas de referência para spawn do item.",
                key=f"loja_posicao_padrao_{user_id}",
            )

        st.markdown("### 📦 Itens do Catálogo")

        df_loja_key = f"df_loja_{server_id_loja}"
        loja_version_key = f"df_loja_version_{server_id_loja}"
        loja_serializada = json.dumps(loja, sort_keys=True, ensure_ascii=False)

        if (df_loja_key not in st.session_state or 
            loja_version_key not in st.session_state or 
            st.session_state[loja_version_key] != loja_serializada):
            st.session_state[df_loja_key] = loja_itens_to_df(loja)
            st.session_state[loja_version_key] = loja_serializada

        df_loja = st.session_state[df_loja_key]

        edited_df_loja = st.data_editor(
            df_loja,
            num_rows="dynamic",
            hide_index=True,
            column_config={
                "id": st.column_config.NumberColumn("ID", min_value=1, step=1),
                "nome": "Nome",
                "classe": "Classe DayZ",
                "categoria": "Categoria",
                "preco": st.column_config.NumberColumn("Preço (💎)", min_value=0, step=1),
                "quantidade": st.column_config.NumberColumn("Qtd", min_value=1, step=1),
                "ativo": st.column_config.CheckboxColumn("Ativo", default=True),
            },
            key=f"editor_loja_{server_id_loja}",
        )

        st.markdown("### 💾 Salvar catálogo")
        col_loja1, col_loja2, col_loja3, col_loja4 = st.columns(4)

        with col_loja1:
            if st.button("Aplicar na Sessão", use_container_width=True, key=f"btn_apply_loja_{user_id}"):
                st.session_state[df_loja_key] = edited_df_loja
                st.success("Alterações aplicadas temporariamente.")

        with col_loja2:
            if st.button("Salvar no Titan Cloud", use_container_width=True, key=f"btn_save_cloud_{user_id}"):
                db_completo = load_db(DB_CLIENTS, {})
                itens_atualizados = df_to_loja_itens(edited_df_loja)
                loja_obj = {"mapa_padrao": loja_mapa_padrao, "posicao_padrao": loja_posicao_padrao, "itens": itens_atualizados}
                db_completo[server_id_loja]["loja"] = loja_obj
                save_db(DB_CLIENTS, db_completo)
                st.session_state.db_clients = db_completo
                st.success("✅ Catálogo salvo e persistido!"); st.rerun()

        with col_loja3:
            itens_atualizados = df_to_loja_itens(edited_df_loja)
            loja_json = json.dumps({"servidor": user_info.get("server"), "itens": itens_atualizados}, indent=4, ensure_ascii=False)
            st.download_button(label="⬇️ Baixar JSON", data=loja_json.encode("utf-8"), file_name="Loja_Titan.json", mime="application/json", use_container_width=True)
            
        with col_loja4:
            if st.button("🔄 Recarregar Base", use_container_width=True, key=f"btn_reload_db_{user_id}"):
                st.session_state.pop(df_loja_key, None)
                st.rerun()

with tab7:
    st.subheader("👤 Jogadores / Vínculos")
    st.info(
        "Gerencie aqui o vínculo entre Gamertag dos jogadores e suas informações básicas. "
        "Esses dados serão usados pela Loja, Banco DzCoins e estatísticas."
    )

    # Busca o server_id a partir da user_key logada
    user_key = st.session_state.get("user_key", "")
    server_id = st.session_state.db_users.get("keys", {}).get(user_key, {}).get("server_id", user_key)
    if not server_id:
        st.error(
            "Nenhum servidor vinculado a este login.\n"
            "Faça login com uma KeyUser válida (gerada no painel de administração)."
        )
        st.stop()

    # Lê diretamente do arquivo para pegar vínculos feitos pelo portal
    db_clients = load_db(DB_CLIENTS, {})

    if not db_clients:
        st.warning("Nenhum cliente/servidor cadastrado.")
        st.stop()

    if server_id not in db_clients:
        st.error(
            f"O servidor com ID {server_id} não foi encontrado em clients_data.\n"
            "Verifique se o server_id existe em clients_data.json."
        )
        st.stop()

    client_data = db_clients[server_id]
    players = load_players_for_client(client_data)

    # Nome amigável do servidor a partir do users_db (se estiver em sessão)
    db_users = st.session_state.get("db_users", {"keys": {}})
    keyuser = st.session_state.get("user_key", "")
    nome_servidor = db_users.get("keys", {}).get(keyuser, {}).get("server", "Servidor sem nome")

    st.markdown("### 🧩 Servidor vinculado a este cliente")
    st.info(
        f"Cliente logado com a KeyUser **{keyuser or '(desconhecida)'}**, "
        f"servidor: **{nome_servidor}** (ID interno: **{server_id}**)"
    )

    # Converte SEMPRE a partir do JSON mais recente
    df_players = players_to_df(players)
    df_players_key = f"df_players_{server_id}"
    st.session_state[df_players_key] = df_players

    st.markdown("### 📋 Lista de jogadores vinculados")
    st.info(
        "Preencha a Gamertag (obrigatória) e, se quiser, apelido e ID do Discord. "
        "Futuramente, esses vínculos serão usados para Loja, Banco e ranking."
    )

    edited_df_players = st.data_editor(
        df_players,
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "gamertag": "Gamertag (obrigatório)",
            "apelido": "Apelido / Nome no Discord",
            "discord_id": "ID do Discord (opcional)",
            "observacoes": "Observações",
        },
    )

    st.markdown("### 💾 Salvar vínculos")

    col_p1, col_p2 = st.columns(2)

    with col_p1:
        if st.button("Aplicar alterações na sessão (Jogadores)", use_container_width=True):
            st.session_state[df_players_key] = edited_df_players
            st.success("Alterações aplicadas na sessão de Jogadores.")

    with col_p2:
        if st.button("Salvar Jogadores no Titan Cloud", use_container_width=True):
            players_atualizados = df_to_players(edited_df_players)
            gamertags_ativas = set(players_atualizados.keys())

            # Remove wallets e bank de jogadores que foram excluídos
            for secao in ("wallets", "bank"):
                if secao in client_data:
                    removidos = [g for g in client_data[secao] if g not in gamertags_ativas]
                    for g in removidos:
                        del client_data[secao][g]

            # Atualiza o client_data e o dicionário carregado do arquivo
            client_data["players"] = players_atualizados
            db_clients[server_id] = client_data

            # Salva em disco
            save_db(DB_CLIENTS, db_clients)

            # Mantém o DF da sessão sincronizado com o que foi salvo
            st.session_state[df_players_key] = edited_df_players

            st.success("Vínculos de jogadores salvos com sucesso no Titan Cloud!")

with tab_analytics:
    # Verifica se o administrador ativou o feed no painel de Governança
    feeds = client_data.get("feeds_config", {})
    if feeds.get("mapa_calor", True):
        render_heatmap(client_data)
    else:
        st.warning("⚠️ O Mapa de Calor está desativado nas configurações de Feeds (Aba ⚙️ Feeds / Bot).")

with tab_ranking:
    st.header("🏆 Configuração do Ranking")
    st.caption("Defina o período, modo de exibição e gerencie o ranking do seu servidor.")

    clients_data_rk = load_db(DB_CLIENTS, {})
    client_data_rk = clients_data_rk.get(server_id, client_data)

    ranking_config = client_data_rk.get("rankingconfig", {
        "ativo": True,
        "datainicial": "",
        "modoexibicao": "cumulativo",
        "tipojanela": "temporada",
        "permitirreprocessamento": True,
        "ultimareconfiguracao": "",
    })

    ranking_stats = client_data_rk.get("rankingstats", {
        "ultimaatualizacao": "",
        "periodoatual": "",
        "acumulado": {},
        "diario": {},
        "semanal": {},
        "mensal": {},
    })

    st.markdown("### Configurações do Período")

    cfg_col_1, cfg_col_2 = st.columns(2)

    with cfg_col_1:
        ranking_ativo = st.toggle(
            "Ranking ativo",
            value=ranking_config.get("ativo", True),
            key="ranking_ativo_toggle",
            help="Desative para pausar o processamento do ranking sem apagar os dados.",
        )

        modo_opcoes = ["cumulativo", "janela"]
        modo_labels = {
            "cumulativo": "Cumulativo desde a data inicial",
            "janela": "Por janela (período fixo)",
        }
        modo_atual = ranking_config.get("modoexibicao", "cumulativo")
        modo_idx = modo_opcoes.index(modo_atual) if modo_atual in modo_opcoes else 0

        ranking_modo = st.selectbox(
            "Modo de exibição",
            options=modo_opcoes,
            index=modo_idx,
            format_func=lambda x: modo_labels[x],
            key="ranking_modo_select",
            help="Cumulativo acumula tudo desde a data inicial. Janela reinicia a cada período.",
        )

    with cfg_col_2:
        from datetime import date

        data_inicial_str = ranking_config.get("datainicial", "")
        try:
            data_inicial_valor = datetime.strptime(data_inicial_str, "%d/%m/%Y").date() if data_inicial_str else date.today()
        except ValueError:
            data_inicial_valor = date.today()

        ranking_data_inicial = st.date_input(
            "Data inicial do ranking",
            value=data_inicial_valor,
            format="DD/MM/YYYY",
            key="ranking_data_inicial_input",
            help="Kills e eventos anteriores a esta data são ignorados no ranking.",
        )

        janela_opcoes = ["temporada", "semanal", "mensal"]
        janela_labels = {
            "temporada": "Temporada manual",
            "semanal": "Semanal (reinicia toda segunda)",
            "mensal": "Mensal (reinicia todo dia 1)",
        }
        janela_atual = ranking_config.get("tipojanela", "temporada")
        janela_idx = janela_opcoes.index(janela_atual) if janela_atual in janela_opcoes else 0

        ranking_janela = st.selectbox(
            "Tipo de janela",
            options=janela_opcoes,
            index=janela_idx,
            format_func=lambda x: janela_labels[x],
            key="ranking_janela_select",
            disabled=(ranking_modo == "cumulativo"),
            help="Somente aplicável no modo por janela.",
        )

        ranking_reprocessamento = st.toggle(
            "Permitir reprocessamento manual",
            value=ranking_config.get("permitirreprocessamento", True),
            key="ranking_reprocessamento_toggle",
            help="Permite que o sistema reprocesse o ranking com base nos logs existentes ao salvar.",
        )

    st.divider()

    if st.button("Salvar Configurações do Ranking", use_container_width=True, key="salvar_ranking_config"):
        agora = get_hora_brasilia().strftime("%d/%m/%Y %H:%M")

        client_data_rk["rankingconfig"] = {
            "ativo": ranking_ativo,
            "datainicial": ranking_data_inicial.strftime("%d/%m/%Y"),
            "modoexibicao": ranking_modo,
            "tipojanela": ranking_janela,
            "permitirreprocessamento": ranking_reprocessamento,
            "ultimareconfiguracao": agora,
        }

        clients_data_rk[server_id] = client_data_rk
        save_db(DB_CLIENTS, clients_data_rk)
        st.session_state.db_clients = clients_data_rk

        st.success(f"Configurações do ranking salvas com sucesso em {agora}")
        st.rerun()

    st.markdown("### Status Atual")
    status_col_1, status_col_2, status_col_3 = st.columns(3)

    with status_col_1:
        st.metric("Status", "Ativo" if ranking_config.get("ativo", True) else "Pausado")

    with status_col_2:
        st.metric("Última atualização", ranking_stats.get("ultimaatualizacao", "Nunca") or "Nunca")

    with status_col_3:
        st.metric("Período atual", ranking_stats.get("periodoatual", "-") or "-")

    ultima_reconfiguracao = ranking_config.get("ultimareconfiguracao", "")
    if ultima_reconfiguracao:
        st.caption(f"Última reconfiguração: {ultima_reconfiguracao}")

    st.divider()

    st.markdown("### Ranking Atual")

    modo_visualizacao = ranking_config.get("modoexibicao", "cumulativo")
    if modo_visualizacao == "cumulativo":
        dados_ranking = ranking_stats.get("acumulado", {})
        st.caption("Exibindo dados cumulativos desde a data inicial.")
    else:
        janela_visualizacao = ranking_config.get("tipojanela", "temporada")
        janela_labels_resumo = {
            "temporada": "Temporada manual",
            "semanal": "Semanal",
            "mensal": "Mensal",
        }
        dados_ranking = ranking_stats.get(janela_visualizacao, ranking_stats.get("acumulado", {}))
        st.caption(f"Exibindo dados da janela: {janela_labels_resumo.get(janela_visualizacao, janela_visualizacao)}.")

    if dados_ranking:
        ranking_list = []

        for jogador, stats in dados_ranking.items():
            if isinstance(stats, dict):
                kills = stats.get("kills", 0)
                mortes = stats.get("deaths", 0)
                xp = stats.get("xp", 0)
                kd = round(kills / max(mortes, 1), 2)

                ranking_list.append({
                    "Jogador": jogador,
                    "Kills": kills,
                    "Mortes": mortes,
                    "KD": kd,
                    "XP": xp,
                })
            else:
                ranking_list.append({
                    "Jogador": jogador,
                    "Kills": int(stats),
                    "Mortes": 0,
                    "KD": 0.0,
                    "XP": 0,
                })

        ranking_list.sort(key=lambda x: x["Kills"], reverse=True)

        for posicao, row in enumerate(ranking_list, start=1):
            row["#"] = posicao

        df_ranking = pd.DataFrame(ranking_list)[["#", "Jogador", "Kills", "Mortes", "KD", "XP"]]
        st.dataframe(df_ranking, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum dado de ranking disponível para o período configurado. Os dados são gerados automaticamente durante o processamento dos logs.")

with tab8:
    st.subheader("🏦 Banco & Carteira")

    st.info(
        "Gerencie aqui o saldo de DzCoins dos jogadores: carteira (com o jogador) e banco (guardado). "
        "Use esta tela para bônus de evento, correções e ajustes manuais."
    )

    # 1) Garante que há um servidor válido na sessão
    user_key = st.session_state.get("user_key", "")
    nitrado_id = st.session_state.db_users.get("keys", {}).get(user_key, {}).get("server_id", "")
    server_id = user_key
    if not server_id:
        st.error(
            "Nenhum servidor vinculado a este login.\n"
            "Faça login com uma KeyUser válida (gerada no painel de administração)."
        )
        st.stop()

    # 2) Carrega dados do servidor
    clients_data = load_db(DB_CLIENTS, {})
    client_data = clients_data.get(server_id, {})
    
    # Garante estruturas básicas
    players = client_data.get("players", {})
    if "wallets" not in client_data:
        client_data["wallets"] = {}
    if "bank" not in client_data:
        client_data["bank"] = {}

    wallets = client_data["wallets"]
    bank = client_data["bank"]

    # --- Configuração do Worker DzCoins ---
    st.markdown("### ⚙️ Configuração de Ganho Automático de DzCoins")
    st.caption(
        "Os jogadores ganham DzCoins automaticamente enquanto estão online. "
        "Configure abaixo a quantidade e o intervalo de distribuição."
    )

    dz_config = client_data.get("dzcoins_config", {})

    col_dz1, col_dz2, col_dz3 = st.columns(3)

    with col_dz1:
        dz_ativo = st.toggle(
            "✅ Ativar distribuição automática",
            value=dz_config.get("ativo", False),
            key="dzcoins_ativo_toggle",
        )

    with col_dz2:
        dz_quantidade = st.number_input(
            "💰 DzCoins por intervalo",
            min_value=1,
            max_value=10000,
            value=int(dz_config.get("quantidade_dzcoins", 10)),
            step=1,
            help="Quantidade de DzCoins distribuída para cada jogador online a cada intervalo.",
            key="dzcoins_quantidade_input",
        )

    with col_dz3:
        dz_intervalo = st.number_input(
            "⏱️ Intervalo (minutos)",
            min_value=1,
            max_value=1440,
            value=int(dz_config.get("intervalo_minutos", 60)),
            step=1,
            help="A cada quantos minutos os DzCoins serão distribuídos para jogadores online.",
            key="dzcoins_intervalo_input",
        )

    # Preview do ganho estimado
    ganho_hora = round((60 / max(dz_intervalo, 1)) * dz_quantidade, 2)
    ganho_dia = round(ganho_hora * 24, 2)

    st.markdown(
        f"""
        <div style="
            background:#0f1b12;
            border:1px solid #1f5a34;
            border-radius:8px;
            padding:12px 16px;
            margin:10px 0 16px 0;
            font-size:13px;
            color:#d6e2f0;
        ">
            📊 <b style="color:#57ff9a;">Estimativa de ganho:</b>
            &nbsp;&nbsp;
            <b style="color:#ffffff;">{ganho_hora}</b>
            <span style="color:#aaa;"> DzCoins/hora</span>
            &nbsp;&nbsp;|&nbsp;&nbsp;
            <b style="color:#ffffff;">{ganho_dia}</b>
            <span style="color:#aaa;"> DzCoins/dia</span>
            &nbsp;&nbsp;
            <span style="color:#666; font-size:11px;">
                (jogador online 100% do tempo)
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.button(
        "💾 Salvar Configuração DzCoins",
        key="salvar_dzcoins_config",
        use_container_width=True,
        type="primary",
    ):
        client_data["dzcoins_config"] = {
            "ativo": dz_ativo,
            "quantidade_dzcoins": int(dz_quantidade),
            "intervalo_minutos": int(dz_intervalo),
        }
        clients_data[server_id] = client_data
        save_db(DB_CLIENTS, clients_data)
        registrar_log(
            user_id,
            f"DzCoins automático: {'ativado' if dz_ativo else 'desativado'} — "
            f"{dz_quantidade} DzCoins a cada {dz_intervalo} min.",
            "sucesso",
        )
        st.success(
            f"✅ Configuração salva! "
            f"{'Sistema ativado' if dz_ativo else 'Sistema desativado'}. "
            f"Jogadores ganharão {dz_quantidade} DzCoins a cada {dz_intervalo} minuto(s)."
        )
        st.rerun()

    # Status atual do worker
    if dz_config.get("ativo", False):
        st.markdown(
            f"""
            <div style="
                background:#0f1b12;
                border:1px solid #1f5a34;
                border-radius:8px;
                padding:10px 14px;
                margin-bottom:16px;
                font-size:12px;
                color:#57ff9a;
            ">
                🟢 <b>Worker DzCoins ativo</b> —
                distribuindo <b>{dz_config.get('quantidade_dzcoins', 0)} DzCoins</b>
                a cada <b>{dz_config.get('intervalo_minutos', 60)} minutos</b>
                para jogadores online.
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
            <div style="
                background:#1b1b1b;
                border:1px solid #444;
                border-radius:8px;
                padding:10px 14px;
                margin-bottom:16px;
                font-size:12px;
                color:#888;
            ">
                🔴 <b>Worker DzCoins desativado</b> —
                nenhum DzCoin está sendo distribuído automaticamente.
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.divider()

    # 3) Selecionar jogador
    st.markdown("### 👤 Selecionar jogador")

    if not players:
        st.warning("Nenhum jogador vinculado ainda. Use a aba 'Jogadores / Vínculos'.")

    lista_gamertags = sorted(players.keys())
    gamertag_sel = st.selectbox("Jogador", lista_gamertags)

    # 4) Recuperar ou criar registros de carteira/banco
    wallet_reg = wallets.get(gamertag_sel, {"balance": 0, "historico": []})
    bank_reg = bank.get(gamertag_sel, {"balance": 0, "historico": []})

    saldo_carteira = wallet_reg.get("balance", 0)
    saldo_banco = bank_reg.get("balance", 0)

    st.markdown("### 💰 Saldos atuais")
    st.info(
        f"Jogador **{gamertag_sel}**\n\n"
        f"- Carteira: **{saldo_carteira} DzCoins**\n"
        f"- Banco: **{saldo_banco} DzCoins**"
    )

    # 5) Ajustes manuais (admin do servidor)
    st.markdown("### 🛠 Ajustes manuais (admin)")

    col_aj_cart, col_aj_bank = st.columns(2)

    with col_aj_cart:
        st.markdown("#### Carteira")
        val_aj_cart = st.number_input(
            "Ajuste na carteira (+ crédito, - débito)",
            key="ajuste_carteira",
            step=100,
            value=0,
        )
        motivo_cart = st.text_input(
            "Motivo (ex.: bônus evento, correção)", key="motivo_carteira"
        )
        if st.button("Aplicar ajuste na carteira", use_container_width=True):
            if val_aj_cart == 0:
                st.error("Informe um valor diferente de zero para ajustar.")
            else:
                saldo_novo = saldo_carteira + val_aj_cart
                wallet_reg["balance"] = saldo_novo

                hora = get_hora_brasilia().strftime("%d/%m/%Y %H:%M")
                if val_aj_cart > 0:
                    msg = f"[{hora}] AJUSTE +{val_aj_cart} (CARTEIRA) - {motivo_cart or 'sem motivo'}"
                else:
                    msg = f"[{hora}] AJUSTE {val_aj_cart} (CARTEIRA) - {motivo_cart or 'sem motivo'}"

                wallet_reg.setdefault("historico", []).append(msg)

                wallets[gamertag_sel] = wallet_reg
                client_data["wallets"] = wallets
                clients_data[server_id] = client_data
                save_db(DB_CLIENTS, clients_data)

                st.success(f"Ajuste aplicado. Novo saldo em carteira: {saldo_novo} DzCoins.")

                saldo_carteira = saldo_novo
                wallet_reg["balance"] = saldo_novo
                wallets[gamertag_sel] = wallet_reg
                client_data["wallets"] = wallets
                clients_data[server_id] = client_data

    with col_aj_bank:
        st.markdown("#### Banco")
        val_aj_bank = st.number_input(
            "Ajuste no banco (+ crédito, - débito)",
            key="ajuste_banco",
            step=100,
            value=0,
        )
        motivo_bank = st.text_input(
            "Motivo (ex.: prêmio, correção)", key="motivo_banco"
        )
        if st.button("Aplicar ajuste no banco", use_container_width=True):
            if val_aj_bank == 0:
                st.error("Informe um valor diferente de zero para ajustar.")
            else:
                saldo_novo = saldo_banco + val_aj_bank
                bank_reg["balance"] = saldo_novo

                hora = get_hora_brasilia().strftime("%d/%m/%Y %H:%M")
                if val_aj_bank > 0:
                    msg = f"[{hora}] AJUSTE +{val_aj_bank} (BANCO) - {motivo_bank or 'sem motivo'}"
                else:
                    msg = f"[{hora}] AJUSTE {val_aj_bank} (BANCO) - {motivo_bank or 'sem motivo'}"

                bank_reg.setdefault("historico", []).append(msg)

                bank[gamertag_sel] = bank_reg
                client_data["bank"] = bank
                clients_data[server_id] = client_data
                save_db(DB_CLIENTS, clients_data)

                st.success(f"Ajuste aplicado. Novo saldo no banco: {saldo_novo} DzCoins.")

                saldo_banco = saldo_novo
                bank_reg["balance"] = saldo_novo
                bank[gamertag_sel] = bank_reg
                client_data["bank"] = bank
                clients_data[server_id] = client_data

    # 6) Histórico consolidado
    st.markdown("### 📜 Histórico de movimentações")

    col_hist_1, col_hist_2 = st.columns([3, 1])

    with col_hist_2:
        if st.button(
            "🧹 Limpar Histórico",
            key=f"limpar_historico_console_{gamertag_sel}",
            use_container_width=True,
        ):
            wallet_reg["historico"] = []
            bank_reg["historico"] = []

            wallets[gamertag_sel] = wallet_reg
            bank[gamertag_sel] = bank_reg
            client_data["wallets"] = wallets
            client_data["bank"] = bank
            clients_data[server_id] = client_data
            save_db(DB_CLIENTS, clients_data)

            st.success("✅ Histórico visual limpo com sucesso.")
            st.rerun()

    historico_comb = []

    for linha in wallet_reg.get("historico", []):
        historico_comb.append(("CARTEIRA", linha))

    for linha in bank_reg.get("historico", []):
        historico_comb.append(("BANCO", linha))

    st.caption("As movimentações mais recentes ficam visíveis neste console com rolagem.")

    if not historico_comb:
        historico_txt = "Nenhuma movimentação registrada ainda."
    else:
        linhas_console = []
        for origem, linha in reversed(historico_comb[-200:]):
            icone = "💰" if origem == "CARTEIRA" else "🏦"
            linhas_console.append(f"{icone} [{origem}] {linha}")
        historico_txt = "\n".join(linhas_console)

    st.markdown(
        f"""
        <div style="
            background:#0f1117;
            border:1px solid #2b3240;
            border-radius:8px;
            padding:12px;
            height:320px;
            overflow-y:auto;
            font-family:Consolas, 'Courier New', monospace;
            font-size:12px;
            color:#d6e2f0;
            white-space:pre-wrap;
            line-height:1.5;
            margin-bottom:12px;
        ">{historico_txt}</div>
        """,
        unsafe_allow_html=True,
    )

with tab_feeds:
    st.header("🔔 Feeds & Webhooks")
    st.caption("Configure os canais do Discord e escolha quais eventos cada canal vai receber.")

    feeds = client_data.get("feeds_config", {})

    # ================================================================
    # SEÇÃO 1 — Gestão de Webhooks Dinâmicos
    # ================================================================
    st.markdown("### 📡 Meus Webhooks")

    webhooks_cfg = client_data.get("webhooks_config", [])

    # --- Formulário para adicionar novo webhook ---
    with st.expander("➕ Adicionar novo Webhook", expanded=False):
        col_wh1, col_wh2 = st.columns([1, 2])

        with col_wh1:
            novo_wh_nome = st.text_input(
                "Nome do canal",
                placeholder="Ex: #kills-pvp, #loja, #staff",
                key="novo_wh_nome",
            )

        with col_wh2:
            novo_wh_url = st.text_input(
                "URL do Webhook",
                placeholder="https://discord.com/api/webhooks/...",
                key="novo_wh_url",
            )

        st.markdown("**Eventos que este webhook vai receber:**")

        # Agrupa eventos por categoria
        categorias_eventos = {}
        for chave, info in WEBHOOK_EVENTOS_DISPONIVEIS.items():
            cat = info["categoria"]
            if cat not in categorias_eventos:
                categorias_eventos[cat] = []
            categorias_eventos[cat].append((chave, info["label"]))

        novos_eventos_sel = []
        cols_cats = st.columns(len(categorias_eventos))

        for idx, (cat, eventos) in enumerate(categorias_eventos.items()):
            with cols_cats[idx]:
                st.markdown(f"**{cat.upper()}**")
                for chave, label in eventos:
                    if st.checkbox(
                        label,
                        key=f"novo_wh_evt_{chave}",
                        value=False,
                    ):
                        novos_eventos_sel.append(chave)

        col_btn1, col_btn2 = st.columns([1, 3])
        with col_btn1:
            if st.button(
                "💾 Adicionar Webhook",
                key="btn_adicionar_webhook",
                use_container_width=True,
                type="primary",
            ):
                if not novo_wh_nome.strip():
                    st.error("❌ Informe um nome para o webhook.")
                elif not novo_wh_url.strip().startswith("https://discord.com/api/webhooks/"):
                    st.error("❌ URL inválida. Use uma URL de webhook do Discord.")
                elif not novos_eventos_sel:
                    st.error("❌ Selecione pelo menos um evento.")
                else:
                    novo_wh = {
                        "id": str(int(datetime.now(FUSO_BR).timestamp())),
                        "nome": novo_wh_nome.strip(),
                        "url": novo_wh_url.strip(),
                        "eventos": novos_eventos_sel,
                        "ativo": True,
                        "criado_em": datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M"),
                    }
                    webhooks_cfg.append(novo_wh)
                    client_data["webhooks_config"] = webhooks_cfg
                    clients_data[server_id] = client_data
                    save_db(DB_CLIENTS, clients_data)
                    registrar_log(
                        user_id,
                        f"Webhook '{novo_wh_nome}' adicionado com {len(novos_eventos_sel)} evento(s).",
                        "sucesso",
                    )
                    st.success(f"✅ Webhook '{novo_wh_nome}' adicionado com sucesso!")
                    st.rerun()

    st.divider()

    # --- Lista de webhooks cadastrados ---
    if not webhooks_cfg:
        st.info("Nenhum webhook cadastrado ainda. Clique em '➕ Adicionar novo Webhook' para começar.")
    else:
        st.markdown(f"**{len(webhooks_cfg)} webhook(s) configurado(s):**")

        for idx, wh in enumerate(webhooks_cfg):
            ativo = wh.get("ativo", True)
            cor_status = "#57ff9a" if ativo else "#ff6b6b"
            status_txt = "🟢 Ativo" if ativo else "🔴 Pausado"
            eventos_wh = wh.get("eventos", [])
            labels_eventos = [
                WEBHOOK_EVENTOS_DISPONIVEIS.get(e, {}).get("label", e)
                for e in eventos_wh
            ]

            with st.expander(
                f"{status_txt} — {wh.get('nome', 'Sem nome')} "
                f"| {len(eventos_wh)} evento(s)",
                expanded=False,
            ):
                col_info, col_acoes = st.columns([3, 1])

                with col_info:
                    st.markdown(
                        f"""
                        <div style="
                            background:#1a1a2e;
                            border-radius:8px;
                            padding:12px;
                            font-size:12px;
                            color:#d6e2f0;
                            margin-bottom:10px;
                        ">
                            <b style="color:#ffffff;">URL:</b>
                            <span style="color:#888;">
                                {wh.get('url', '')[:60]}...
                            </span><br>
                            <b style="color:#ffffff;">Criado em:</b>
                            <span style="color:#888;">
                                {wh.get('criado_em', '---')}
                            </span><br>
                            <b style="color:#ffffff;">Eventos:</b><br>
                            {"<br>".join(f"&nbsp;&nbsp;• {l}" for l in labels_eventos)}
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                with col_acoes:
                    # Toggle ativo/pausado
                    novo_status = st.toggle(
                        "Ativo",
                        value=ativo,
                        key=f"wh_toggle_{wh['id']}",
                    )
                    if novo_status != ativo:
                        webhooks_cfg[idx]["ativo"] = novo_status
                        client_data["webhooks_config"] = webhooks_cfg
                        clients_data[server_id] = client_data
                        save_db(DB_CLIENTS, clients_data)
                        st.rerun()

                    # Botão de teste
                    if st.button(
                        "🧪 Testar",
                        key=f"wh_test_{wh['id']}",
                        use_container_width=True,
                    ):
                        try:
                            test_payload = {
                                "embeds": [{
                                    "title": "🧪 Teste de Webhook",
                                    "description": (
                                        f"Webhook **{wh.get('nome')}** "
                                        f"está funcionando corretamente!"
                                    ),
                                    "color": 0x00D4FF,
                                    "footer": {
                                        "text": "Titan Cloud PRO • Teste de Conectividade"
                                    },
                                    "timestamp": datetime.now(FUSO_BR).isoformat(),
                                }]
                            }
                            resp = requests.post(
                                wh["url"],
                                json=test_payload,
                                timeout=5,
                            )
                            if resp.status_code in (200, 204):
                                st.success("✅ Enviado!")
                            else:
                                st.error(f"❌ Status {resp.status_code}")
                        except Exception as e:
                            st.error(f"❌ Erro: {e}")

                    # Botão de remover
                    if st.button(
                        "🗑️ Remover",
                        key=f"wh_del_{wh['id']}",
                        use_container_width=True,
                        type="primary",
                    ):
                        webhooks_cfg.pop(idx)
                        client_data["webhooks_config"] = webhooks_cfg
                        clients_data[server_id] = client_data
                        save_db(DB_CLIENTS, clients_data)
                        registrar_log(
                            user_id,
                            f"Webhook '{wh.get('nome')}' removido.",
                            "info",
                        )
                        st.success(f"✅ Webhook removido.")
                        st.rerun()

    st.divider()

    # ================================================================
    # SEÇÃO 2 — Configurações de Auditoria (feeds existentes)
    # ================================================================
    st.markdown("### 🛡️ Auditoria & Analytics")

    cf1, cf2 = st.columns(2)

    with cf1:
        st.subheader("Anti-Glitch")
        feeds["glitch_subsolo"] = st.toggle(
            "Glitch Subsolo",
            value=feeds.get("glitch_subsolo", True),
        )
        feeds["glitch_fogueiras"] = st.toggle(
            "Spam de Fogueiras",
            value=feeds.get("glitch_fogueiras", True),
        )
        feeds["glitch_hortas"] = st.toggle(
            "Spam de Hortas",
            value=feeds.get("glitch_hortas", True),
        )

    with cf2:
        st.subheader("Analytics")
        feeds["mapa_calor"] = st.toggle(
            "Mapa de Calor",
            value=feeds.get("mapa_calor", True),
        )
        feeds["ranking_auto"] = st.toggle(
            "Ranking Global",
            value=feeds.get("ranking_auto", True),
        )

    st.divider()

    if st.button(
        "💾 Salvar Configurações de Auditoria",
        key="salvar_feeds_auditoria",
        use_container_width=True,
    ):
        client_data["feeds_config"] = feeds
        clients_data[server_id] = client_data
        save_db(DB_CLIENTS, clients_data)
        st.success("✅ Configurações de auditoria salvas!")
    
    # --- ABA PLANOS ---
with tab_planos:
    starter_border = "2px solid #aaaaaa" if plano_atual == "Starter" else "1px solid #444"
    starter_badge = '<div style="position:absolute; top:-12px; left:50%; transform:translateX(-50%); background:#aaaaaa; color:#000; font-size:11px; font-weight:bold; padding:3px 14px; border-radius:999px;">SEU PLANO</div>' if plano_atual == "Starter" else '<div style="position:absolute; top:-12px; left:50%; transform:translateX(-50%); background:#aaaaaa; color:#000; font-size:11px; font-weight:bold; padding:3px 14px; border-radius:999px;">BÁSICO</div>'
    pro_border = "2px solid #00d4ff" if plano_atual == "Pro" else "1px solid #00d4ff55"
    pro_badge = '<div style="position:absolute; top:-12px; left:50%; transform:translateX(-50%); background:#00d4ff; color:#000; font-size:11px; font-weight:bold; padding:3px 14px; border-radius:999px;">SEU PLANO</div>' if plano_atual == "Pro" else '<div style="position:absolute; top:-12px; left:50%; transform:translateX(-50%); background:#00d4ff; color:#000; font-size:11px; font-weight:bold; padding:3px 14px; border-radius:999px;">MAIS POPULAR</div>'
    enterprise_border = "2px solid #FFD700" if plano_atual == "Enterprise" else "1px solid #FFD70055"
    enterprise_badge = '<div style="position:absolute; top:-12px; left:50%; transform:translateX(-50%); background:#FFD700; color:#000; font-size:11px; font-weight:bold; padding:3px 14px; border-radius:999px;">SEU PLANO</div>' if plano_atual == "Enterprise" else '<div style="position:absolute; top:-12px; left:50%; transform:translateX(-50%); background:#FFD700; color:#000; font-size:11px; font-weight:bold; padding:3px 14px; border-radius:999px;">PREMIUM</div>'
    html = """
        <div style="background:#1a1a2e; border-radius:12px; padding:24px; margin-bottom:20px;">
            <div style="text-align:center; margin-bottom:24px;">
                <div style="font-size:26px; font-weight:bold; color:#00d4ff;">
                    💎 Planos Titan Cloud Pro
                </div>
                <div style="font-size:13px; color:#888; margin-top:6px;">
                    Escolha o plano ideal para o seu servidor DayZ
                </div>
            </div>
            <div style="display:flex; gap:16px; flex-wrap:wrap; justify-content:center;">
                <div style="flex:1; min-width:220px; max-width:300px; background:#1a1a2e; border:{starter_border}; border-radius:10px; padding:20px; text-align:center; position:relative;">
                    {starter_badge}
                    <div style="font-size:28px; margin-top:8px;">🔹</div>
                    <div style="font-size:18px; font-weight:bold; color:#aaaaaa; margin:8px 0 4px;">Starter</div>
                    <div style="font-size:26px; font-weight:bold; color:#ffffff; margin-bottom:4px;">R$ 19,99</div>
                    <div style="font-size:11px; color:#666; margin-bottom:16px;">por mês</div>
                    <hr style="border-color:#333; margin-bottom:16px;">
                    <div style="font-size:12px; color:#d6e2f0; text-align:left; line-height:2;">
                        ✅ 2 agendamentos ativos<br>
                        ✅ Agendamento único/diário/semanal<br>
                        ✅ Painel web básico<br>
                        ✅ Histórico de logs<br>
                        ✅ Backup e restore<br>
                        ✅ Banco e Carteira DzCoins<br>
                        ✅ Ranking Semanal<br>
                        ✅ Transferência DzCoins<br>
                        ✅ Loja Virtual (Trader)<br>
                        ✅ Worker DzCoins automático<br>
                        ✅ Portal do Jogador (Discord)<br>
                        ✅ Players online + reset<br>
                        ✅ Chernarus ou Livonia<br>
                        🔒 Editores XML/JSON<br>
                    </div>
                    <div style="margin-top:16px; background:#333; border-radius:6px; padding:8px; font-size:12px; color:#aaa;">
                        📧 Suporte por E-mail
                    </div>
                </div>
                <div style="flex:1; min-width:220px; max-width:300px; background:#1a1a2e; border:{pro_border}; border-radius:10px; padding:20px; text-align:center; position:relative;">
                    {pro_badge}
                    <div style="font-size:28px; margin-top:8px;">⭐</div>
                    <div style="font-size:18px; font-weight:bold; color:#00d4ff; margin:8px 0 4px;">Pro</div>
                    <div style="font-size:26px; font-weight:bold; color:#ffffff; margin-bottom:4px;">R$ 49,99</div>
                    <div style="font-size:11px; color:#666; margin-bottom:16px;">por mês</div>
                    <hr style="border-color:#333; margin-bottom:16px;">
                    <div style="font-size:12px; color:#d6e2f0; text-align:left; line-height:2;">
                        ✅ 8 agendamentos ativos<br>
                        ✅ Agendamento único/diário/semanal<br>
                        ✅ Painel web completo<br>
                        ✅ Histórico de logs<br>
                        ✅ Backup e restore<br>
                        ✅ Banco e Carteira DzCoins<br>
                        ✅ Loja Virtual (Trader) completa<br>
                        ✅ Worker DzCoins automático<br>
                        ✅ Portal do Jogador (Discord)<br>
                        ✅ Players online + reset<br>
                        ✅ Chernarus ou Livonia<br>
                        ✅ Editores XML/JSON<br>
                        ✅ Ranking Semanal<br>
                        ✅ Transferência DzCoins<br>
                    </div>
                    <div style="margin-top:16px; background:#00d4ff22; border:1px solid #00d4ff44; border-radius:6px; padding:8px; font-size:12px; color:#00d4ff;">
                        📧 Suporte por E-mail
                    </div>
                </div>
                <div style="flex:1; min-width:220px; max-width:300px; background:#1a1a2e; border:{enterprise_border}; border-radius:10px; padding:20px; text-align:center; position:relative;">
                    {enterprise_badge}
                    <div style="font-size:28px; margin-top:8px;">👑</div>
                    <div style="font-size:18px; font-weight:bold; color:#FFD700; margin:8px 0 4px;">Enterprise</div>
                    <div style="font-size:26px; font-weight:bold; color:#ffffff; margin-bottom:4px;">R$ 59,99</div>
                    <div style="font-size:11px; color:#666; margin-bottom:16px;">por mês</div>
                    <hr style="border-color:#333; margin-bottom:16px;">
                    <div style="font-size:12px; color:#d6e2f0; text-align:left; line-height:2;">
                        ✅ 16 agendamentos ativos<br>
                        ✅ Agendamento único/diário/semanal<br>
                        ✅ Painel web completo<br>
                        ✅ Histórico de logs<br>
                        ✅ Backup e restore<br>
                        ✅ Banco e Carteira DzCoins<br>
                        ✅ Loja Virtual (Trader) completa<br>
                        ✅ Worker DzCoins automático<br>
                        ✅ Portal do Jogador (Discord)<br>
                        ✅ Players online + reset<br>
                        ✅ Chernarus ou Livonia<br>
                        ✅ Editores XML/JSON<br>
                        ✅ Ranking Semanal<br>
                        ✅ Transferência DzCoins<br>
                    </div>
                    <div style="margin-top:16px; background:#FFD70022; border:1px solid #FFD70044; border-radius:6px; padding:8px; font-size:12px; color:#FFD700;">
                        🎫 Suporte Ticket Prioritário
                    </div>
                </div>
            </div>
            <div style="text-align:center; font-size:11px; color:#555; margin-top:24px;">
                DzCoins são moedas virtuais fictícias sem valor monetário real.<br>
                Planos renovados mensalmente. Entre em contato para assinar ou fazer upgrade.
            </div>
        </div>
    """.format(starter_border=starter_border, starter_badge=starter_badge, pro_border=pro_border, pro_badge=pro_badge, enterprise_border=enterprise_border, enterprise_badge=enterprise_badge)
    st.markdown(html, unsafe_allow_html=True)

# --- INÍCIO DO WORKER DE AUTOMAÇÃO ---
if "worker_started" not in st.session_state:
    threading.Thread(target=proworker, daemon=True).start()
    st.session_state["worker_started"] = True
