import streamlit as st
import json
import os
import requests
import urllib.parse
import io
import re
import html
import math
from datetime import datetime, timezone, timedelta
from ftplib import FTP
from functools import lru_cache
from pathlib import Path

# Import seguro da função de FTP
try:
    from ftp_utils import enviar_pedidos_via_ftp
except ImportError as e:
    st.error(f"Erro ao importar função enviar_pedidos_via_ftp: {e}")
    enviar_pedidos_via_ftp = None

# =========================================================
# 1. CONFIG / AMBIENTE / CONSTANTES
# =========================================================

IS_DEV = os.environ.get("IS_DEV", "False") == "True"

if os.path.exists("/var/data"):
    DB_USERS = "/var/data/users_db.json"
    DB_CLIENTS = "/var/data/clients_data.json"
else:
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if IS_DEV:
        DB_USERS = os.path.join(BASE_DIR, "users_db_dev.json")
        DB_CLIENTS = os.path.join(BASE_DIR, "clients_data_dev.json")
    else:
        DB_USERS = os.path.join(BASE_DIR, "users_db.json")
        DB_CLIENTS = os.path.join(BASE_DIR, "clients_data.json")

DISCORD_CLIENT_ID = os.environ.get("DISCORD_CLIENT_ID", "")
DISCORD_CLIENT_SECRET = os.environ.get("DISCORD_CLIENT_SECRET", "")
DISCORD_REDIRECT_URI = os.environ.get(
    "DISCORD_REDIRECT_URI",
    "https://titan-cloud-dayz-dev.onrender.com/player_portal",
)
DISCORD_AUTHORIZE_URL = "https://discord.com/api/oauth2/authorize"
DISCORD_TOKEN_URL = "https://discord.com/api/oauth2/token"
DISCORD_API_BASE = "https://discord.com/api"
DISCORD_SCOPE = "identify guilds"

NITRADO_TOKEN = os.environ.get("NITRADO_TOKEN", "")
NITRADO_API = "https://api.nitrado.net"

FUSO_BR = timezone(timedelta(hours=-3))
DAYZ_LOG_DIR = "dayzxb/config"
RESTART_LOG_FILENAME = "restart.log"

# =========================================================
# 1.1 DADOS DE MAPA / ELEVAÇÃO
# =========================================================
MODULE_DIR = Path(__file__).resolve().parent
MAP_DATA_DIR = MODULE_DIR / "map_data"

CHERNARUS_ELEVATION_PY = MAP_DATA_DIR / "chernarus_elevation.py"
CHERNARUS_ZONE_LOOKUP_JSON = MAP_DATA_DIR / "chernarus_zone_lookup.json"
CHERNARUS_KNOWN_POINTS_JSON = MAP_DATA_DIR / "chernarus_known_points.json"
CHERNARUS_HEIGHTMAP_METADATA_JSON = MAP_DATA_DIR / "chernarus_heightmap_metadata.json"
CHERNARUS_HEIGHTMAP_ASC = MAP_DATA_DIR / "terrain_heightmap.asc"
CHERNARUS_HEIGHTMAP_NPY = MAP_DATA_DIR / "chernarus_heightmap.npy"

LIVONIA_ELEVATION_PY = MAP_DATA_DIR / "livonia_elevation.py"
LIVONIA_ZONE_LOOKUP_JSON = MAP_DATA_DIR / "livonia_zone_lookup.json"
LIVONIA_KNOWN_POINTS_JSON = MAP_DATA_DIR / "livonia_known_points.json"
LIVONIA_HEIGHTMAP_METADATA_JSON = MAP_DATA_DIR / "livonia_heightmap_metadata.json"
LIVONIA_HEIGHTMAP_ASC = MAP_DATA_DIR / "enoch_heightmap.asc"
LIVONIA_HEIGHTMAP_NPY = MAP_DATA_DIR / "livonia_heightmap.npy"

# Import seguro do módulo de elevação do Chernarus
try:
    import sys
    if str(MAP_DATA_DIR) not in sys.path:
        sys.path.append(str(MAP_DATA_DIR))
    from chernarus_elevation import ChernarusHeightmap
except Exception as e:
    ChernarusHeightmap = None
    print(f"[Elevation] Módulo de elevação do Chernarus indisponível: {e}")

try:
    import sys
    if str(MAP_DATA_DIR) not in sys.path:
        sys.path.append(str(MAP_DATA_DIR))
    from livonia_elevation import LivoniaHeightmap
except Exception as e:
    LivoniaHeightmap = None
    print(f"[Elevation] Módulo de elevação do Livonia indisponível: {e}")

# =========================================================
# 2. FUNÇÕES DE UTILIDADE E CÁLCULO (NOVO)
# =========================================================

def calcular_distancia_3d(pos1, pos2):
    """
    Calcula a distância euclidiana entre dois pontos no mapa DayZ (X, Y, Z).
    Utilizada para auditoria de proximidade de objetos (raio de 25m).
    """
    return math.sqrt((pos1[0]-pos2[0])**2 + (pos1[1]-pos2[1])**2 + (pos1[2]-pos2[2])**2)

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

# =========================================================
# 2. FUNÇÕES DE PERSISTÊNCIA
# =========================================================

def load_db(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_db(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
    except Exception as e:
        st.error(f"Erro ao salvar dados: {e}")

# =========================================================
# 2.1 FUNÇÕES DE ELEVAÇÃO / MAPA
# =========================================================
@lru_cache(maxsize=1)
def get_chernarus_heightmap():
    """
    Carrega a fonte de elevação do Chernarus uma única vez.
    Prioridade:
    1) terrain_heightmap.asc real
    2) chernarus_heightmap.npy real/preprocessado
    3) lookup JSON aproximado
    4) fallback interno do módulo
    """
    if ChernarusHeightmap is None:
        return None, "modulo_indisponivel"

    try:
        if CHERNARUS_HEIGHTMAP_ASC.exists():
            hm = ChernarusHeightmap.from_asc(str(CHERNARUS_HEIGHTMAP_ASC))
            return hm, "asc_real"

        if CHERNARUS_HEIGHTMAP_NPY.exists():
            hm = ChernarusHeightmap.from_npy(str(CHERNARUS_HEIGHTMAP_NPY))
            return hm, "npy_real"

        if CHERNARUS_ZONE_LOOKUP_JSON.exists():
            hm = ChernarusHeightmap.from_json_lookup(str(CHERNARUS_ZONE_LOOKUP_JSON))
            return hm, "json_lookup"

        hm = ChernarusHeightmap()
        return hm, "fallback_modulo"

    except Exception as e:
        print(f"[Elevation] Erro ao carregar heightmap Chernarus: {e}")
        return None, "erro_carregamento"


def get_chernarus_elevation_local(x: float, z: float):
    """
    Retorna (y, fonte) para Chernarus a partir dos arquivos locais.
    """
    hm, source = get_chernarus_heightmap()
    if hm is None:
        return None, source

    try:
        y = float(hm.get_elevation(float(x), float(z)))
        return round(y, 4), source
    except Exception as e:
        print(f"[Elevation] Erro consultando elevação local ({x}, {z}): {e}")
        return None, "erro_consulta"

@lru_cache(maxsize=1)
def get_livonia_heightmap():
    """
    Carrega a fonte de elevação do Livonia uma única vez.
    Prioridade:
    1) enoch_heightmap.asc real
    2) livonia_heightmap.npy real/preprocessado
    3) lookup JSON aproximado
    4) fallback interno do módulo
    """
    if LivoniaHeightmap is None:
        return None, "modulo_indisponivel"

    try:
        if LIVONIA_HEIGHTMAP_ASC.exists():
            hm = LivoniaHeightmap.from_asc(str(LIVONIA_HEIGHTMAP_ASC))
            return hm, "asc_real"

        if LIVONIA_HEIGHTMAP_NPY.exists():
            hm = LivoniaHeightmap.from_npy(str(LIVONIA_HEIGHTMAP_NPY))
            return hm, "npy_real"

        if LIVONIA_ZONE_LOOKUP_JSON.exists():
            hm = LivoniaHeightmap.from_zone_lookup(str(LIVONIA_ZONE_LOOKUP_JSON))
            return hm, "json_lookup"

        hm = LivoniaHeightmap()
        return hm, "fallback_modulo"

    except Exception as e:
        print(f"[Elevation] Erro ao carregar heightmap Livonia: {e}")
        return None, "erro_carregamento"


def get_livonia_elevation_local(x: float, z: float):
    """
    Retorna y, fonte para Livonia a partir dos arquivos locais.
    """
    hm, source = get_livonia_heightmap()
    if hm is None:
        return None, source

    try:
        y = float(hm.get_elevation(float(x), float(z)))
        return round(y, 4), source
    except Exception as e:
        print(f"[Elevation] Erro consultando elevação local Livonia ({x}, {z}): {e}")
        return None, "erro_consulta"

def get_local_elevation_by_map(x: float, z: float, mapa: str):
    """
    Dispatcher simples por mapa.
    Suporta Chernarus e Livonia/Enoch.
    """
    mapa_norm = (mapa or "").strip().lower()

    if mapa_norm in ("chernarus", "chernarusplus", "chernarus_plus"):
        return get_chernarus_elevation_local(x, z)

    if mapa_norm in ("livonia", "enoch"):
        return get_livonia_elevation_local(x, z)

    return None, "mapa_sem_suporte_local"

def resolver_y_loja(ftp_cfg: dict | None, x: float, z: float, mapa: str):
    """
    Resolve a coordenada Y para compras da loja.

    Ordem de prioridade:
    1) Elevação local por mapa (Chernarus ou Livonia)
    2) FTP / cfgeventspawns.xml
    3) Falha controlada

    Retorna dict com:
    - y: float | None
    - fonte: str
    - distancia: float | None
    - detalhe: str
    """
    # 1) tenta fonte local do mapa
    y_local, fonte_local = get_local_elevation_by_map(x, z, mapa)
    if y_local is not None:
        return {
            "y": round(float(y_local), 4),
            "fonte": f"local:{fonte_local}",
            "distancia": None,
            "detalhe": "Elevação obtida por dados locais do mapa",
        }

    # 2) fallback FTP
    if ftp_cfg:
        try:
            y_ftp, dist_ftp = ftp_buscar_y_por_coordenadas(ftp_cfg, x, z, mapa)
            if y_ftp is not None:
                return {
                    "y": round(float(y_ftp), 4),
                    "fonte": "ftp:cfgeventspawns",
                    "distancia": round(float(dist_ftp), 2),
                    "detalhe": "Elevação obtida por ponto mais próximo do cfgeventspawns.xml",
                }
        except Exception as e:
            print(f"[Elevation] Falha no fallback FTP ({mapa} {x}, {z}): {e}")

    # 3) falha final
    return {
        "y": None,
        "fonte": "indisponivel",
        "distancia": None,
        "detalhe": "Não foi possível resolver a elevação",
    }

# =========================================================
# 3. FUNÇÕES DE DOMÍNIO
# =========================================================

def load_players_for_client(client_data_obj):
    if "players" not in client_data_obj:
        client_data_obj["players"] = {}
    return client_data_obj["players"]

def validar_membro_discord(portal_guilds: list, discord_guild_id: str) -> bool:
    if not discord_guild_id or not portal_guilds:
        return False
    return any(str(g.get("id")) == str(discord_guild_id) for g in portal_guilds)

def trocar_code_por_token(code: str):
    data = {
        "client_id": DISCORD_CLIENT_ID,
        "client_secret": DISCORD_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": DISCORD_REDIRECT_URI,
        "scope": DISCORD_SCOPE,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        token_resp = requests.post(DISCORD_TOKEN_URL, data=data, headers=headers, timeout=10)
    except Exception as e:
        st.error(f"Erro de conexão com Discord: {e}")
        return None

    if token_resp.status_code != 200:
        st.error(f"Erro Discord (status {token_resp.status_code})")
        return None

    access_token = token_resp.json().get("access_token")
    if not access_token:
        return None

    auth_header = {"Authorization": f"Bearer {access_token}"}
    try:
        user_resp = requests.get(f"{DISCORD_API_BASE}/users/@me", headers=auth_header, timeout=10)
        guilds_resp = requests.get(f"{DISCORD_API_BASE}/users/@me/guilds", headers=auth_header, timeout=10)
    except Exception as e:
        st.error(f"Erro ao buscar dados Discord: {e}")
        return None

    if user_resp.status_code != 200:
        return None

    user_info = user_resp.json()
    guilds = guilds_resp.json() if guilds_resp.status_code == 200 else []
    return {
        "discord_id": user_info.get("id"),
        "discord_name": user_info.get("username"),
        "discord_avatar": user_info.get("avatar"),
        "guilds": guilds,
    }

# =========================================================
# 3.1 FUNÇÕES DE PLANO
# =========================================================

PERMISSOES = {
    "transferencia_jogador": ["Pro", "Enterprise", "Admin"],
    "ranking_semanal":       ["Pro", "Enterprise", "Admin"],
}

def plano_permite(plano_atual: str, funcionalidade: str) -> bool:
    return plano_atual in PERMISSOES.get(funcionalidade, [])

def bloquear_funcionalidade(plano_atual: str, funcionalidade_nome: str, plano_minimo: str = "Pro"):
    st.warning(
        f"🔒 **{funcionalidade_nome}** não está disponível no seu plano atual "
        f"(**{plano_atual}**). Esta funcionalidade está disponível a partir do plano "
        f"**{plano_minimo}**. Entre em contato com o suporte para fazer upgrade."
    )

# =========================================================
# 4. FUNÇÕES NITRADO API
# =========================================================

def nitrado_headers():
    return {"Authorization": f"Bearer {NITRADO_TOKEN}"}

def get_players_online(nitrado_id: str, nitrado_token: str = "") -> dict:
    token = nitrado_token or NITRADO_TOKEN
    if not token:
        return {"players": [], "total": 0, "erro": "Token não configurado"}
    try:
        url = f"{NITRADO_API}/services/{nitrado_id}/gameservers"
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            gs = data.get("data", {}).get("gameserver", {})
            query = gs.get("query", {})
            players_raw = query.get("players", [])
            if players_raw and isinstance(players_raw[0], dict):
                nomes = [p.get("name", "?") for p in players_raw]
            else:
                nomes = [str(p) for p in players_raw]
            return {
                "players": nomes,
                "total": query.get("player_current", len(nomes)),
                "max": query.get("player_max", 0),
                "status": gs.get("status", "unknown"),
            }
        else:
            return {"players": [], "total": 0, "erro": f"API status {resp.status_code}"}
    except Exception as e:
        return {"players": [], "total": 0, "erro": str(e)}

# =========================================================
# 4.1 FUNÇÕES FTP + LOG ADM
# =========================================================

def get_client_ftp_config(client_data: dict):
    ftp_cfg = client_data.get("ftp", {})
    host = ftp_cfg.get("host", "")
    user = ftp_cfg.get("user", "")
    pwd = ftp_cfg.get("pass", "")
    port = int(ftp_cfg.get("port", 21) or 21)
    if not host or not user or not pwd:
        return None
    return {"host": host, "user": user, "pass": pwd, "port": port}

def ftp_list_adm_files(ftp_cfg: dict):
    try:
        with FTP() as ftp:
            ftp.connect(ftp_cfg["host"], ftp_cfg["port"], timeout=10)
            ftp.login(ftp_cfg["user"], ftp_cfg["pass"])
            try:
                ftp.cwd(DAYZ_LOG_DIR)
            except Exception:
                return []
            arquivos = ftp.nlst()
            arquivos = [a for a in arquivos if a.upper().endswith(".ADM")]
            arquivos.sort(reverse=True)
            return arquivos
    except Exception:
        return []

def ftp_download_latest_adm(ftp_cfg: dict):
    arquivos = ftp_list_adm_files(ftp_cfg)
    if not arquivos:
        return None, "Nenhum arquivo .ADM encontrado em dayzxb/config."
    ultimo = arquivos[0]
    buffer = io.BytesIO()
    try:
        with FTP() as ftp:
            ftp.connect(ftp_cfg["host"], ftp_cfg["port"], timeout=10)
            ftp.login(ftp_cfg["user"], ftp_cfg["pass"])
            ftp.cwd(DAYZ_LOG_DIR)
            ftp.retrbinary(f"RETR {ultimo}", buffer.write)
        texto = buffer.getvalue().decode("utf-8", errors="ignore")
        return texto, None
    except Exception as e:
        return None, f"Erro ao baixar .ADM: {e}"

def ftp_download_restart_log(ftp_cfg: dict):
    buffer = io.BytesIO()
    try:
        with FTP() as ftp:
            ftp.connect(ftp_cfg["host"], ftp_cfg["port"], timeout=10)
            ftp.login(ftp_cfg["user"], ftp_cfg["pass"])
            files = ftp.nlst()
            if RESTART_LOG_FILENAME not in files:
                return None, f"{RESTART_LOG_FILENAME} não encontrado na raiz do FTP."
            ftp.retrbinary(f"RETR {RESTART_LOG_FILENAME}", buffer.write)
        texto = buffer.getvalue().decode("utf-8", errors="ignore")
        return texto, None
    except Exception as e:
        return None, f"Erro ao baixar {RESTART_LOG_FILENAME}: {e}"

def ftp_buscar_y_por_coordenadas(ftp_cfg: dict, x: float, z: float, mapa: str = "Chernarus") -> tuple[float | None, float]:
    """
    Baixa o cfgeventspawns.xml via FTP, varre todos os pontos com y definido
    e retorna o y do ponto mais próximo + a distância em metros.
    Retorna (y, distancia) ou (None, 0) se não encontrar.
    """
    import math
    import xml.etree.ElementTree as ET

    # Caminho baseado no mapa
    if mapa == "Livonia":
        caminho = "dayzxb_missions/dayzOffline.enoch"
    else:
        caminho = "dayzxb_missions/dayzOffline.chernarusplus"

    arquivo = "cfgeventspawns.xml"
    buffer = io.BytesIO()

    try:
        with FTP() as ftp:
            ftp.connect(ftp_cfg["host"], ftp_cfg["port"], timeout=10)
            ftp.login(ftp_cfg["user"], ftp_cfg["pass"])
            ftp.cwd(caminho)
            ftp.retrbinary(f"RETR {arquivo}", buffer.write)
    except Exception as e:
        print(f"[Y-Finder] Erro ao baixar {arquivo}: {e}")
        return None, 0.0

    try:
        conteudo = buffer.getvalue().decode("utf-8", errors="ignore")
        root = ET.fromstring(conteudo)
    except Exception as e:
        print(f"[Y-Finder] Erro ao parsear XML: {e}")
        return None, 0.0

    melhor_y = None
    melhor_dist = float("inf")

    for pos in root.iter("pos"):
        try:
            px = float(pos.get("x", 0))
            py = float(pos.get("y", 0))
            pz = float(pos.get("z", 0))

            # Ignora pontos sem y definido (y == 0 geralmente = não definido)
            if py == 0.0:
                continue

            dist = math.sqrt((px - x) ** 2 + (pz - z) ** 2)
            if dist < melhor_dist:
                melhor_dist = dist
                melhor_y = py
        except Exception:
            continue

    return melhor_y, melhor_dist

def parse_last_restart_from_restart_log(log_text: str):
    if not log_text:
        return None
    last_dt = None
    for line in log_text.splitlines():
        line = line.strip()
        if not line or "Reiniciando o Servidor" not in line:
            continue
        try:
            data_str = line.split("Reiniciando")[0].strip()
            dt = datetime.strptime(data_str, "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=FUSO_BR)
        except Exception:
            continue
        if last_dt is None or dt > last_dt:
            last_dt = dt
    return last_dt

def format_seconds_hhmmss(segundos: int) -> str:
    if segundos < 0:
        segundos = 0
    h = segundos // 3600
    m = (segundos % 3600) // 60
    s = segundos % 60
    if h >= 24:
        dias = h // 24
        h_rest = h % 24
        return f"{dias}d {h_rest:02d}:{m:02d}:{s:02d}"
    return f"{h:02d}:{m:02d}:{s:02d}"

def parse_adm_sessions_and_pve(log_text: str) -> dict:
    """
    Parser do arquivo .ADM do DayZ.
    Extrai sessões de jogo, hits PvE e suicídios de cada jogador.
    Retorna: {"players": {nome: {...stats...}}}
    """
    if not log_text or not log_text.strip():
        return {"players": {}}

    players = {}

    # Regex para linha de player
    re_player_line = re.compile(r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)"')

    # Palavras-chave de eventos
    key_connected     = "is connected"
    key_disconnected  = "has been disconnected"
    key_suicide_emote = "performed EmoteSuicide"
    key_committed_sui = "committed suicide"
    key_hit_infected  = "hit by Infected"

    # Tenta extrair a data do cabeçalho do log
    log_date = None
    for line in log_text.splitlines():
        if "AdminLog started on " in line:
            try:
                parte = line.split("AdminLog started on ")[1]
                data_str = parte.split(" at ")[0].strip()
                log_date = datetime.strptime(data_str, "%Y-%m-%d").date()
            except Exception:
                pass
            break

    if not log_date:
        log_date = datetime.now(FUSO_BR).date()

    # ---- funções auxiliares ----

    def ensure_player(name: str) -> dict:
        if name not in players:
            players[name] = {
                "total_play_seconds": 0,
                "session_count": 0,
                "last_connect": None,
                "last_disconnect": None,
                "last_death_time": None,
                "pve_hits": 0,
                "pve_suicides": 0,
            }
        return players[name]

    def parse_dt(tstr: str):
        try:
            dt = datetime.strptime(f"{log_date} {tstr}", "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=FUSO_BR)
        except Exception:
            return None

    # ---- processamento linha a linha ----

    for line in log_text.splitlines():
        line = line.strip()
        if not line:
            continue

        m = re_player_line.match(line)
        if not m:
            continue

        hora_str = m.group(1)
        nome = m.group(2)
        dt_evento = parse_dt(hora_str)
        p = ensure_player(nome)

        if key_connected in line and "is connecting" not in line:
            p["last_connect"] = dt_evento

        elif key_disconnected in line:
            if p.get("last_connect") and dt_evento:
                delta = (dt_evento - p["last_connect"]).total_seconds()
                if delta > 0:
                    p["total_play_seconds"] += int(delta)
                    p["session_count"] += 1
            p["last_disconnect"] = dt_evento
            p["last_connect"] = None

        if key_suicide_emote in line or key_committed_sui in line:
            p["pve_suicides"] += 1
            p["last_death_time"] = dt_evento

        if key_hit_infected in line:
            p["pve_hits"] += 1

    return {"players": players}

# =========================================================
# 4.2 PARSERS KILLFEED PvE, PvP E CONEXÃO
# =========================================================

def parse_adm_killfeed_pve(log_text: str) -> list:
    """
    Extrai eventos de morte PvE do log .ADM.
    Retorna lista de dicts com os eventos ordenados do mais recente.
    Captura:
    - Hits por Infected (dano real > 0)
    - Mortes por suicídio / EmoteSuicide
    - Mortes com died. Stats
    """
    eventos = []

    if not log_text or not log_text.strip():
        return eventos

    # Data do log
    log_date = None
    for line in log_text.splitlines():
        if "AdminLog started on " in line:
            try:
                parte = line.split("AdminLog started on ")[1]
                data_str = parte.split(" at ")[0].strip()
                log_date = datetime.strptime(data_str, "%Y-%m-%d").date()
            except Exception:
                pass
            break
    if not log_date:
        log_date = datetime.now(FUSO_BR).date()

    # Regex (sem escape duplo no arquivo real)
    re_hit_infected = re.compile(
        r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)" '
        r'\([^)]*\)\[HP: ([\d.]+)\] hit by Infected into (\w+)\(\d+\) '
        r'for ([\d.]+) damage \((\w+)\)'
    )
    re_suicide_emote = re.compile(
        r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)" '
        r'\([^)]*\) performed EmoteSuicide with (.+)$'
    )
    re_died = re.compile(
        r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)" \(DEAD\) '
        r'\([^)]*pos=<([\d., -]+)>\) died\. Stats> Water: ([\d.]+) Energy: ([\d.]+)'
    )
    re_committed_suicide = re.compile(
        r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)" \(DEAD\) '
        r'\([^)]*\) committed suicide'
    )

    ultima_arma_suicidio = {}
    ultima_pos = {}

    re_pos = re.compile(r'pos=<([\d., -]+)>')

    def parse_dt(tstr):
        try:
            dt = datetime.strptime(f"{log_date} {tstr}", "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=FUSO_BR)
        except Exception:
            return None

    for line in log_text.splitlines():
        line = line.strip()
        if not line:
            continue

        m_pos = re_pos.search(line)
        m_nome = re.search(r'Player "([^"]+)"', line)
        if m_pos and m_nome:
            ultima_pos[m_nome.group(1)] = m_pos.group(1)

        # Hit por Infected (só dano real > 0)
        m = re_hit_infected.match(line)
        if m:
            hora, nome, hp, parte_corpo, dano, tipo = (
                m.group(1), m.group(2), float(m.group(3)),
                m.group(4), float(m.group(5)), m.group(6)
            )
            if dano > 0:
                eventos.append({
                    "tipo": "hit_pve",
                    "hora": hora,
                    "dt": parse_dt(hora),
                    "jogador": nome,
                    "hp_restante": hp,
                    "parte_corpo": parte_corpo,
                    "dano": dano,
                    "tipo_ataque": tipo,
                    "posicao": ultima_pos.get(nome, ""),
                    "icone": "🧟",
                    "descricao": f"Atacado por Zumbi — {dano:.1f} dano em {parte_corpo} (HP: {hp:.1f})",
                })
            continue

        # EmoteSuicide — rastreia arma
        m = re_suicide_emote.match(line)
        if m:
            hora, nome, arma = m.group(1), m.group(2), m.group(3)
            ultima_arma_suicidio[nome] = arma.strip()
            continue

        # Died (morte real)
        m = re_died.match(line)
        if m:
            hora, nome, pos, water, energy = (
                m.group(1), m.group(2), m.group(3),
                float(m.group(4)), float(m.group(5))
            )
            arma = ultima_arma_suicidio.get(nome, "Desconhecida")
            eventos.append({
                "tipo": "morte_pve",
                "hora": hora,
                "dt": parse_dt(hora),
                "jogador": nome,
                "posicao": pos,
                "water": water,
                "energy": energy,
                "arma": arma,
                "icone": "💀",
                "descricao": f"Morreu — Água: {water:.0f} | Energia: {energy:.0f} | Arma: {arma}",
            })
            ultima_arma_suicidio.pop(nome, None)
            continue

    eventos.sort(key=lambda x: x.get("dt") or datetime.min.replace(tzinfo=FUSO_BR), reverse=True)
    return eventos


def parse_adm_conexoes(log_text: str, feeds_config: dict = None) -> list:
    """
    Extrai eventos de conexão e desconexão do log .ADM.
    Retorna lista de dicts ordenada do mais recente.
    feeds_config: dicionário com as permissões de exibição (ex: coordenadas_killfeed).
    """
    eventos = []

    if not log_text or not log_text.strip():
        return eventos

    # Determina se as coordenadas devem ser exibidas
    exibir_coords = feeds_config.get("coordenadas_killfeed", True) if feeds_config else True[cite: 7]

    log_date = None
    for line in log_text.splitlines():
        if "AdminLog started on " in line:
            try:
                parte = line.split("AdminLog started on ")[1]
                data_str = parte.split(" at ")[0].strip()
                log_date = datetime.strptime(data_str, "%Y-%m-%d").date()
            except Exception:
                pass
            break
    if not log_date:
        log_date = datetime.now(FUSO_BR).date()

    re_connecting   = re.compile(r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)" \([^)]*\) is connecting')
    re_connected    = re.compile(r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)" \([^)]*pos=<([^>]+)>\) is connected')
    re_disconnected = re.compile(r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)" \([^)]*pos=<([^>]+)>\) has been disconnected')

    hora_connect = {}

    def parse_dt(tstr):
        try:
            dt = datetime.strptime(f"{log_date} {tstr}", "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=FUSO_BR)
        except Exception:
            return None

    for line in log_text.splitlines():
        line = line.strip()
        if not line:
            continue

        m = re_connecting.match(line)
        if m:
            hora, nome = m.group(1), m.group(2)
            eventos.append({
                "tipo": "connecting",
                "hora": hora,
                "dt": parse_dt(hora),
                "jogador": nome,
                "posicao": "",
                "duracao": "",
                "icone": "🔄",
                "descricao": "Conectando...",
            })
            continue

        m = re_connected.match(line)
        if m:
            hora, nome, pos = m.group(1), m.group(2), m.group(3)
            hora_connect[nome] = parse_dt(hora)
            
            # Filtro de Governança aplicado à descrição
            desc = f"Conectou em {pos}" if exibir_coords else "Conectou ao servidor"
            
            eventos.append({
                "tipo": "connected",
                "hora": hora,
                "dt": parse_dt(hora),
                "jogador": nome,
                "posicao": pos if exibir_coords else "",
                "duracao": "",
                "icone": "🟢",
                "descricao": desc,
            })
            continue

        m = re_disconnected.match(line)
        if m:
            hora, nome, pos = m.group(1), m.group(2), m.group(3)
            dt_disc = parse_dt(hora)
            duracao = ""
            if nome in hora_connect and dt_disc and hora_connect[nome]:
                delta = int((dt_disc - hora_connect[nome]).total_seconds())
                duracao = format_seconds_hhmmss(delta)
                hora_connect.pop(nome, None)
            
            # Filtro de Governança aplicado à descrição[cite: 7]
            local_txt = f" de {pos}" if exibir_coords else ""
            sessao_txt = f" — Sessão: {duracao}" if duracao else ""
            
            eventos.append({
                "tipo": "disconnected",
                "hora": hora,
                "dt": dt_disc,
                "jogador": nome,
                "posicao": pos if exibir_coords else "",
                "duracao": duracao,
                "icone": "🔴",
                "descricao": f"Desconectou{local_txt}{sessao_txt}",
            })
            continue

    eventos.sort(key=lambda x: x.get("dt") or datetime.min.replace(tzinfo=FUSO_BR), reverse=True)
    return eventos


def parse_adm_killfeed_pvp(log_text: str, feeds_config: dict = None) -> list:
    """
    Extrai eventos de kill PvP do log .ADM.
    feeds_config: dicionário com as permissões de exibição.
    """
    eventos = []

    if not log_text or not log_text.strip():
        return eventos

    # Determina se as coordenadas devem ser ocultadas (GRC)
    exibir_coords = feeds_config.get("coordenadas_killfeed", True) if feeds_config else True

    log_date = None
    for line in log_text.splitlines():
        if "AdminLog started on " in line:
            try:
                parte = line.split("AdminLog started on ")[1]
                data_str = parte.split(" at ")[0].strip()
                log_date = datetime.strptime(data_str, "%Y-%m-%d").date()
            except Exception:
                pass
            break
    if not log_date:
        log_date = datetime.now(FUSO_BR).date()

    def parse_dt(tstr):
        try:
            dt = datetime.strptime(f"{log_date} {tstr}", "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=FUSO_BR)
        except Exception:
            return None

    re_killed = re.compile(
        r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)" \([^)]*\) killed Player "([^"]+)"'
    )
    re_hit_pvp = re.compile(
        r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)" \([^)]*\)\[HP: 0\] '
        r'hit by Player "([^"]+)" .* for ([\d.]+) damage \(([^)]+)\)'
    )
    re_died_pvp = re.compile(
        r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)" \(DEAD\) '
        r'\([^)]*pos=<([^>]+)>\) died'
    )

    ultima_arma_pvp = {}

    for line in log_text.splitlines():
        line = line.strip()
        if not line:
            continue

        # Padrão 1: killed
        m = re_killed.match(line)
        if m:
            hora, assassino, vitima = m.group(1), m.group(2), m.group(3)
            arma = ultima_arma_pvp.pop(vitima, "Desconhecida")
            eventos.append({
                "tipo": "pvp_kill",
                "hora": hora,
                "dt": parse_dt(hora),
                "assassino": assassino,
                "vitima": vitima,
                "arma": arma,
                "dano": "",
                "parte_corpo": "",
                "posicao": "",
                "icone": "💀",
                "descricao": f"{assassino} eliminou {vitima}",
            })
            continue

        # Padrão 2: hit by Player com HP 0
        m = re_hit_pvp.match(line)
        if m:
            hora, vitima, assassino, dano, arma = (
                m.group(1), m.group(2), m.group(3),
                m.group(4), m.group(5)
            )
            ultima_arma_pvp[vitima] = arma
            eventos.append({
                "tipo": "pvp_kill",
                "hora": hora,
                "dt": parse_dt(hora),
                "assassino": assassino,
                "vitima": vitima,
                "arma": arma,
                "dano": dano,
                "parte_corpo": "",
                "posicao": "",
                "icone": "💀",
                "descricao": f"{assassino} eliminou {vitima} com {arma} ({dano} dano)",
            })
            continue

        # (Opcional) Padrão 3: died PvP com posição controlada
        m = re_died_pvp.match(line)
        if m:
            hora, vitima, pos = m.group(1), m.group(2), m.group(3)
            arma = ultima_arma_pvp.pop(vitima, "Desconhecida")
            
            # Filtro de exibição de local
            local_txt = f" em {pos}" if exibir_coords else ""
            
            eventos.append({
                "tipo": "pvp_kill",
                "hora": hora,
                "dt": parse_dt(hora),
                "assassino": "",
                "vitima": vitima,
                "arma": arma,
                "dano": "",
                "parte_corpo": "",
                "posicao": pos if exibir_coords else "",
                "icone": "💀",
                "descricao": f"{vitima} morreu (PvP){local_txt} — arma: {arma}",
            })
            continue

    eventos.sort(key=lambda x: x.get("dt") or datetime.min.replace(tzinfo=FUSO_BR), reverse=True)
    return eventos

def analisar_glitches(log_text: str, feeds_config: dict, client_data: dict, mapa: str):
    """
    Auditoria de Glitch: Detecta Subsolo e Spam de Objetos (Fogueiras/Hortas).
    Regra Spam: >1 objeto no raio de 25m em menos de 60min = BAN.
    """
    violacoes = []
    if not log_text or not feeds_config:
        return violacoes

    # 'tracking_acoes' armazena o histórico temporário no seu clients_data.json
    tracking = client_data.get("tracking_acoes", {})
    agora = datetime.now(FUSO_BR) 
    
    # Mapeamento de chaves para os switches do painel
    itens_spam = {
        "Fireplace": "glitch_fogueiras",
        "GardenPlot": "glitch_hortas"
    }

    # Regex 1: Posição Geral (Subsolo)
    re_pos = re.compile(r'Player "([^"]+)" \([^)]*pos=<([\d., -]+)>')
    # Regex 2: Colocação de Objetos (Spam)
    re_placed = re.compile(r'Player "([^"]+)" .* placed (Fireplace|GardenPlot) at pos=<([\d., -]+)>')

    for line in log_text.splitlines():
        line_lower = line.lower()
        
        # --- PARTE A: AUDITORIA DE SPAM (FOGUEIRAS/HORTAS) ---
        m_spam = re_placed.search(line)
        if m_spam:
            nome, item, pos_str = m_spam.group(1), m_spam.group(2), m_spam.group(3)
            
            if feeds_config.get(itens_spam[item], True):
                try:
                    parts = [float(p.strip()) for p in pos_str.split(',')]
                    pos_atual = (parts[0], parts[1], parts[2])
                    
                    if nome not in tracking:
                        tracking[nome] = []
                    
                    # Filtra histórico do jogador: mesmo item nos últimos 60 minutos
                    acoes_recentes = [
                        a for a in tracking[nome] 
                        if a['tipo'] == item and (agora - datetime.fromisoformat(a['dt'])).total_seconds() < 3600
                    ]

                    # Verifica Raio de 25 metros
                    for acao in acoes_recentes:
                        distancia = calcular_distancia_3d(pos_atual, acao['pos'])
                        if distancia <= 25.0:
                            violacoes.append({
                                "jogador": nome,
                                "tipo": f"Spam de {item}",
                                "detalhe": f"Multiplos {item} em raio de {distancia:.1f}m em < 60min",
                                "pos": pos_str,
                                "banir": True  # Gatilho para inclusão na Banlist
                            })
                    
                    # Registra a ação para correlação futura
                    tracking[nome].append({
                        "tipo": item, 
                        "pos": pos_atual, 
                        "dt": agora.isoformat()
                    })
                except: pass

        # --- PARTE B: AUDITORIA DE SUBSOLO (ELEVAÇÃO) ---
        m_pos = re_pos.search(line)
        if m_pos and feeds_config.get("glitch_subsolo", True):
            nome, pos_str = m_pos.group(1), m_pos.group(2)
            try:
                parts = [float(p.strip()) for p in pos_str.split(',')]
                px, py, pz = parts[0], parts[1], parts[2]
                
                y_terreno, _ = get_local_elevation_by_map(px, pz, mapa)
                if y_terreno and py < (y_terreno - 1.5): # Margem de segurança
                    violacoes.append({
                        "jogador": nome,
                        "tipo": "Subsolo",
                        "detalhe": f"Abaixo do mapa (Y:{py:.1f} / Terra:{y_terreno:.1f})",
                        "pos": pos_str,
                        "banir": False # Geralmente kick ou aviso, admin decide se bane
                    })
            except: pass
            
    # --- LIMPEZA DE DADOS (PURGE) ---
    # Remove registros com mais de 1h para manter o JSON leve
    for player in list(tracking.keys()):
        tracking[player] = [
            a for a in tracking[player] 
            if (agora - datetime.fromisoformat(a['dt'])).total_seconds() < 3600
        ]
        if not tracking[player]:
            tracking.pop(player)
        
    return violacoes

def extrair_coordenadas_mapa(log_text: str):
    """
    Extrai todas as coordenadas de players para gerar o Mapa de Calor.
    """
    coords = []
    if not log_text:
        return coords

    # Regex para capturar qualquer posição de jogador no log
    re_pos = re.compile(r'pos=<([\d., -]+)>')
    
    for line in log_text.splitlines():
        m = re_pos.search(line)
        if m:
            try:
                parts = [float(p.strip()) for p in m.group(1).split(',')]
                # DayZ usa (X, Y, Z). Para o mapa, focamos em X e Z (horizontal)
                coords.append([parts[2], parts[0]]) # Invertido para o padrão de mapas (Lat/Lon)
            except:
                continue
    return coords

def aplicar_banimento_ftp(ftp_cfg: dict, gamertag: str):
    """
    Executa o banimento automático via FTP.
    Adiciona a gamertag ao arquivo banlist.txt do servidor.
    """
    try:
        # 1. Conexão e Download
        with ftplib.FTP(ftp_cfg['host'], ftp_cfg['user'], ftp_cfg['pass']) as ftp:
            ftp.cwd(ftp_cfg.get('path', '/')) # Acessa a pasta raiz ou definida
            
            # Tenta baixar o arquivo atual
            temp_filename = "banlist_temp.txt"
            with open(temp_filename, "wb") as f:
                try:
                    ftp.retrbinary("RETR banlist.txt", f.write)
                except:
                    # Se o arquivo não existir, cria um novo
                    pass

            # 2. Edição do Arquivo (Adiciona o infrator)
            with open(temp_filename, "a") as f:
                f.write(f"\n{gamertag}")
            
            # 3. Upload do arquivo atualizado
            with open(temp_filename, "rb") as f:
                ftp.storbinary("STOR banlist.txt", f)
            
            # Limpeza local
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
            return True
    except Exception as e:
        print(f"Erro ao aplicar banimento FTP: {e}")
        return False

# =========================================================
# 4.3 RANKING SEMANAL — ACUMULADO 7 DIAS
# =========================================================

def ftp_download_adm_files_weekly(ftp_cfg: dict, max_files: int = 7) -> str:
    arquivos = ftp_list_adm_files(ftp_cfg)
    if not arquivos:
        print("[TITAN DEBUG] Nenhum arquivo .ADM encontrado para processar o ranking.")
        return ""

    arquivos_semana = arquivos[:max_files]
    conteudo_total = ""

    try:
        with FTP() as ftp:
            ftp.connect(ftp_cfg["host"], ftp_cfg["port"], timeout=30)
            ftp.login(ftp_cfg["user"], ftp_cfg["pass"])
            ftp.cwd(DAYZ_LOG_DIR)

            for nome_arquivo in arquivos_semana:
                buffer = io.BytesIO()
                try:
                    ftp.retrbinary(f"RETR {nome_arquivo}", buffer.write)
                    texto = buffer.getvalue().decode("utf-8", errors="ignore")
                    
                    if texto.strip():
                        conteudo_total += texto + "\n"
                        print(f"[TITAN DEBUG] Sucesso ao baixar: {nome_arquivo}")
                except Exception as e:
                    print(f"[TITAN DEBUG] Erro ao baixar arquivo {nome_arquivo}: {e}")
                    continue
    except Exception as e:
        print(f"[TITAN DEBUG] Erro fatal na conexão FTP do Ranking: {e}")

    return conteudo_total


def parse_adm_semanal(log_text: str) -> dict:
    """
    Parser completo para ranking semanal.
    Processa múltiplos arquivos .ADM concatenados.
    Retorna dict com stats acumulados por jogador:
    {
      "nome": {
        "total_play_seconds": int,       # tempo total de jogo
        "session_count": int,            # número de sessões
        "max_survival_seconds": int,     # maior tempo sobrevivendo sem morrer
        "current_survival_seconds": int, # tempo sobrevivendo atual (sessão em aberto)
        "total_survival_seconds": int,   # acumulado de todos os períodos vivos
        "pve_hits": int,                 # hits por zumbi
        "pve_suicides": int,             # suicídios
        "pvp_kills": int,                # kills PvP
        "pvp_deaths": int,               # mortes PvP
        "last_connect_dt": datetime|None,
        "last_spawn_dt": datetime|None,
        "last_death_dt": datetime|None,
        "xp": float,                     # calculado: baseado em tempo de sobrevivência
      }
    }
    """
    if not log_text or not log_text.strip():
        return {}

    players = {}

    # Regex (forma correta em raw strings)
    re_player_line   = re.compile(r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)"')
    re_date_header   = re.compile(r'AdminLog started on (\d{4}-\d{2}-\d{2})')
    re_hit_infected  = re.compile(r'hit by Infected .* for ([\d.]+) damage')
    re_killed_pvp    = re.compile(r'\) killed Player "([^"]+)"')
    re_died          = re.compile(r'\(DEAD\).*died\.')
    re_pos           = re.compile(r'pos=<([\d., -]+)>')

    # Palavras-chave
    key_connected     = "is connected"
    key_connecting    = "is connecting"
    key_disconnected  = "has been disconnected"
    key_suicide_emote = "performed EmoteSuicide"
    key_committed_sui = "committed suicide"
    key_hit_infected  = "hit by Infected"
    key_killed        = ") killed Player"
    key_died          = "(DEAD)"

    # Data corrente do bloco sendo processado
    current_date = datetime.now(FUSO_BR).date()

    def parse_dt(tstr: str, date=None):
        base = date or current_date
        try:
            dt = datetime.strptime(f"{base} {tstr}", "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=FUSO_BR)
        except Exception:
            return None

    def ensure_player(name: str) -> dict:
        if name not in players:
            players[name] = {
                "total_play_seconds": 0,
                "session_count": 0,
                "max_survival_seconds": 0,
                "current_survival_seconds": 0,
                "total_survival_seconds": 0,
                "pve_hits": 0,
                "pve_suicides": 0,
                "pvp_kills": 0,
                "pvp_deaths": 0,
                "last_connect_dt": None,
                "last_spawn_dt": None,
                "last_death_dt": None,
                "xp": 0.0,
            }
        return players[name]

    def registrar_morte(p: dict, dt_morte):
        """Registra morte: fecha período de sobrevivência e acumula XP."""
        if p.get("last_spawn_dt") and dt_morte:
            delta = (dt_morte - p["last_spawn_dt"]).total_seconds()
            if delta > 0:
                surv = int(delta)
                p["total_survival_seconds"] += surv
                if surv > p["max_survival_seconds"]:
                    p["max_survival_seconds"] = surv
        p["last_death_dt"] = dt_morte
        p["last_spawn_dt"] = None
        # XP provisório (será recalculado no final também)
        p["xp"] = round(p["total_survival_seconds"] / 60, 2)

    for line in log_text.splitlines():
        line = line.strip()
        if not line:
            continue

        # Atualiza data do bloco
        m_date = re_date_header.search(line)
        if m_date:
            try:
                current_date = datetime.strptime(m_date.group(1), "%Y-%m-%d").date()
            except Exception:
                pass
            continue

        m = re_player_line.match(line)
        if not m:
            continue

        hora_str = m.group(1)
        nome = m.group(2)
        is_dead_line = key_died in line

        dt_evento = parse_dt(hora_str, current_date)
        p = ensure_player(nome)

        # Conexão
        if key_connected in line and key_connecting not in line and not is_dead_line:
            p["last_connect_dt"] = dt_evento
            # Considera connect como novo spawn (novo personagem)
            if p["last_spawn_dt"] is None:
                p["last_spawn_dt"] = dt_evento

        # Desconexão — fecha sessão de jogo
        elif key_disconnected in line:
            if p.get("last_connect_dt") and dt_evento:
                delta = (dt_evento - p["last_connect_dt"]).total_seconds()
                if delta > 0:
                    p["total_play_seconds"] += int(delta)
                    p["session_count"] += 1
            # Fecha período de sobrevivência se jogador desconectou ainda vivo
            if p.get("last_spawn_dt") and dt_evento:
                delta_surv = (dt_evento - p["last_spawn_dt"]).total_seconds()
                if delta_surv > 0:
                    surv = int(delta_surv)
                    p["total_survival_seconds"] += surv
                    if surv > p["max_survival_seconds"]:
                        p["max_survival_seconds"] = surv
            p["last_connect_dt"] = None
            p["last_spawn_dt"] = None

        # Hit por Infected (dano real) — inclui golpe fatal (linha com DEAD)
        if key_hit_infected in line:
            m_hit = re_hit_infected.search(line)
            if m_hit:
                try:
                    if float(m_hit.group(1)) > 0:
                        p["pve_hits"] += 1
                except Exception:
                    pass

        # Suicídio / EmoteSuicide — no DayZ Console a linha já vem com (DEAD)
        if key_suicide_emote in line or key_committed_sui in line:
            p["pve_suicides"] += 1
            registrar_morte(p, dt_evento)

        # Morte real (DEAD died) — só conta PvP se houver killer identificado na linha
        if is_dead_line and "died." in line and key_committed_sui not in line:
            registrar_morte(p, dt_evento)
            # pvp_deaths só incrementa se killer for identificado via key_killed
            if key_killed not in line:
                p["pve_suicides"] += 0  # morte por ambiente (zumbi, fome, queda) — não conta PvP

        # Kill PvP — credita para o assassino
        if key_killed in line and not is_dead_line:
            m_kill = re_killed_pvp.search(line)
            if m_kill:
                p["pvp_kills"] += 1
                vitima = m_kill.group(1)
                pv = ensure_player(vitima)
                registrar_morte(pv, dt_evento)
                pv["pvp_deaths"] += 1

    # Recalcula XP final para todos
    for nome, p in players.items():
        if p.get("last_spawn_dt") and p.get("last_connect_dt"):
            agora = datetime.now(FUSO_BR)
            delta_atual = (agora - p["last_spawn_dt"]).total_seconds()
            if delta_atual > 0:
                current = int(delta_atual)
                p["current_survival_seconds"] = current
                total_xp_base = p["total_survival_seconds"] + current
            else:
                total_xp_base = p["total_survival_seconds"]
        else:
            total_xp_base = p["total_survival_seconds"]

        p["xp"] = round(total_xp_base / 60, 2)
        p["nivel"] = max(1, int(p["xp"] // 100) + 1)
        p["xp_no_nivel"] = round(p["xp"] % 100, 2)

    return players


def get_magnata_ranking(clients_db: dict, server_id: str) -> list:
    """
    Retorna top 10 jogadores por saldo total (banco + carteira) em DzCoins.
    """
    client_data = clients_db.get(server_id, {})
    wallets = client_data.get("wallets", {})
    bank = client_data.get("bank", {})

    ranking = []
    todos_jogadores = set(list(wallets.keys()) + list(bank.keys()))

    for gt in todos_jogadores:
        saldo_w = wallets.get(gt, {}).get("balance", 0)
        saldo_b = bank.get(gt, {}).get("balance", 0)
        total = saldo_w + saldo_b
        ranking.append({
            "gamertag": gt,
            "carteira": saldo_w,
            "banco": saldo_b,
            "total": total,
        })

    ranking.sort(key=lambda x: x["total"], reverse=True)
    return ranking[:10]
    
# =========================================================
# 4.4 FUNÇÕES DA LOJA VIRTUAL
# =========================================================

def registrar_compra(
    clients_db: dict,
    server_id: str,
    gamertag: str,
    item: dict,
    origem: str,
    coordenadas: str,
    hora_br: str,
) -> tuple[bool, str]:
    """
    Registra uma compra na loja:
    - Debita DzCoins do banco ou carteira
    - Salva pedido em client_data["pedidos"]
    - Envia imediatamente o arquivo loja_spawn.json via FTP
    - Retorna (sucesso, mensagem)
    """
    client_data = clients_db.get(server_id, {})

    if "wallets" not in client_data:
        client_data["wallets"] = {}
    if "bank" not in client_data:
        client_data["bank"] = {}
    if "pedidos" not in client_data:
        client_data["pedidos"] = []

    wallets = client_data["wallets"]
    bank    = client_data["bank"]
    preco   = int(item.get("preco", 0))

    wallet_reg = wallets.get(gamertag, {"balance": 0, "historico": []})
    bank_reg   = bank.get(gamertag, {"balance": 0, "historico": []})

    saldo_w = wallet_reg.get("balance", 0)
    saldo_b = bank_reg.get("balance", 0)

    # Valida saldo
    if origem == "💰 Carteira":
        if saldo_w < preco:
            return False, f"Saldo insuficiente na carteira ({saldo_w} DzCoins)."
        wallet_reg["balance"] = saldo_w - preco
        wallet_reg.setdefault("historico", []).append(
            f"[{hora_br}] COMPRA LOJA — {item['nome']} x{item.get('quantidade', 1)} "
            f"-{preco} DzCoins (carteira)"
        )
        wallets[gamertag] = wallet_reg

    elif origem == "🏦 Banco":
        if saldo_b < preco:
            return False, f"Saldo insuficiente no banco ({saldo_b} DzCoins)."
        bank_reg["balance"] = saldo_b - preco
        bank_reg.setdefault("historico", []).append(
            f"[{hora_br}] COMPRA LOJA — {item['nome']} x{item.get('quantidade', 1)} "
            f"-{preco} DzCoins (banco)"
        )
        bank[gamertag] = bank_reg

    else:
        return False, "Origem de pagamento inválida."

    # Registra pedido
    pedido = {
        "id": f"{gamertag}_{hora_br.replace('/', '').replace(':', '').replace(' ', '_')}",
        "gamertag": gamertag,
        "item_id": item.get("id"),
        "item_nome": item.get("nome"),
        "item_classe": item.get("classe"),
        "item_categoria": item.get("categoria"),
        "quantidade": item.get("quantidade", 1),
        "preco": preco,
        "origem_pagamento": origem,
        "coordenadas": coordenadas.strip(),
        "data_compra": hora_br,

        # Exibição no portal
        "status": "Entregue",
        "data_entrega": datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M"),

        # Controle logístico real para o spawn
        "spawn_pendente": True,
    }

    client_data["pedidos"].insert(0, pedido)
    client_data["wallets"] = wallets
    client_data["bank"]    = bank
    clients_db[server_id]  = client_data

    # Envia imediatamente o loja_spawn.json via FTP com todos os pedidos
    # que ainda precisam permanecer no spawn
    mapa = client_data.get("loja", {}).get("mapa_padrao", "Chernarus")
    pedidos_pendentes = [
        p for p in client_data["pedidos"]
        if p.get("spawn_pendente", False) is True
    ]

    success = enviar_pedidos_via_ftp(
        client_id=server_id,
        pedidos=pedidos_pendentes,
        mapa=mapa
    )

    if success:
        clients_db[server_id] = client_data
        return True, "Pedido registrado e arquivo enviado via FTP."
    else:
        return True, "Pedido registrado, mas falha ao enviar via FTP. Será entregue no próximo reset."

def sincronizar_pedidos_apos_reset(
    clients_db: dict,
    server_id: str,
) -> tuple[bool, str]:
    """
    Sincroniza pedidos após detectar reset real do servidor:
    - Lê o restart.log via FTP
    - Descobre o último reset
    - Marca como não pendentes no spawn os pedidos comprados antes ou no reset
    - Reenvia o loja_spawn.json apenas com os pedidos ainda pendentes
    """
    client_data = clients_db.get(server_id, {})
    pedidos = client_data.get("pedidos", [])

    if not pedidos:
        return False, "Nenhum pedido para sincronizar."

    ftpcfg = get_client_ftp_config(client_data)
    if not ftpcfg:
        return False, "FTP não configurado para este servidor."

    restart_text, err = ftp_download_restart_log(ftpcfg)
    if err or not restart_text:
        return False, f"Não foi possível ler restart.log: {err or 'arquivo vazio'}"

    last_reset_dt = parse_last_restart_from_restart_log(restart_text)
    if not last_reset_dt:
        return False, "Nenhum reset encontrado no restart.log."

    houve_alteracao = False

    for p in pedidos:
        if p.get("spawn_pendente", False) is not True:
            continue

        data_compra_str = p.get("data_compra")
        if not data_compra_str:
            continue

        try:
            data_compra_dt = datetime.strptime(data_compra_str, "%d/%m/%Y %H:%M")
            data_compra_dt = data_compra_dt.replace(tzinfo=FUSO_BR)
        except Exception:
            continue

        if data_compra_dt <= last_reset_dt:
            p["spawn_pendente"] = False
            p["resetado_em"] = last_reset_dt.strftime("%d/%m/%Y %H:%M:%S")
            houve_alteracao = True

    mapa = client_data.get("loja", {}).get("mapa_padrao", "Chernarus")
    pedidos_pendentes = [
        p for p in pedidos
        if p.get("spawn_pendente", False) is True
    ]

    success = enviar_pedidos_via_ftp(
        client_id=server_id,
        pedidos=pedidos_pendentes,
        mapa=mapa
    )

    if houve_alteracao:
        client_data["pedidos"] = pedidos
        clients_db[server_id] = client_data

    if not success:
        return False, "Pedidos sincronizados, mas falhou ao atualizar lojas_pawn.json."

    if houve_alteracao:
        return True, "Pedidos sincronizados após reset e lojas_pawn.json atualizado."

    return True, "Nenhum pedido precisava ser alterado; lojas_pawn.json foi apenas revalidado."

    
# =========================================================
# 5. COMPONENTES DE UI
# =========================================================

def render_relogio():
    @st.fragment(run_every="1s")
    def _clock():
        agora = datetime.now(FUSO_BR)
        st.markdown(
            f"""
            <div style="text-align:center; padding: 8px 16px;
                        background: #1a1a2e; border-radius: 10px;
                        border: 1px solid #444;">
                <span style="font-size:13px; color:#aaa;">🕒 Horário Brasília</span><br>
                <span style="font-size:22px; font-weight:bold; color:#00d4ff; font-family:monospace;">
                    {agora.strftime("%H:%M:%S")}
                </span><br>
                <span style="font-size:12px; color:#888;">
                    {agora.strftime("%d/%m/%Y")}
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )
    _clock()

def get_online_from_adm(ftp_cfg: dict, feeds_config: dict = None) -> list:
    """
    Lê o log ADM do dia e retorna lista de jogadores atualmente online.
    feeds_config: dicionário com as permissões (ex: conta_players_online).
    """
    # Validação de Governança: Verifica se a função está ativa no painel
    # Usando players_online_auto conforme definido na sua estrutura de feeds
    if feeds_config and not feeds_config.get("players_online_auto", True):
        return []

    log_text, _ = ftp_download_latest_adm(ftp_cfg)
    if not log_text:
        return []

    conectados = {}
    for line in log_text.splitlines():
        line = line.strip()
        if not line:
            continue
            
        m = re.match(r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)"', line)
        if not m:
            continue
            
        nome = m.group(2)
        if "(DEAD)" in line:
            continue
            
        if "is connected" in line and "is connecting" not in line:
            conectados[nome] = True
        elif "has been disconnected" in line:
            conectados.pop(nome, None)
            
    return [n for n, v in conectados.items() if v]


def render_players_online(nitrado_id: str, ftp_cfg: dict = None, nitrado_token: str = "", feeds_config: dict = None):
    """
    Renderiza o box de jogadores online com fallback para o log ADM.
    feeds_config: dicionário vindo do client_data['feeds_config'].
    """
    @st.fragment(run_every=60)
    def _players():
        # 1. Verifica se a função de monitoramento está ativa globalmente para o servidor
        if feeds_config and not feeds_config.get("players_online_auto", True):
            st.info("ℹ️ Monitoramento de Players Online desativado pelo administrador.")
            return

        # Tenta obter dados da API Nitrado
        dados = get_players_online(nitrado_id, nitrado_token=nitrado_token)

        # 2. AJUSTE: Se a API falhar, tenta o Log ADM via FTP (respeitando as chaves de feed)
        if "erro" in dados:
            if ftp_cfg:
                # Passamos o feeds_config para a função de parser
                players_adm = get_online_from_adm(ftp_cfg, feeds_config=feeds_config)
                
                if players_adm is not None:
                    total_adm = len(players_adm)
                    st.markdown(
                        f'<div style="background:#1a1a2e;border-radius:10px;padding:14px;border:1px solid #7a4b1f;margin-bottom:8px;">'
                        f'<div style="font-size:13px;color:#ffcc66;margin-bottom:4px;">🌐 Players Online (via Log ADM)</div>'
                        f'<div style="font-size:28px;font-weight:bold;color:#00ff88;">{total_adm}</div></div>',
                        unsafe_allow_html=True,
                    )
                    if total_adm > 0:
                        with st.expander(f"Ver jogadores ({total_adm})", expanded=False):
                            for nome in players_adm:
                                st.markdown(f"◆ `{nome}`")
                    else:
                        st.caption("Nenhum jogador online detectado no log.")
                    return 

        # 3. Se não houver erro na API, segue o fluxo normal
        if "erro" in dados:
            st.warning(f"⚠️ Players Online indisponível: {dados['erro']}")
            return

        total = dados.get("total", 0)
        players = dados.get("players", [])
        maximo = dados.get("max", 0)

        # Fallback caso a API retorne total > 0 mas sem nomes (respeitando feeds_config)[cite: 3, 4]
        if total > 0 and not players and ftp_cfg:
            players = get_online_from_adm(ftp_cfg, feeds_config=feeds_config)

        st.markdown(
            f'<div style="background:#1a1a2e;border-radius:10px;padding:14px;border:1px solid #444;margin-bottom:8px;">'
            f'<div style="font-size:13px;color:#aaa;margin-bottom:4px;">🌐 Players Online</div>'
            f'<div style="font-size:28px;font-weight:bold;color:#00ff88;">{total}'
            f'<span style="font-size:14px;color:#666;">/ {maximo}</span></div></div>',
            unsafe_allow_html=True,
        )

        if total > 0 and players:
            with st.expander(f"Ver jogadores ({total})", expanded=False):
                for nome in players:
                    st.markdown(f"◆ `{nome}`")
        elif total > 0:
            st.caption(f"{total} jogador(es) online, lista de nomes indisponível.")
        else:
            st.caption("Nenhum jogador online no momento.")

    _players()

# =========================================================
# 3. COMPONENTES DE ANALYTICS E INTELIGÊNCIA
# =========================================================

def processar_ranking_global(eventos_pvp: list, eventos_conexao: list):
    """
    Consolida estatísticas para o Ranking Global.
    Regra: Kills (+10 pts), Mortes (-5 pts), Tempo Online (1 pt/hora).
    """
    ranking = {}

    # Processa Kills e Mortes (PvP)
    for ev in eventos_pvp:
        vitima = ev.get("vitima")
        assassino = ev.get("assassino")

        if assassino:
            stats = ranking.get(assassino, {"kills": 0, "mortes": 0, "pontos": 0})
            stats["kills"] += 1
            stats["pontos"] += 10
            ranking[assassino] = stats

        if vitima:
            stats = ranking.get(vitima, {"kills": 0, "mortes": 0, "pontos": 0})
            stats["mortes"] += 1
            stats["pontos"] = max(0, stats["pontos"] - 5)
            ranking[vitima] = stats

    # Retorna lista ordenada por pontos
    return sorted(ranking.items(), key=lambda x: x[1]['pontos'], reverse=True)

def render_reset_info(client_data: dict):
    agora = datetime.now(FUSO_BR)
    horarios_reset = [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22]

    proximo_reset = None
    for h in horarios_reset:
        candidato = agora.replace(hour=h, minute=0, second=0, microsecond=0)
        if candidato > agora:
            proximo_reset = candidato
            break

    if proximo_reset is None:
        proximo_reset = (agora + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    delta = proximo_reset - agora
    minutos_restantes = max(0, int(delta.total_seconds() // 60))

    ultimo_reset_texto = "FTP não configurado — usando apenas regra de horário."
    try:
        ftp_cfg = get_client_ftp_config(client_data)
        if ftp_cfg:
            restart_text, err = ftp_download_restart_log(ftp_cfg)
            if not err and restart_text:
                last_reset_dt = parse_last_restart_from_restart_log(restart_text)
                if last_reset_dt:
                    ultimo_reset_texto = (
                        f"Último reset detectado: "
                        f"<b>{last_reset_dt.strftime('%d/%m/%Y %H:%M:%S')}</b>"
                    )
                else:
                    ultimo_reset_texto = "Nenhuma linha de reset encontrada em restart.log."
    except Exception:
        pass

    st.markdown(
        f"""
        <div style="background:#1a1a2e; border-radius:10px; padding:14px;
                    border:1px solid #444; margin-bottom:8px;">
            <div style="font-size:13px; color:#aaa; margin-bottom:8px;">
                🔄 Reset do Servidor
            </div>
            <div style="font-size:14px; color:#ddd; margin-bottom:6px;">
                Próximo reset (regra a cada 2h):<br>
                <b>{proximo_reset.strftime("%d/%m/%Y %H:%M")}</b>
            </div>
            <div style="font-size:12px; color:#999; margin-bottom:6px;">
                Falta aproximadamente <b>{minutos_restantes} minuto(s)</b>.
            </div>
            <div style="font-size:12px; color:#aaa;">
                {ultimo_reset_texto}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def render_player_card(dados_stats, gamertag, nid, discord_tag, saldo_total):
    """
    Gera o visual estilizado do Player Stats.
    """
    xp = dados_stats.get("xp", 0.0)
    nivel = dados_stats.get("nivel", 1)
    # Garante que format_seconds_hhmmss seja acessível aqui
    barra_progresso = "▉" * int((xp % 100) / 10) + "░" * (10 - int((xp % 100) / 10))
    pct = int(xp % 100)

    st.markdown(
        f"""
        <div style="background:#111; border-left: 5px solid #00ff00; padding:20px; border-radius:10px; font-family:monospace; color:white;">
            <div style="font-size:18px; font-weight:bold;">ℙ𝕝𝕒𝕪𝕖𝕣 𝕊𝕥𝕒𝕥𝕤</div>
            <div style="color:#aaa; font-size:12px;">nid: {nid}</div>
            <div style="color:#aaa; font-size:12px;">@{discord_tag}</div>
            <br>
            <div style="font-size:24px; font-weight:bold;">{gamertag}</div>
            <div style="color:#00d4ff; font-style:italic;"><b>XP</b> {xp:.2f} <b>Nível:</b> {nivel}</div>
            <div style="font-size:16px;">{barra_progresso} <span style="background:#333; padding:2px 5px; border-radius:4px; font-size:10px;">{pct}%</span></div>
            <br>
            <div style="line-height:1.2;">
                ╒Total de vítimas: {dados_stats.get('pvp_kills', 0)}<br>
                ╞Total de mortes: {dados_stats.get('pvp_deaths', 0)}<br>
                ╞Matou na semana: {dados_stats.get('pvp_kills', 0)}<br>
                ╘Morreu em PvP: {dados_stats.get('pvp_deaths', 0)}<br>
                ╒Total em DzCoins: {saldo_total:,.0f}<br>
                ╞Tempo Sobrevivendo: {format_seconds_hhmmss(dados_stats.get('total_survival_seconds', 0))}<br>
                ╘Tempo online total: {format_seconds_hhmmss(dados_stats.get('total_play_seconds', 0))}
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

# =========================================================
# 6. ABA BANCO DZCOINS
# =========================================================

def render_banco(client_data: dict, clients_db: dict, server_id: str, gamertag: str, plano_atual: str = "Starter"):
    players = client_data.get("players", {})

    if gamertag not in players:
        st.warning("Gamertag não encontrada. Faça o vínculo primeiro.")
        return

    if "wallets" not in client_data:
        client_data["wallets"] = {}
    if "bank" not in client_data:
        client_data["bank"] = {}

    wallets = client_data["wallets"]
    bank = client_data["bank"]

    wallet_reg = wallets.get(gamertag, {"balance": 0, "historico": []})
    bank_reg = bank.get(gamertag, {"balance": 0, "historico": []})

    saldo_carteira = wallet_reg.get("balance", 0)
    saldo_banco = bank_reg.get("balance", 0)

    col1, col2 = st.columns(2)
    col1.metric("💰 Carteira", f"{saldo_carteira} DzCoins")
    col2.metric("🏦 Banco", f"{saldo_banco} DzCoins")

    st.divider()

    opcoes_banco = [
        "📋 Extrato",
        "➡️ Depositar (Carteira → Banco)",
        "⬅️ Sacar (Banco → Carteira)",
    ]

    if plano_permite(plano_atual, "transferencia_jogador"):
        opcoes_banco.append("🔁 Transferir para outro jogador")

    op = st.radio(
        "Operação",
        opcoes_banco,
        horizontal=False,
        label_visibility="collapsed",
        key="op_banco_radio",
    )

    hora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")

    if op == "📋 Extrato":
        historico_comb = []

        for linha in wallet_reg.get("historico", []):
            historico_comb.append(("CARTEIRA", linha))

        for linha in bank_reg.get("historico", []):
            historico_comb.append(("BANCO", linha))

        col_ext_1, col_ext_2 = st.columns([3, 1])
        with col_ext_1:
            st.markdown("#### 📋 Extrato de movimentações")
        with col_ext_2:
            if st.button("🧹 Limpar Extrato", key="limpar_extrato_jogador", use_container_width=True):
                wallet_reg["historico"] = []
                bank_reg["historico"] = []

                wallets[gamertag] = wallet_reg
                bank[gamertag] = bank_reg
                client_data["wallets"] = wallets
                client_data["bank"] = bank
                clients_db[server_id] = client_data
                save_db(DB_CLIENTS, clients_db)

                st.success("✅ Extrato limpo com sucesso.")
                st.rerun()

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
            ">{historico_txt}</div>
            """,
            unsafe_allow_html=True,
        )

    elif op == "➡️ Depositar (Carteira → Banco)":
        st.markdown("#### ➡️ Depositar na conta bancária")
        valor = st.number_input("Valor (DzCoins)", min_value=0, step=100, key="dep_val")
        if st.button("Confirmar depósito", use_container_width=True):
            if valor <= 0:
                st.error("Informe um valor maior que zero.")
            elif valor > saldo_carteira:
                st.error(f"Saldo insuficiente na carteira ({saldo_carteira} DzCoins).")
            else:
                wallet_reg["balance"] = saldo_carteira - valor
                bank_reg["balance"] = saldo_banco + valor
                wallet_reg.setdefault("historico", []).append(
                    f"[{hora_br}] DEPÓSITO → BANCO -{valor}"
                )
                bank_reg.setdefault("historico", []).append(
                    f"[{hora_br}] DEPÓSITO ← CARTEIRA +{valor}"
                )
                wallets[gamertag] = wallet_reg
                bank[gamertag] = bank_reg
                client_data["wallets"] = wallets
                client_data["bank"] = bank
                clients_db[server_id] = client_data
                save_db(DB_CLIENTS, clients_db)
                st.success(f"✅ {valor} DzCoins depositados no banco!")
                st.rerun()

    elif op == "⬅️ Sacar (Banco → Carteira)":
        st.markdown("#### ⬅️ Sacar do banco para a carteira")
        valor = st.number_input("Valor (DzCoins)", min_value=0, step=100, key="saq_val")
        if st.button("Confirmar saque", use_container_width=True):
            if valor <= 0:
                st.error("Informe um valor maior que zero.")
            elif valor > saldo_banco:
                st.error(f"Saldo insuficiente no banco ({saldo_banco} DzCoins).")
            else:
                wallet_reg["balance"] = saldo_carteira + valor
                bank_reg["balance"] = saldo_banco - valor
                bank_reg.setdefault("historico", []).append(
                    f"[{hora_br}] SAQUE → CARTEIRA -{valor}"
                )
                wallet_reg.setdefault("historico", []).append(
                    f"[{hora_br}] SAQUE ← BANCO +{valor}"
                )
                wallets[gamertag] = wallet_reg
                bank[gamertag] = bank_reg
                client_data["wallets"] = wallets
                client_data["bank"] = bank
                clients_db[server_id] = client_data
                save_db(DB_CLIENTS, clients_db)
                st.success(f"✅ {valor} DzCoins sacados para a carteira!")
                st.rerun()

    elif op == "🔁 Transferir para outro jogador":
        if not plano_permite(plano_atual, "transferencia_jogador"):
            bloquear_funcionalidade(plano_atual, "🔁 Transferência entre Jogadores")
        else:
            st.markdown("🔁 Transferir DzCoins")
        outros_players = [p for p in players.keys() if p != gamertag]
        if not outros_players:
            st.info("Nenhum outro jogador vinculado neste servidor ainda.")
            return

        destino = st.selectbox("Jogador destino", outros_players)
        origem_op = st.radio(
            "Debitar de:", ["💰 Carteira", "🏦 Banco"], horizontal=True
        )
        valor = st.number_input("Valor (DzCoins)", min_value=0, step=100, key="transf_val")
        saldo_origem = saldo_carteira if origem_op == "💰 Carteira" else saldo_banco

        if st.button("Confirmar transferência", use_container_width=True):
            if valor <= 0:
                st.error("Informe um valor maior que zero.")
            elif valor > saldo_origem:
                st.error(f"Saldo insuficiente ({saldo_origem} DzCoins).")
            else:
                if origem_op == "💰 Carteira":
                    wallet_reg["balance"] = saldo_carteira - valor
                    wallet_reg.setdefault("historico", []).append(
                        f"[{hora_br}] TRANSF. → {destino} -{valor} (carteira)"
                    )
                else:
                    bank_reg["balance"] = saldo_banco - valor
                    bank_reg.setdefault("historico", []).append(
                        f"[{hora_br}] TRANSF. → {destino} -{valor} (banco)"
                    )

                dest_wallet = wallets.get(destino, {"balance": 0, "historico": []})
                dest_wallet["balance"] = dest_wallet.get("balance", 0) + valor
                dest_wallet.setdefault("historico", []).append(
                    f"[{hora_br}] RECEBIDO ← {gamertag} +{valor}"
                )

                wallets[gamertag] = wallet_reg
                bank[gamertag] = bank_reg
                wallets[destino] = dest_wallet
                client_data["wallets"] = wallets
                client_data["bank"] = bank
                clients_db[server_id] = client_data
                save_db(DB_CLIENTS, clients_db)
                st.success(f"✅ {valor} DzCoins transferidos para **{destino}**!")
                st.rerun()

# =========================================================
# 7. ABA RANKING
# =========================================================

def render_ranking(client_data: dict, gamertag_vinculada: str, clients_db: dict, server_id: str):
    # --- TRAVA DE GOVERNANÇA (GRC) ---
    # Verifica se o administrador ativou o Ranking Automático
    feeds = client_data.get("feeds_config", {})
    if not feeds.get("ranking_auto", True):
        st.info("ℹ️ O Ranking Global está temporariamente desativado pelo administrador.")
        return

    ftp_cfg = get_client_ftp_config(client_data)
    if not ftp_cfg:
        st.warning(
            "FTP do servidor não está configurado. "
            "Peça ao admin para configurar no painel."
        )
        return

    @st.fragment(run_every="300s")
    def _ranking(ftp_cfg, gamertag_vinculada, clients_db, server_id):
        with st.spinner("Carregando ranking semanal (últimos 7 dias)..."):
            log_text_semanal = ftp_download_adm_files_weekly(ftp_cfg, max_files=7)

        if not log_text_semanal or not log_text_semanal.strip():
            st.warning("Não foi possível carregar os logs do servidor.")
            return

        stats = parse_adm_semanal(log_text_semanal)

        if not stats:
            st.info("Nenhuma estatística encontrada nos logs da semana.")
            return

        # Persiste XP e nível no clients_data (nunca regride)
        clients_db_fresh = load_db(DB_CLIENTS, {})
        client_fresh = clients_db_fresh.get(server_id, {})
        if "xp_stats" not in client_fresh:
            client_fresh["xp_stats"] = {}
        for nome_xp, dados_xp in stats.items():
            xp_atual = dados_xp.get("xp", 0.0)
            nivel_atual = dados_xp.get("nivel", 1)
            xp_salvo = client_fresh["xp_stats"].get(nome_xp, {}).get("xp", 0.0)
            if xp_atual > xp_salvo:
                client_fresh["xp_stats"][nome_xp] = {
                    "xp": xp_atual,
                    "nivel": nivel_atual,
                    "xp_no_nivel": dados_xp.get("xp_no_nivel", 0.0),
                    "ultima_atualizacao": datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M"),
                }
        clients_db_fresh[server_id] = client_fresh
        save_db(DB_CLIENTS, clients_db_fresh)

        st.caption(
            f"📊 {len(stats)} jogadores encontrados nos logs dos últimos 7 dias — "
            "dados atualizados a cada 5 minutos."
        )

        # ---- Sub-abas de ranking ----
        sub_play, sub_surv, sub_xp, sub_pvp, sub_pve, sub_dzcoins = st.tabs([
            "⏱️ Tempo de Jogo",
            "🏕️ Sobrevivência",
            "⭐ XP",
            "⚔️ PvP",
            "🧟 PvE",
            "💰 Magnata DzCoins",
        ])

        medalhas = ["🥇", "🥈", "🥉"] + ["4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]

        # ---- TEMPO DE JOGO TOP 10 ----
        with sub_play:
            st.markdown("#### ⏱️ Tempo de Jogo Total — Top 10")
            ranking_play = sorted(
                stats.items(),
                key=lambda x: x[1].get("total_play_seconds", 0),
                reverse=True,
            )[:10]

            if not ranking_play:
                st.info("Sem dados de tempo de jogo.")
            else:
                for idx, (nome, dados) in enumerate(ranking_play):
                    destaque = (nome == gamertag_vinculada)
                    cor = "#00d4ff" if destaque else "#e0e0e0"
                    total_seg = dados.get("total_play_seconds", 0)
                    sessoes = dados.get("session_count", 0)
                    st.markdown(
                        f"""
                        <div style="background:#1a1a2e; border-radius:8px; padding:10px 14px;
                                    border-left:3px solid {'#00d4ff' if destaque else '#333'};
                                    margin-bottom:5px;">
                            <span style="font-size:16px;">{medalhas[idx]}</span>
                            <span style="color:{cor}; font-weight:bold; margin-left:8px;">
                                {nome}
                            </span>
                            <span style="color:#aaa; float:right;">
                                ⏱️ {format_seconds_hhmmss(total_seg)}
                                &nbsp;|&nbsp; 🔁 {sessoes} sessões
                            </span>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

        # ---- SOBREVIVÊNCIA TOP 10 ----
        with sub_surv:
            st.markdown("#### 🏕️ Maior Tempo Sobrevivendo — Top 10")
            st.caption("Tempo máximo que o jogador ficou vivo sem morrer em uma única vida.")
            ranking_surv = sorted(
                stats.items(),
                key=lambda x: x[1].get("max_survival_seconds", 0),
                reverse=True,
            )[:10]

            if not ranking_surv:
                st.info("Sem dados de sobrevivência.")
            else:
                for idx, (nome, dados) in enumerate(ranking_surv):
                    destaque = (nome == gamertag_vinculada)
                    cor = "#00d4ff" if destaque else "#e0e0e0"
                    total_surv = dados.get("total_survival_seconds", 0)
                    max_surv = dados.get("max_survival_seconds", 0)
                    st.markdown(
                        f"""
                        <div style="background:#1a1a2e; border-radius:8px; padding:10px 14px;
                                    border-left:3px solid {'#00d4ff' if destaque else '#333'};
                                    margin-bottom:5px;">
                            <span style="font-size:16px;">{medalhas[idx]}</span>
                            <span style="color:{cor}; font-weight:bold; margin-left:8px;">
                                {nome}
                            </span>
                            <span style="color:#aaa; float:right;">
                                🏆 Melhor: {format_seconds_hhmmss(max_surv)}
                                &nbsp;|&nbsp; Total: {format_seconds_hhmmss(total_surv)}
                            </span>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

        # ---- XP TOP 10 ----
        with sub_xp:
            st.markdown("#### ⭐ XP (Experiência) — Top 10")
            st.caption("XP é calculado com base no tempo total sobrevivendo. 1 minuto vivo = 1 XP.")
            ranking_xp = sorted(
                stats.items(),
                key=lambda x: x[1].get("xp", 0.0),
                reverse=True,
            )[:10]

            if not ranking_xp:
                st.info("Sem dados de XP.")
            else:
                xp_max = ranking_xp[0][1].get("xp", 1) if ranking_xp else 1
                xp_max = xp_max or 1  # evita divisão por zero
                for idx, (nome, dados) in enumerate(ranking_xp):
                    destaque = (nome == gamertag_vinculada)
                    cor = "#00d4ff" if destaque else "#e0e0e0"
                    xp = dados.get("xp", 0.0)
                    nivel = dados.get("nivel", max(1, int(xp // 100) + 1))
                    xp_no_nivel = dados.get("xp_no_nivel", round(xp % 100, 2))
                    barra_pct = int((xp_no_nivel / 100) * 100)
                    st.markdown(
                        f"""
                        <div style="background:#1a1a2e; border-radius:8px; padding:10px 14px;
                                    border-left:3px solid {'#00d4ff' if destaque else '#333'};
                                    margin-bottom:5px;">
                            <span style="font-size:16px;">{medalhas[idx]}</span>
                            <span style="color:{cor}; font-weight:bold; margin-left:8px;">
                                {nome}
                            </span>
                            <span style="color:#aaa; float:right;">
                                Nvl {nivel} &nbsp;|&nbsp; ⭐ {xp:.1f} XP &nbsp;|&nbsp; {xp_no_nivel:.1f}/100 XP p/ próx. nível
                            </span>
                            <div style="background:#333; border-radius:4px; height:4px;
                                        margin-top:6px;">
                                <div style="background:#00d4ff; width:{barra_pct}%;
                                            height:4px; border-radius:4px;"></div>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

        # ---- PVP TOP 10 ----
        with sub_pvp:
            st.markdown("#### ⚔️ PvP por Período — Top 10")

            ranking_pvp = sorted(
                stats.items(),
                key=lambda x: x[1].get("pvp_kills", 0),
                reverse=True,
            )[:10]

            tem_pvp = any(d.get("pvp_kills", 0) > 0 for _, d in ranking_pvp)

            if not tem_pvp:
                st.info("Nenhum evento PvP registrado nos últimos 7 dias.")
                st.caption("O ranking será preenchido automaticamente quando ocorrerem kills PvP no servidor.")
            else:
                for idx, (nome, dados) in enumerate(ranking_pvp):
                    destaque = (nome == gamertag_vinculada)
                    cor = "#00d4ff" if destaque else "#e0e0e0"
                    kills = dados.get("pvp_kills", 0)
                    deaths = dados.get("pvp_deaths", 0)
                    kd = round(kills / max(deaths, 1), 2)
                    st.markdown(
                        f"""
                        <div style="background:#1a1a2e; border-radius:8px; padding:10px 14px;
                                    border-left:3px solid {'#00d4ff' if destaque else '#ff4444'};
                                    margin-bottom:5px;">
                            <span style="font-size:16px;">{medalhas[idx]}</span>
                            <span style="color:{cor}; font-weight:bold; margin-left:8px;">
                                {nome}
                            </span>
                            <span style="color:#aaa; float:right;">
                                ⚔️ K: {kills} &nbsp;|&nbsp; 💀 D: {deaths}
                                &nbsp;|&nbsp; K/D: {kd}
                            </span>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

        # ---- PVE TOP 10 ----
        with sub_pve:
            st.markdown("#### 🧟 Hits PvE & Suicídios — Top 10")
            ranking_pve = sorted(
                stats.items(),
                key=lambda x: x[1].get("pve_hits", 0),
                reverse=True,
            )[:10]

            if not ranking_pve:
                st.info("Sem dados de PvE.")
            else:
                for idx, (nome, dados) in enumerate(ranking_pve):
                    destaque = (nome == gamertag_vinculada)
                    cor = "#00d4ff" if destaque else "#e0e0e0"
                    hits = dados.get("pve_hits", 0)
                    suicidios = dados.get("pve_suicides", 0)
                    st.markdown(
                        f"""
                        <div style="background:#1a1a2e; border-radius:8px; padding:10px 14px;
                                    border-left:3px solid {'#00d4ff' if destaque else '#ff8800'};
                                    margin-bottom:5px;">
                            <span style="font-size:16px;">{medalhas[idx]}</span>
                            <span style="color:{cor}; font-weight:bold; margin-left:8px;">
                                {nome}
                            </span>
                            <span style="color:#aaa; float:right;">
                                🧟 Hits: {hits}
                                &nbsp;|&nbsp; 💀 Suicídios: {suicidios}
                            </span>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

        # ---- MAGNATA DZCOINS TOP 10 ----
        with sub_dzcoins:
            st.markdown("#### 💰 Magnata DzCoins — Top 10")
            st.caption("Ranking baseado no saldo total (Carteira + Banco).")
            ranking_mag = get_magnata_ranking(clients_db, server_id)

            if not ranking_mag:
                st.info("Nenhum jogador com DzCoins registrado ainda.")
            else:
                total_max = ranking_mag[0]["total"] if ranking_mag else 1
                total_max = total_max or 1
                for idx, r in enumerate(ranking_mag):
                    destaque = (r["gamertag"] == gamertag_vinculada)
                    cor = "#00d4ff" if destaque else "#e0e0e0"
                    carteira = r["carteira"]
                    banco = r["banco"]
                    total = r["total"]
                    barra_pct = int((total / max(total_max, 1)) * 100)
                    st.markdown(
                        f"""
                        <div style="background:#1a1a2e; border-radius:8px; padding:10px 14px;
                                    border-left:3px solid {'#00d4ff' if destaque else '#FFD700'};
                                    margin-bottom:5px;">
                            <span style="font-size:16px;">{medalhas[idx]}</span>
                            <span style="color:{cor}; font-weight:bold; margin-left:8px;">
                                {r['gamertag']}
                            </span>
                            <span style="color:#aaa; float:right;">
                                💰 {carteira} carteira
                                &nbsp;|&nbsp; 🏦 {banco} banco
                                &nbsp;|&nbsp; 💎 {total} total
                            </span>
                            <div style="background:#333; border-radius:4px; height:4px;
                                        margin-top:6px;">
                                <div style="background:#FFD700; width:{barra_pct}%;
                                            height:4px; border-radius:4px;"></div>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

        # ---- MEU DESEMPENHO ----
        st.divider()
        st.markdown("#### 👤 Meu desempenho na semana")
        meu = stats.get(gamertag_vinculada)
        if not meu:
            st.info("Sua Gamertag ainda não aparece nos logs desta semana.")
        else:
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("⏱️ Tempo de Jogo", format_seconds_hhmmss(meu.get("total_play_seconds", 0)))
            col2.metric("🏕️ Melhor Vida", format_seconds_hhmmss(meu.get("max_survival_seconds", 0)))
            col3.metric("⭐ XP", f"{meu.get('xp', 0.0):.1f}", f"Nível {meu.get('nivel', 1)}")
            col4.metric("⚔️ Kills PvP", meu.get("pvp_kills", 0))
            col5.metric("🧟 Hits PvE", meu.get("pve_hits", 0))

    _ranking(ftp_cfg, gamertag_vinculada, clients_db, server_id)

# =========================================================
# 8. UI PRINCIPAL
# =========================================================

def main():
    st.set_page_config(
        page_title="Titan Cloud Pro - Portal do Jogador",
        page_icon="🎮",
        layout="wide",
    )

    # --- TEMA ---
    if "portal_tema" not in st.session_state:
        st.session_state.portal_tema = "dark"

    tema = st.session_state.portal_tema

    if tema == "dark":
        css = """
        <style>
        .stApp { background-color: #050510; color: #f0f0f0; }
        .stTabs [data-baseweb="tab"] { font-size: 15px; font-weight: bold; color: #bbbbbb; }
        .stTabs [aria-selected="true"] { color: #00e0ff !important; border-bottom: 2px solid #00e0ff !important; }
        div[data-testid="metric-container"] { background: #111827; border-radius: 10px; padding: 12px; border: 1px solid #374151; }
        .block-container { padding-top: 3.5rem; }
        .stButton > button { color: #ffffff !important; background-color: #1d4ed8 !important; border-radius: 8px !important; border: 1px solid #1d4ed8 !important; }
        .stButton > button:hover { background-color: #2563eb !important; }
        thead tr th { color: #e5e7eb !important; font-weight: 700 !important; background-color: #111827 !important; }
        tbody tr td { color: #e5e7eb !important; background-color: #020617 !important; }
        </style>
        """
    else:
        css = """
        <style>
        .stApp { background-color: #f3f4f6; color: #1f2933; }
        .stTabs [data-baseweb="tab"] { font-size: 15px; font-weight: bold; color: #4b5563; }
        .stTabs [aria-selected="true"] { color: #0f766e !important; border-bottom: 2px solid #0f766e !important; }
        div[data-testid="metric-container"] { background: #ffffff; border-radius: 10px; padding: 12px; border: 1px solid #d1d5db; }
        .block-container { padding-top: 3.5rem; }
        .stButton > button { color: #ffffff !important; background-color: #2563eb !important; border-radius: 8px !important; }
        thead tr th { color: #111827 !important; font-weight: 700 !important; }
        tbody tr td { color: #374151 !important; }
        </style>
        """
    st.markdown(css, unsafe_allow_html=True)

    # ----------------------------------------------------------
    # 8.1 PROCESSAR RETORNO DISCORD
    # ----------------------------------------------------------
    query_params = st.query_params
    code = query_params.get("code")

    if code and not st.session_state.get("portal_discord_id"):
        with st.spinner("Autenticando com o Discord..."):
            resultado = trocar_code_por_token(code)
        if resultado:
            st.session_state.portal_discord_id = resultado["discord_id"]
            st.session_state.portal_discord_name = resultado["discord_name"]
            st.session_state.portal_discord_avatar = resultado.get("discord_avatar")
            st.session_state.portal_discord_guilds = resultado["guilds"]
            st.query_params.clear()
            st.rerun()
        else:
            st.query_params.clear()

    # ----------------------------------------------------------
    # 8.2 TELA DE LOGIN
    # ----------------------------------------------------------
    if not st.session_state.get("portal_discord_id"):
        col_c, _ = st.columns([1, 1])
        with col_c:
            st.markdown(
                """
                <div style="text-align:center; padding: 60px 20px;">
                    <h1 style="color:#00d4ff;">🎮 Titan Cloud Pro</h1>
                    <h3 style="color:#aaa;">Portal do Jogador</h3>
                    <p style="color:#666; margin-bottom: 40px;">
                        Vincule sua Gamertag, acompanhe rankings,<br>
                        gerencie seus DzCoins e muito mais.
                    </p>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if not DISCORD_CLIENT_ID or not DISCORD_REDIRECT_URI:
                st.warning("Login com Discord não configurado. Contate o administrador.")
            else:
                params = {
                    "client_id": DISCORD_CLIENT_ID,
                    "redirect_uri": DISCORD_REDIRECT_URI,
                    "response_type": "code",
                    "scope": DISCORD_SCOPE,
                    "prompt": "consent",
                }
                auth_url = DISCORD_AUTHORIZE_URL + "?" + urllib.parse.urlencode(params)
                st.markdown(
                    f"""
                    <div style="text-align:center;">
                        <a href="{auth_url}" target="_self" style="
                            display: inline-block;
                            background: linear-gradient(135deg, #5865F2, #7289da);
                            color: white; padding: 14px 32px;
                            border-radius: 10px; text-decoration: none;
                            font-weight: bold; font-size: 18px;
                            box-shadow: 0 4px 15px rgba(88,101,242,0.4);
                        ">
                        👾 Entrar com Discord
                        </a>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
        st.stop()

    # ----------------------------------------------------------
    # 8.3 CARREGA DADOS
    # ----------------------------------------------------------
    users_db = load_db(DB_USERS, {"keys": {}})
    clients_db = load_db(DB_CLIENTS, {})

    discord_id = st.session_state.get("portal_discord_id")
    discord_name = st.session_state.get("portal_discord_name", "Jogador")
    server_id = st.session_state.get("portal_server_id")
    server_name = st.session_state.get("portal_server_name", "Servidor")

    # ----------------------------------------------------------
    # 8.4 SELEÇÃO DO SERVIDOR
    # ----------------------------------------------------------
    nome_para_server_id = {}
    nitrado_id_map = {}

    for keyuser, data in users_db.get("keys", {}).items():
        server_name_cfg = str(data.get("server", "")).strip()
        server_id_cfg = str(data.get("server_id", "")).strip() or keyuser
        nid = str(data.get("server_id", "")).strip()

        if server_name_cfg and server_id_cfg:
            nome_para_server_id[server_name_cfg.lower()] = server_id_cfg
            nitrado_id_map[server_id_cfg] = nid

    server_id = st.session_state.get("portal_server_id")

    if not server_id:
        st.markdown("### 🏷️ Qual servidor você joga?")
        nome_input = st.text_input("Nome do servidor", "")

        if st.button("Confirmar servidor"):
            nome_limpo = nome_input.strip().lower()
            sid = nome_para_server_id.get(nome_limpo)

            if not sid:
                st.error("Servidor não encontrado. Verifique com o administrador.")
            elif sid not in clients_db:
                st.error("Servidor não configurado. Avise o administrador.")
            else:
                st.session_state.portal_server_id = sid
                st.session_state.portal_server_nome = nome_limpo.title()
                st.rerun()

        st.stop()

    # Busca client_data por server_id ou por user_key (KeyUser)
    # pois o FTP e configs sao salvos sob a KeyUser no eventos_dev.py
    client_data = clients_db.get(server_id, {})
    if not client_data.get("ftp", {}).get("host", ""):
        # Tenta achar pelo keyuser cujo server_id bate com o atual
        for _kuser, _kdata in users_db.get("keys", {}).items():
            if str(_kdata.get("server_id", "")).strip() == str(server_id).strip():
                _alt = clients_db.get(_kuser, {})
                if _alt.get("ftp", {}).get("host", ""):
                    client_data = _alt
                    break
    players = load_players_for_client(client_data)
    server_nome = st.session_state.get("portal_server_nome", "Servidor")
    nitrado_id = nitrado_id_map.get(server_id, server_id)

    # ----------------------------------------------------------
    # 8.4.1 PLANO DO SERVIDOR
    # ----------------------------------------------------------
    plano_atual = "Starter"
    for key_data in users_db.get("keys", {}).values():
        if str(key_data.get("server_id", "")).strip() == str(server_id):
            plano_atual = key_data.get("plano", "Starter")
            break

    # ----------------------------------------------------------
    # 8.5 VALIDAÇÃO DISCORD GUILD
    # ----------------------------------------------------------
    discord_guild_id = None
    for key_data in users_db.get("keys", {}).values():
        if str(key_data.get("server_id", "")) == str(server_id):
            discord_guild_id = key_data.get("discord_guild_id", "")
            break

    portal_guilds = st.session_state.get("portal_discord_guilds", [])
    if discord_guild_id:
        if not validar_membro_discord(portal_guilds, discord_guild_id):
            st.error("❌ Você não é membro do servidor Discord oficial. Entre no servidor e tente novamente.")
            st.stop()

    # ----------------------------------------------------------
    # 8.6 VERIFICA GAMERTAG VINCULADA
    # ----------------------------------------------------------
    gamertag_vinculada = None
    for gt, info in players.items():
        if str(info.get("discord_id", "")) == str(discord_id):
            gamertag_vinculada = gt
            break

    if not gamertag_vinculada:
        st.markdown(
            f"""
            <div style="text-align:center; padding:30px 0 10px 0;">
                <h2 style="color:#00d4ff;">Bem-vindo, {discord_name}! 👋</h2>
                <p style="color:#555;">Para acessar o portal, vincule sua Gamertag primeiro.</p>
            </div>
            <style>
                div[data-testid="stForm"] label {{
                    color: #222222 !important;
                    font-weight: 600 !important;
                }}
                div[data-testid="stForm"] button[kind="primaryFormSubmit"],
                div[data-testid="stForm"] button[data-testid="stFormSubmitButton"] {{
                    background-color: #00d4ff !important;
                    color: #000000 !important;
                    font-weight: bold !important;
                    border: none !important;
                }}
            </style>
            """,
            unsafe_allow_html=True,
        )
        with st.form("form_vinculo"):
            gamertag = st.text_input("🎮 Gamertag (exatamente como aparece no console)", "")
            apelido = st.text_input("Apelido (opcional)", "")
            observacoes = st.text_area("Observações (opcional)", "")
            submitted = st.form_submit_button("✅ Vincular minha Gamertag", use_container_width=True, type="primary")

        if submitted:
            gamertag_clean = gamertag.strip()
            if not gamertag_clean:
                st.error("Preencha a Gamertag.")
            else:
                players[gamertag_clean] = {
                    "gamertag": gamertag_clean,
                    "apelido": apelido.strip(),
                    "discord_id": discord_id,
                    "observacoes": observacoes.strip(),
                }
                client_data["players"] = players
                clients_db[server_id] = client_data
                save_db(DB_CLIENTS, clients_db)
                st.session_state.portal_gamertag = gamertag_clean
                st.success(f"✅ Gamertag **{gamertag_clean}** vinculada com sucesso!")
                st.rerun()
        st.stop()

    st.session_state.portal_gamertag = gamertag_vinculada

    # ----------------------------------------------------------
    # 8.7 HEADER DO JOGADOR
    # ----------------------------------------------------------
    col_h1, col_h2, col_h3 = st.columns([3, 1, 1])

    with col_h1:
        st.markdown(
            f"""
            <div style="padding: 10px 0;">
                <span style="font-size:22px; font-weight:bold; color:#00d4ff;">
                    🎮 {gamertag_vinculada}
                </span>
                <span style="font-size:14px; color:#888; margin-left:12px;">
                    {server_nome}
                </span><br>
                <span style="font-size:13px; color:#666;">
                    Discord: {discord_name}
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col_h2:
        render_relogio()

    with col_h3:
        if st.button("🚪 Sair", use_container_width=True):
            for k in [
                "portal_discord_id",
                "portal_discord_name",
                "portal_discord_guilds",
                "portal_server_id",
                "portal_server_nome",
                "portal_gamertag",
                "portal_discord_avatar",
            ]:
                st.session_state.pop(k, None)
            st.rerun()

        st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)

        tema_atual = st.session_state.portal_tema
        novo_tema_label = st.selectbox(
            "",
            ["Escuro", "Claro"],
            index=0 if tema_atual == "dark" else 1,
            label_visibility="collapsed",
            key="select_tema_portal",
        )
        mapa_tema = {"Escuro": "dark", "Claro": "light"}
        tema_novo = mapa_tema.get(novo_tema_label, "dark")
        if tema_novo != tema_atual:
            st.session_state.portal_tema = tema_novo
            st.rerun()

    st.divider()

    # ----------------------------------------------------------
    # 8.8 ABAS DINÂMICAS POR PERFIL (GRC)
    # ----------------------------------------------------------
    
    # Define a lista base de títulos
    titulos_abas = ["🏠 Início", "🏦 Banco DzCoins", "🏆 Ranking", "🛒 Loja Virtual"]

    # Identifica se é o Administrador do Servidor (Dono da KeyUser)
    # Jogadores comuns entram via Discord e não possuem role 'client' neste contexto
    is_admin_servidor = st.session_state.get("role") == "client"

    if is_admin_servidor:
        titulos_abas.append("⚙️ Feeds / Bot")

    # Cria as abas dinamicamente
    abas_objetos = st.tabs(titulos_abas)

    # Atribuição das variáveis baseada na ordem
    tab_inicio = abas_objetos[0]
    tab_banco = abas_objetos[1]
    tab_ranking = abas_objetos[2]
    tab_loja = abas_objetos[3]

    # --- ABA INÍCIO ---
    with tab_inicio:
        col_a, col_b, col_c = st.columns(3)

        with col_a:
            st.markdown("#### 🌐 Players Online")
            ftp_cfg_online = get_client_ftp_config(client_data)
            nitrado_token_cliente = next(
                (
                    v.get("nitrado_token", "")
                    for k, v in users_db.get("keys", {}).items()
                    if (
                        str(v.get("server_id", "")).strip() == str(server_id).strip()
                        or str(k).strip() == str(server_id).strip()
                    ) and v.get("nitrado_token", "")
                ),
                ""
            )
            render_players_online(nitrado_id, ftp_cfg=ftp_cfg_online, nitrado_token=nitrado_token_cliente, feeds_config=client_data.get("feeds_config"))

        with col_b:
            st.markdown("#### 🔄 Reset do Servidor")
            render_reset_info(client_data)

        with col_c:
            st.markdown("#### 📊 Meu Resumo")
            
            ftp_cfg_stats = get_client_ftp_config(client_data)
            meu_stats = {}
            if ftp_cfg_stats:
                log_semanal = ftp_download_adm_files_weekly(ftp_cfg_stats, max_files=1)
                all_stats = parse_adm_semanal(log_semanal)
                meu_stats = all_stats.get(gamertag_vinculada, {})

            wallet_saldo = client_data.get("wallets", {}).get(gamertag_vinculada, {}).get("balance", 0)
            bank_saldo = client_data.get("bank", {}).get(gamertag_vinculada, {}).get("balance", 0)
            
            render_player_card(
                dados_stats=meu_stats,
                gamertag=gamertag_vinculada,
                nid=nitrado_id,
                discord_tag=discord_name,
                saldo_total=(wallet_saldo + bank_saldo)
            )

    # --- ABA BANCO ---
    with tab_banco:
        st.markdown(f"### 🏦 Banco DzCoins — {gamertag_vinculada}")
        clients_db_fresh = load_db(DB_CLIENTS, {})
        client_data_fresh = clients_db_fresh.get(server_id, {})
        render_banco(
            client_data_fresh,
            clients_db_fresh,
            server_id,
            gamertag_vinculada,
            plano_atual,
        )

    # --- ABA RANKING ---
    with tab_ranking:
        st.markdown("### 🏆 Ranking Semanal")
        if not plano_permite(plano_atual, "ranking_semanal"):
            bloquear_funcionalidade(plano_atual, "🏆 Ranking Semanal")
        elif not client_data.get("feeds_config", {}).get("ranking", True):
            st.warning("⚠️ O módulo de Ranking Semanal está desativado para este servidor.")
            st.info("O administrador do servidor pode reativá-lo na aba '⚙️ Feeds / Bot'.")
        else:
            clients_db_fresh = load_db(DB_CLIENTS, {})
            client_data_fresh = clients_db_fresh.get(server_id, {})
            if not client_data_fresh.get("ftp", {}).get("host", ""):
                users_db_fresh = load_db(DB_USERS, {"keys": {}})
                for _ku, _kd in users_db_fresh.get("keys", {}).items():
                    if str(_kd.get("server_id", "")).strip() == str(server_id).strip():
                        _alt = clients_db_fresh.get(_ku, {})
                        if _alt.get("ftp", {}).get("host", ""):
                            client_data_fresh = _alt
                            break
            render_ranking(client_data_fresh, gamertag_vinculada, clients_db_fresh, server_id)

    # --- ABA FEEDS / BOT (EXCLUSIVA ADMIN SERVIDOR) ---
    if is_admin_servidor:
        tab_feeds = abas_objetos[4]
        with tab_feeds:
            st.header("⚙️ Configuração de Feeds e Auditoria")
            st.info("Painel restrito para gestão de governança do bot e logs.")
            
            feeds = client_data.get("feeds_config", {})
            
            c_f1, c_f2 = st.columns(2)
            with c_f1:
                st.subheader("🛡️ Auditoria")
                feeds["glitch_subsolo"] = st.toggle("Glitch Subsolo", value=feeds.get("glitch_subsolo", True))
                feeds["glitch_fogueiras"] = st.toggle("Spam Fogueiras", value=feeds.get("glitch_fogueiras", True))
                feeds["glitch_hortas"] = st.toggle("Spam Hortas", value=feeds.get("glitch_hortas", True))
            with c_f2:
                st.subheader("📊 Analytics")
                feeds["mapa_calor"] = st.toggle("Mapa de Calor", value=feeds.get("mapa_calor", True))
                feeds["ranking_auto"] = st.toggle("Ranking Global", value=feeds.get("ranking_auto", True))
            
            st.divider()
            webhook_online = st.text_input("🌐 Webhook: Players Online", value=feeds.get("webhook_players_online", ""))
            webhook_admin = st.text_input("🛡️ Webhook: Alertas Staff", value=feeds.get("webhook_admin_logs", ""))
            
            if st.button("💾 Salvar Configurações de Governança"):
                feeds["webhook_players_online"] = webhook_online
                feeds["webhook_admin_logs"] = webhook_admin
                client_data["feeds_config"] = feeds
                save_db(DB_CLIENTS, st.session_state.db_clients)
                st.success("✅ Governança atualizada!")

        # --- ABA LOJA VIRTUAL ---
    with tab_loja:
        st.markdown("### 🛒 Loja Virtual")

        clients_db_loja = load_db(DB_CLIENTS, {})
        client_data_loja = clients_db_loja.get(server_id, {})
        loja = client_data_loja.get("loja", {})
        itens = [i for i in loja.get("itens", []) if i.get("ativo", True)]

        if not itens:
            st.info("A loja ainda não possui itens cadastrados ou ativos.")
        else:
            hora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")

            wallets_loja = client_data_loja.get("wallets", {})
            bank_loja = client_data_loja.get("bank", {})
            saldo_w = wallets_loja.get(gamertag_vinculada, {}).get("balance", 0)
            saldo_b = bank_loja.get(gamertag_vinculada, {}).get("balance", 0)

            col_sw, col_sb, col_st = st.columns(3)
            col_sw.metric("💰 Carteira", f"{saldo_w} DzCoins")
            col_sb.metric("🏦 Banco", f"{saldo_b} DzCoins")
            col_st.metric("💎 Total", f"{saldo_w + saldo_b} DzCoins")

            st.divider()

            categorias = sorted(set(i.get("categoria", "Geral") for i in itens))
            categorias_opcoes = ["Todas"] + categorias
            cat_sel = st.selectbox(
                "Filtrar por categoria",
                categorias_opcoes,
                key="loja_cat_sel",
            )

            itens_filtrados = (
                itens
                if cat_sel == "Todas"
                else [i for i in itens if i.get("categoria") == cat_sel]
            )

            st.markdown(f"**{len(itens_filtrados)} item(ns) disponível(is)**")
            st.divider()

            for item in itens_filtrados:
                with st.expander(
                    f"🎒 {item['nome']} — {item['preco']} DzCoins "
                    f"| Qtd: {item.get('quantidade', 1)} "
                    f"| {item.get('categoria', '')}",
                    expanded=False,
                ):
                    col_info, col_compra = st.columns([2, 3])

                    with col_info:
                        st.markdown(
                            f"""
                            <div style="background:#1a1a2e; border-radius:8px;
                                        padding:12px; border:1px solid #333;">
                                <div style="font-size:18px; font-weight:bold;
                                            color:#00d4ff; margin-bottom:8px;">
                                    {item['nome']}
                                </div>
                                <div style="font-size:13px; color:#aaa;">
                                    🏷️ Classe: <b style="color:#fff;">{item.get('classe','')}</b><br>
                                    📦 Quantidade: <b style="color:#fff;">{item.get('quantidade', 1)}</b><br>
                                    🗂️ Categoria: <b style="color:#fff;">{item.get('categoria','')}</b><br>
                                    💰 Preço: <b style="color:#FFD700;">{item['preco']} DzCoins</b>
                                </div>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

                        tem_saldo_w = saldo_w >= item["preco"]
                        tem_saldo_b = saldo_b >= item["preco"]

                        if not tem_saldo_w and not tem_saldo_b:
                            st.error(
                                f"❌ Saldo insuficiente. Você precisa de {item['preco']} DzCoins."
                            )
                        elif tem_saldo_w and not tem_saldo_b:
                            st.info("💰 Saldo disponível apenas na Carteira.")
                        elif not tem_saldo_w and tem_saldo_b:
                            st.info("🏦 Saldo disponível apenas no Banco.")
                        else:
                            st.success("✅ Saldo suficiente na Carteira e no Banco.")

                    with col_compra:
                        st.markdown("#### 🛍️ Finalizar Compra")

                        origem_pag = st.radio(
                            "Debitar de:",
                            ["💰 Carteira", "🏦 Banco"],
                            horizontal=True,
                            key=f"origem_{item['id']}",
                        )

                        st.markdown("#### 📍 Localização de entrega")
                        st.markdown(
                            """
                            <div style="
                                background:#141a24;
                                border-radius:6px;
                                padding:8px 12px;
                                border:1px solid #2a3650;
                                font-size:12px;
                                color:#9fb3c8;
                                margin-bottom:10px;
                            ">
                                O eixo <b style="color:#ffffff">Y</b> é calculado automaticamente com base nos dados locais do terreno do mapa configurado no servidor.
                                <br>
                                Caso a base local não esteja disponível, o sistema tenta usar o <b style="color:#ffffff">fallback do servidor</b>.
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

                        col_cx, col_cz = st.columns(2)
                        with col_cx:
                            coord_x = st.text_input(
                                "Eixo X",
                                placeholder="Ex: 4106.11",
                                key=f"coord_x_{item['id']}",
                            )
                        with col_cz:
                            coord_z = st.text_input(
                                "Eixo Z",
                                placeholder="Ex: 5179.62",
                                key=f"coord_z_{item['id']}",
                            )

                        # Calcula Y em background quando X e Z forem preenchidos
                        coord_y = None
                        dist_ref = None
                        fonte_y = None
                        detalhe_y = None

                        if coord_x.strip() and coord_z.strip():
                            try:
                                fx = float(coord_x.strip())
                                fz = float(coord_z.strip())
                                mapa_loja = loja.get("mapa_padrao", "Chernarus")
                                cache_key = f"y_cache_{server_id}_{mapa_loja}_{fx:.1f}_{fz:.1f}"

                                if cache_key not in st.session_state:
                                    ftp_cfg_loja = get_client_ftp_config(client_data_loja)

                                    resultado_y = resolver_y_loja(
                                        ftp_cfg=ftp_cfg_loja,
                                        x=fx,
                                        z=fz,
                                        mapa=mapa_loja,
                                    )
                                    st.session_state[cache_key] = resultado_y

                                resultado_y = st.session_state[cache_key]

                                coord_y = resultado_y.get("y")
                                dist_ref = resultado_y.get("distancia")
                                fonte_y = resultado_y.get("fonte")
                                detalhe_y = resultado_y.get("detalhe")

                                if coord_y is not None:
                                    fonte_raw = (fonte_y or "").strip().lower()
                                    badge_origem = "Fonte alternativa"
                                    detalhe_fonte = detalhe_y or "Referência não especificada"
                                    bg_box = "#1b1b1b"
                                    border_box = "#444444"
                                    cor_y = "#cccccc"

                                    if fonte_raw.startswith("local"):
                                        badge_origem = "🗺️ Terreno local"
                                        if "asc" in fonte_raw:
                                            detalhe_fonte = "Base local do mapa (ASC real)"
                                        elif "npy" in fonte_raw:
                                            detalhe_fonte = "Base local do mapa (NPY pré-processado)"
                                        elif "json" in fonte_raw:
                                            detalhe_fonte = "Base local do mapa (lookup JSON)"
                                        else:
                                            detalhe_fonte = "Base local do mapa carregada no portal"
                                        bg_box = "#0f1b12"
                                        border_box = "#1f5a34"
                                        cor_y = "#57ff9a"

                                    elif fonte_raw.startswith("ftp"):
                                        badge_origem = "🌐 Fallback do servidor"
                                        if dist_ref is not None:
                                            detalhe_fonte = f"Ponto de referência encontrado a {dist_ref:.0f}m"
                                        else:
                                            detalhe_fonte = detalhe_y or "Referência encontrada no servidor"
                                        bg_box = "#22170d"
                                        border_box = "#7a4b1f"
                                        cor_y = "#ffcc66"

                                    html_y = (
                                        f'<div style="background:{bg_box};border-radius:8px;padding:10px 12px;border:1px solid {border_box};font-size:12px;color:#b8c0cc;margin-bottom:8px;">'
                                        f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;">'
                                        f'<span style="font-size:12px;color:#d7dde8;">🧭 Eixo Y calculado automaticamente</span>'
                                        f'<span style="font-size:11px;padding:3px 8px;border-radius:999px;background:rgba(255,255,255,0.08);color:#fff;">{html.escape(str(badge_origem))}</span>'
                                        f'</div>'
                                        f'<div style="font-size:20px;font-weight:bold;color:{cor_y};margin-bottom:6px;">{coord_y:.4f}</div>'
                                        f'<div style="font-size:12px;color:#9fb0c3;">'
                                        f'<b style="color:#fff;">Origem:</b> {html.escape(str(fonte_y or "desconhecida"))}<br>'
                                        f'<b style="color:#fff;">Detalhe:</b> {html.escape(str(detalhe_fonte))}'
                                        f'</div></div>'
                                    )
                                    st.markdown(html_y, unsafe_allow_html=True)

                                else:
                                    st.warning("⚠️ Não foi possível calcular o Y. Verifique os dados do mapa ou o FTP configurado.")

                            except ValueError:
                                st.error("❌ X e Z devem ser números. Ex: 4106.11")

                        # Monta string final de coordenadas
                        if coord_x.strip() and coord_z.strip() and coord_y is not None:
                            coordenadas = f"{coord_x.strip()} {coord_y:.4f} {coord_z.strip()}"
                        else:
                            coordenadas = ""

                        st.markdown(
                            """
                            <div style="background:#1a1a2e; border-radius:6px;
                                        padding:8px 12px; border:1px solid #444;
                                        font-size:12px; color:#aaa; margin-bottom:10px;">
                                ⏰ O item será incluído no servidor no
                                <b style="color:#00d4ff;">próximo reset</b>
                                após a confirmação da compra.
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

                        confirmar = st.button(
                            f"✅ Confirmar Compra — {item['preco']} DzCoins",
                            key=f"comprar_{item['id']}",
                            use_container_width=True,
                            type="primary",
                        )

                        if confirmar:
                            if not coord_x.strip() or not coord_z.strip():
                                st.error("Informe o Eixo X e o Eixo Z antes de confirmar.")
                            elif coord_y is None:
                                st.error("Aguarde o cálculo do Eixo Y ou verifique se o FTP está configurado.")
                            else:
                                ok, msg = registrar_compra(
                                    clients_db_loja,
                                    server_id,
                                    gamertag_vinculada,
                                    item,
                                    origem_pag,
                                    coordenadas,
                                    hora_br,
                                )
                                if ok:
                                    mapa_loja = loja.get("mapa_padrao", "Chernarus")
                                    cache_key = (
                                        f"y_cache_{server_id}_{mapa_loja}_"
                                        f"{float(coord_x.strip()):.1f}_{float(coord_z.strip()):.1f}"
                                    )
                                    st.session_state.pop(cache_key, None)
                                    save_db(DB_CLIENTS, clients_db_loja)
                                    st.success(
                                        f"✅ Compra confirmada! **{item['nome']}** x{item.get('quantidade',1)} "
                                        f"será entregue em **X:{coord_x.strip()} "
                                        f"Y:{coord_y:.4f} "
                                        f"Z:{coord_z.strip()}** no próximo reset."
                                    )
                                    st.balloons()
                                    st.rerun()
                                else:
                                    st.error(msg)

            st.divider()

            col_titulo, col_acao = st.columns([3, 1])
            with col_titulo:
                st.markdown("### 📜 Minhas Compras")

            with col_acao:
                if st.button(
                    "🗑️ Limpar entregues",
                    key="limpar_compras_entregues",
                    use_container_width=True,
                ):
                    pedidos_atuais = client_data_loja.get("pedidos", [])
                    pedidos_filtrados = [
                        p
                        for p in pedidos_atuais
                        if not (
                            p.get("gamertag") == gamertag_vinculada
                            and p.get("status") == "Entregue"
                        )
                    ]

                    client_data_loja["pedidos"] = pedidos_filtrados
                    clients_db_loja[server_id] = client_data_loja
                    save_db(DB_CLIENTS, clients_db_loja)

                    st.success("Histórico de compras entregues limpo com sucesso!")
                    st.rerun()

            pedidos = client_data_loja.get("pedidos", [])
            meus_pedidos = [p for p in pedidos if p.get("gamertag") == gamertag_vinculada]

            st.caption("As compras mais recentes ficam visíveis neste console com rolagem.")

            if not meus_pedidos:
                compras_txt = "Você ainda não realizou nenhuma compra."
            else:
                linhas_compras = []

                for pedido in meus_pedidos[:200]:
                    status = pedido.get("status", "Aguardando Reset")
                    spawn_pendente = pedido.get("spawn_pendente", False)

                    if spawn_pendente:
                        status_txt = "PENDENTE RESET"
                        icone_status = "🟡"
                    elif status == "Entregue":
                        status_txt = "ENTREGUE"
                        icone_status = "🟢"
                    else:
                        status_txt = str(status).upper()
                        icone_status = "⚪"

                    linha = (
                        f"[{pedido.get('data_compra', '--')}] "
                        f"{pedido.get('item_nome', '?')} x{pedido.get('quantidade', 1)}\n"
                        f"💰 {pedido.get('preco', 0)} DzCoins | "
                        f"{pedido.get('origem_pagamento', '?')} | "
                        f"{icone_status} {status_txt}\n"
                        f"📍 {pedido.get('coordenadas', '?')}"
                    )

                    linhas_compras.append(linha)

                compras_txt = "\n\n".join(linhas_compras)

            st.markdown(
                f"""
                <div style="
                    background:#0f1117;
                    border:1px solid #2b3240;
                    border-radius:8px;
                    padding:12px;
                    height:340px;
                    overflow-y:auto;
                    font-family:Consolas, 'Courier New', monospace;
                    font-size:12px;
                    color:#d6e2f0;
                    white-space:pre-wrap;
                    line-height:1.55;
                ">{compras_txt}</div>
                """,
                unsafe_allow_html=True,
            )

if __name__ == "__main__":
    main()
