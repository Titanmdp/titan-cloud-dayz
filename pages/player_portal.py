import streamlit as st
import json
import os
import requests
import urllib.parse
import io
import re
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

# Import seguro do módulo de elevação do Chernarus
try:
    import sys

    if str(MAP_DATA_DIR) not in sys.path:
        sys.path.append(str(MAP_DATA_DIR))

    from chernarus_elevation import ChernarusHeightmap
except Exception as e:
    ChernarusHeightmap = None
    print(f"[Elevation] Módulo de elevação do Chernarus indisponível: {e}")

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


def get_local_elevation_by_map(x: float, z: float, mapa: str):
    """
    Dispatcher simples por mapa.
    Hoje: Chernarus local.
    Futuro: Livonia.
    """
    mapa_norm = (mapa or "").strip().lower()

    if mapa_norm in ("chernarus", "chernarusplus", "chernarus+"):
        return get_chernarus_elevation_local(x, z)

    return None, "mapa_sem_suporte_local"

def resolver_y_loja(ftp_cfg: dict | None, x: float, z: float, mapa: str):
    """
    Resolve a coordenada Y para compras da loja.

    Ordem de prioridade:
    1) Elevação local por mapa (Chernarus)
    2) FTP/cfgeventspawns.xml
    3) Falha controlada

    Retorna dict com:
    {
        "y": float | None,
        "fonte": str,
        "distancia": float | None,
        "detalhe": str
    }
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
# 4. FUNÇÕES NITRADO API
# =========================================================

def nitrado_headers():
    return {"Authorization": f"Bearer {NITRADO_TOKEN}"}

def get_players_online(nitrado_id: str) -> dict:
    if not NITRADO_TOKEN:
        return {"players": [], "total": 0, "erro": "Token não configurado"}
    try:
        url = f"{NITRADO_API}/services/{nitrado_id}/gameservers"
        resp = requests.get(url, headers=nitrado_headers(), timeout=10)
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


def parse_adm_conexoes(log_text: str) -> list:
    """
    Extrai eventos de conexão e desconexão do log .ADM.
    Retorna lista de dicts ordenada do mais recente.
    """
    eventos = []

    if not log_text or not log_text.strip():
        return eventos

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
            eventos.append({
                "tipo": "connected",
                "hora": hora,
                "dt": parse_dt(hora),
                "jogador": nome,
                "posicao": pos,
                "duracao": "",
                "icone": "🟢",
                "descricao": f"Conectou em {pos}",
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
            eventos.append({
                "tipo": "disconnected",
                "hora": hora,
                "dt": dt_disc,
                "jogador": nome,
                "posicao": pos,
                "duracao": duracao,
                "icone": "🔴",
                "descricao": f"Desconectou de {pos}" + (f" — Sessão: {duracao}" if duracao else ""),
            })
            continue

    eventos.sort(key=lambda x: x.get("dt") or datetime.min.replace(tzinfo=FUSO_BR), reverse=True)
    return eventos


def parse_adm_killfeed_pvp(log_text: str) -> list:
    """
    Extrai eventos de kill PvP do log .ADM.
    Formato esperado:
    HH:MM:SS | Player "Assassino" (...) killed Player "Vitima" (...)
    ou
    HH:MM:SS | Player "Vitima" (...)[HP: 0] hit by Player "Assassino" ... for X damage (Arma)
    """
    eventos = []

    if not log_text or not log_text.strip():
        return eventos

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

        # (Opcional) Padrão 3: died PvP com posição
        m = re_died_pvp.match(line)
        if m:
            hora, vitima, pos = m.group(1), m.group(2), m.group(3)
            arma = ultima_arma_pvp.pop(vitima, "Desconhecida")
            eventos.append({
                "tipo": "pvp_kill",
                "hora": hora,
                "dt": parse_dt(hora),
                "assassino": "",
                "vitima": vitima,
                "arma": arma,
                "dano": "",
                "parte_corpo": "",
                "posicao": pos,
                "icone": "💀",
                "descricao": f"{vitima} morreu (PvP) em {pos} — arma: {arma}",
            })
            continue

    eventos.sort(key=lambda x: x.get("dt") or datetime.min.replace(tzinfo=FUSO_BR), reverse=True)
    return eventos

# =========================================================
# 4.3 RANKING SEMANAL — ACUMULADO 7 DIAS
# =========================================================

def ftp_download_adm_files_weekly(ftp_cfg: dict, max_files: int = 7) -> str:
    """
    Baixa os últimos max_files arquivos .ADM via FTP e concatena o conteúdo.
    Retorna string com todos os logs unidos.
    """
    arquivos = ftp_list_adm_files(ftp_cfg)
    if not arquivos:
        return ""

    # Pega os últimos max_files arquivos (mais recentes primeiro)
    arquivos_semana = arquivos[:max_files]
    conteudo_total = ""

    try:
        with FTP() as ftp:
            ftp.connect(ftp_cfg["host"], ftp_cfg["port"], timeout=15)
            ftp.login(ftp_cfg["user"], ftp_cfg["pass"])
            ftp.cwd(DAYZ_LOG_DIR)

            for nome_arquivo in arquivos_semana:
                buffer = io.BytesIO()
                try:
                    ftp.retrbinary(f"RETR {nome_arquivo}", buffer.write)
                    texto = buffer.getvalue().decode("utf-8", errors="ignore")
                    conteudo_total += texto + "\n"
                except Exception:
                    continue
    except Exception:
        pass

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
            p["last_connect_dt"] = None

        # Hit por Infected (dano real)
        if key_hit_infected in line and not is_dead_line:
            m_hit = re_hit_infected.search(line)
            if m_hit:
                try:
                    if float(m_hit.group(1)) > 0:
                        p["pve_hits"] += 1
                except Exception:
                    pass

        # Suicídio / EmoteSuicide
        if key_suicide_emote in line or key_committed_sui in line:
            if not is_dead_line:
                p["pve_suicides"] += 1
            registrar_morte(p, dt_evento)

        # Morte real (DEAD died)
        if is_dead_line and "died." in line and key_committed_sui not in line:
            registrar_morte(p, dt_evento)
            p["pvp_deaths"] += 1

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
        # Se jogador ainda está vivo (sem morte registrada), estima sobrevivência atual
        if p.get("last_spawn_dt") and p.get("last_connect_dt"):
            agora = datetime.now(FUSO_BR)
            delta_atual = (agora - p["last_spawn_dt"]).total_seconds()
            if delta_atual > 0:
                p["current_survival_seconds"] = int(delta_atual)
        # XP final baseado em total_survival_seconds
        p["xp"] = round(p["total_survival_seconds"] / 60, 2)

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

def render_players_online(nitrado_id: str):
    @st.fragment(run_every=60)
    def _players():
        dados = get_players_online(nitrado_id)

        if "erro" in dados:
            st.warning(f"⚠️ Players Online indisponível: {dados['erro']}")
            return

        total = dados.get("total", 0)
        players = dados.get("players", [])
        maximo = dados.get("max", 0)

        st.markdown(
            f"""
            <div style="background:#1a1a2e; border-radius:10px; padding:14px;
                        border:1px solid #444; margin-bottom:8px;">
                <div style="font-size:13px; color:#aaa; margin-bottom:4px;">
                    🌐 Players Online
                </div>
                <div style="font-size:28px; font-weight:bold; color:#00ff88;">
                    {total}
                    <span style="font-size:14px; color:#666;">/ {maximo}</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        if total > 0 and players:
            with st.expander(f"Ver jogadores ({total})", expanded=False):
                for nome in players:
                    st.markdown(f"◆ `{nome}`")
        elif total > 0 and not players:
            st.caption(f"{total} jogador(es) online, lista de nomes indisponível.")
        else:
            st.caption("Nenhum jogador online no momento.")

    _players()

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

# =========================================================
# 6. ABA BANCO DZCOINS
# =========================================================

def render_banco(client_data: dict, clients_db: dict, server_id: str, gamertag: str):
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

    op = st.radio(
        "Operação",
        [
            "📋 Extrato",
            "➡️ Depositar (Carteira → Banco)",
            "⬅️ Sacar (Banco → Carteira)",
            "🔁 Transferir para outro jogador",
        ],
        horizontal=False,
        label_visibility="collapsed",
    )

    hora_br = datetime.now(FUSO_BR).strftime("%d/%m/%Y %H:%M")

    if op == "📋 Extrato":
        st.markdown("#### 📋 Extrato de movimentações")
        historico_comb = []
        for linha in wallet_reg.get("historico", []):
            historico_comb.append(("CARTEIRA", linha))
        for linha in bank_reg.get("historico", []):
            historico_comb.append(("BANCO", linha))

        if not historico_comb:
            st.info("Nenhuma movimentação registrada ainda.")
        else:
            for origem, linha in reversed(historico_comb[-30:]):
                icone = "💰" if origem == "CARTEIRA" else "🏦"
                st.markdown(f"{icone} `[{origem}]` {linha}")

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
        st.markdown("#### 🔁 Transferir DzCoins")
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
                    nivel = max(1, int(xp // 100) + 1)
                    barra_pct = int((xp / max(xp_max, 1)) * 100)
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
                                Nvl {nivel} &nbsp;|&nbsp; ⭐ {xp:.1f} XP
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
            col3.metric("⭐ XP", f"{meu.get('xp', 0.0):.1f}")
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

    client_data = clients_db.get(server_id, {})
    players = load_players_for_client(client_data)
    server_nome = st.session_state.get("portal_server_nome", "Servidor")
    nitrado_id = nitrado_id_map.get(server_id, server_id)

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
                <p style="color:#aaa;">Para acessar o portal, vincule sua Gamertag primeiro.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        with st.form("form_vinculo"):
            gamertag = st.text_input("🎮 Gamertag (exatamente como aparece no console)", "")
            apelido = st.text_input("Apelido (opcional)", "")
            observacoes = st.text_area("Observações (opcional)", "")
            submitted = st.form_submit_button("✅ Vincular minha Gamertag", use_container_width=True)

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
    # 8.8 ABAS PRINCIPAIS (todas dentro de main())
    # ----------------------------------------------------------
    tab_inicio, tab_banco, tab_ranking, tab_loja = st.tabs([
        "🏠 Início",
        "🏦 Banco DzCoins",
        "🏆 Ranking",
        "🛒 Loja Virtual",
    ])

    # --- ABA INÍCIO ---
    with tab_inicio:
        col_a, col_b, col_c = st.columns(3)

        with col_a:
            st.markdown("#### 🌐 Players Online")
            render_players_online(nitrado_id)

        with col_b:
            st.markdown("#### 🔄 Reset do Servidor")
            render_reset_info(client_data)

        with col_c:
            st.markdown("#### 📊 Meu Resumo")
            wallet_saldo = client_data.get("wallets", {}).get(
                gamertag_vinculada, {}
            ).get("balance", 0)
            bank_saldo = client_data.get("bank", {}).get(
                gamertag_vinculada, {}
            ).get("balance", 0)
            st.metric("💰 Carteira", f"{wallet_saldo} DzCoins")
            st.metric("🏦 Banco", f"{bank_saldo} DzCoins")
            st.metric("💎 Total", f"{wallet_saldo + bank_saldo} DzCoins")

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
        )

    # --- ABA RANKING ---
    with tab_ranking:
        st.markdown("### 🏆 Ranking Semanal")
        clients_db_fresh = load_db(DB_CLIENTS, {})
        client_data_fresh = clients_db_fresh.get(server_id, {})
        render_ranking(
            client_data_fresh,
            gamertag_vinculada,
            clients_db_fresh,
            server_id,
        )

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
                itens if cat_sel == "Todas"
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
                                    if fonte_y and fonte_y.startswith("local:"):
                                        qualidade = "🗺️ Dados locais do mapa"
                                        referencia_txt = "Referência local"
                                    elif dist_ref is not None:
                                        qualidade = (
                                            "✅ Alta precisão"   if dist_ref < 300 else
                                            "⚠️ Precisão média" if dist_ref < 800 else
                                            "🔴 Precisão baixa"
                                        )
                                        referencia_txt = f"Referência a <b>{dist_ref:.0f}m</b>"
                                    else:
                                        qualidade = "ℹ️ Fonte alternativa"
                                        referencia_txt = "Referência não disponível"

                                    st.markdown(
                                        f"""
                                        <div style="background:#0d1f0d; border-radius:6px;
                                                    padding:8px 12px; border:1px solid #1a4a1a;
                                                    font-size:12px; color:#aaa; margin-bottom:6px;">
                                            🧭 Eixo Y calculado automaticamente:
                                            <b style="color:#00ff88;">{coord_y:.4f}</b>
                                            &nbsp;|&nbsp; {referencia_txt}
                                            &nbsp;|&nbsp; {qualidade}
                                            <br>
                                            <span style="color:#7fd1ff;">Fonte: {fonte_y or 'desconhecida'}</span>
                                        </div>
                                        """,
                                        unsafe_allow_html=True,
                                    )
                                else:
                                    st.warning(
                                        "⚠️ Não foi possível calcular o Y. Verifique os dados do mapa ou o FTP configurado."
                                    )
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
                                    # Limpa cache das coordenadas após compra confirmada
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
                        p for p in pedidos_atuais
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

            if not meus_pedidos:
                st.info("Você ainda não realizou nenhuma compra.")
            else:
                for pedido in meus_pedidos[:20]:
                    status = pedido.get("status", "Aguardando Reset")
                    cor_status = (
                        "#00ff88" if status == "Entregue"
                        else "#FFD700" if status == "Aguardando Reset"
                        else "#aaa"
                    )
                    st.markdown(
                        f"""
                        <div style="background:#1a1a2e; border-radius:8px;
                                    padding:10px 14px; border-left:3px solid {cor_status};
                                    margin-bottom:6px;">
                            <span style="font-size:13px; color:#00d4ff; font-weight:bold;">
                                {pedido.get('data_compra','--')} — {pedido.get('item_nome','?')}
                                x{pedido.get('quantidade',1)}
                            </span><br>
                            <span style="font-size:12px; color:#aaa;">
                                💰 {pedido.get('preco',0)} DzCoins ({pedido.get('origem_pagamento','?')})
                                &nbsp;|&nbsp; 📍 {pedido.get('coordenadas','?')}
                                &nbsp;|&nbsp;
                                <span style="color:{cor_status};">● {status}</span>
                            </span>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

if __name__ == "__main__":
    main()
