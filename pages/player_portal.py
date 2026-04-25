import streamlit as st
import json
import os
import requests
import urllib.parse
import io
import re
from datetime import datetime, timezone, timedelta
from ftplib import FTP

# =========================================================
# 1. CONFIG / AMBIENTE / CONSTANTES
# =========================================================

IS_DEV = os.environ.get("IS_DEV", "False") == "True"

if os.path.exists("/var/data"):
    DB_USERS = "/var/data/users_db.json"
    DB_CLIENTS = "/var/data/clients_data.json"
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
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

    # Regex
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

    # Rastreia última arma de suicídio por jogador
    ultima_arma_suicidio = {}
    # Rastreia última posição conhecida por jogador
    ultima_pos = {}

    # Extrai posição de linhas genéricas
    re_pos = re.compile(r'pos=<([\d., -]+)>')

    for line in log_text.splitlines():
        line = line.strip()
        if not line:
            continue

        # Atualiza última posição conhecida
        m_pos = re_pos.search(line)
        m_nome = re.search(r'Player "([^"]+)"', line)
        if m_pos and m_nome:
            ultima_pos[m_nome.group(1)] = m_pos.group(1)

        def parse_dt(tstr):
            try:
                dt = datetime.strptime(f"{log_date} {tstr}", "%Y-%m-%d %H:%M:%S")
                return dt.replace(tzinfo=FUSO_BR)
            except Exception:
                return None

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

    # Ordena do mais recente para o mais antigo
    eventos.sort(key=lambda x: x.get("dt") or datetime.min.replace(tzinfo=FUSO_BR), reverse=True)
    return eventos


def parse_adm_conexoes(log_text: str) -> list:
    """
    Extrai eventos de conexão e desconexão do log .ADM.
    Retorna lista de dicts ordenada do mais recente.
    """
    eventos = []

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

    # Rastreia horário de conexão para calcular duração
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

    # Padrão 1: killed
    re_killed = re.compile(
        r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)" \([^)]*\) killed Player "([^"]+)"'
    )
    # Padrão 2: hit by Player com HP 0
    re_hit_pvp = re.compile(
        r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)" \([^)]*\)\[HP: 0\] '
        r'hit by Player "([^"]+)" .* for ([\d.]+) damage \(([^)]+)\)'
    )
    # Padrão 3: died após hit PvP
    re_died_pvp = re.compile(
        r'^(\d{2}:\d{2}:\d{2}) \| Player "([^"]+)" \(DEAD\) '
        r'\([^)]*pos=<([^>]+)>\) died'
    )

    ultima_arma_pvp = {}

    for line in log_text.splitlines():
        line = line.strip()
        if not line:
            continue

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

    eventos.sort(key=lambda x: x.get("dt") or datetime.min.replace(tzinfo=FUSO_BR), reverse=True)
    return eventos

    # ---- funções auxiliares (dentro do escopo correto) ----

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

        if key_connected in line:
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

def render_ranking(client_data: dict, gamertag_vinculada: str):
    ftp_cfg = get_client_ftp_config(client_data)
    if not ftp_cfg:
        st.warning(
            "FTP do servidor não está configurado para este cliente. "
            "Peça ao admin para configurar no painel."
        )
        return

    @st.fragment(run_every=300)
    def _ranking(ftp_cfg, gamertag_vinculada):
        with st.spinner("Carregando dados de ranking a partir dos logs do servidor..."):
            log_text, err = ftp_download_latest_adm(ftp_cfg)

        if err or not log_text:
            st.warning("Não foi possível carregar os logs do servidor.")
            st.caption(f"Detalhes: {err or 'log vazio'}")
            return

        parsed = parse_adm_sessions_and_pve(log_text)

        if not isinstance(parsed, dict):
            st.warning("Formato de log não reconhecido.")
            return

        pstats = parsed.get("players", {})
        if not pstats:
            st.info("Nenhuma estatística encontrada no log .ADM mais recente.")
            return

        ranking_play = []
        for nome, dados in pstats.items():
            total_play = dados.get("total_play_seconds", 0)
            ranking_play.append({
                "Jogador": nome,
                "Tempo de jogo": format_seconds_hhmmss(total_play),
                "_segundos": total_play,
                "Sessões": dados.get("session_count", 0),
                "Hits PvE": dados.get("pve_hits", 0),
                "Suicídios": dados.get("pve_suicides", 0),
            })

        ranking_play_sorted = sorted(
            ranking_play, key=lambda x: x["_segundos"], reverse=True
        )[:10]

        col_r1, col_r2 = st.columns(2)

        with col_r1:
            st.markdown("#### ⏱️ Tempo de jogo total — Top 10")
            if ranking_play_sorted:
                st.table([{
                    "#": idx + 1,
                    "Jogador": r["Jogador"],
                    "Tempo de jogo": r["Tempo de jogo"],
                    "Sessões": r["Sessões"],
                } for idx, r in enumerate(ranking_play_sorted)])
            else:
                st.info("Sem dados de tempo de jogo neste log.")

        with col_r2:
            st.markdown("#### 🧟 Hits PvE & Suicídios — Top 10")
            ranking_pve = sorted(
                ranking_play, key=lambda x: x["Hits PvE"], reverse=True
            )[:10]
            if ranking_pve:
                st.table([{
                    "#": idx + 1,
                    "Jogador": r["Jogador"],
                    "Hits PvE": r["Hits PvE"],
                    "Suicídios": r["Suicídios"],
                } for idx, r in enumerate(ranking_pve)])
            else:
                st.info("Sem dados de PvE neste log.")

        st.markdown("---")
        st.markdown("#### 👤 Meu desempenho no log atual")

        meu_reg = next(
            (r for r in ranking_play if r["Jogador"] == gamertag_vinculada),
            None,
        )
        if not meu_reg:
            st.info("Ainda não há dados seus neste log (nenhuma sessão registrada).")
        else:
            col_m1, col_m2, col_m3, col_m4 = st.columns(4)
            col_m1.metric("⏱️ Tempo de jogo", meu_reg["Tempo de jogo"])
            col_m2.metric("🔁 Sessões", meu_reg["Sessões"])
            col_m3.metric("🧟 Hits PvE", meu_reg["Hits PvE"])
            col_m4.metric("💀 Suicídios", meu_reg["Suicídios"])

    _ranking(ftp_cfg, gamertag_vinculada)

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

    # ----------------------------------------------------------
    # 8.4 SELEÇÃO DO SERVIDOR
    # ----------------------------------------------------------
    nome_para_server_id = {}
    nitrado_id_map = {}
    for keyuser, data in users_db.get("keys", {}).items():
        server_name = str(data.get("server", "")).strip()
        server_id = str(data.get("server_id", "")).strip() or keyuser
        nid = str(data.get("server_id", "")).strip()
        if server_name and server_id:
            nome_para_server_id[server_name.lower()] = server_id
            nitrado_id_map[server_id] = nid

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
                "portal_discord_id", "portal_discord_name", "portal_discord_guilds",
                "portal_server_id", "portal_server_nome", "portal_gamertag",
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
    tab_inicio, tab_banco, tab_ranking, tab_pvp, tab_pve, tab_conexao = st.tabs([
        "🏠 Início",
        "🏦 Banco DzCoins",
        "🏆 Ranking",
        "⚔️ Killfeed PvP",
        "🧟 Killfeed PvE",
        "🔌 Conexão",
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
        render_banco(client_data_fresh, clients_db_fresh, server_id, gamertag_vinculada)

    # --- ABA RANKING ---
    with tab_ranking:
        st.markdown("### 🏆 Ranking — Tempo de Jogo & PvE")
        render_ranking(client_data, gamertag_vinculada)

    # --- ABA KILLFEED PVP ---
    with tab_pvp:
        st.markdown("### ⚔️ Killfeed PvP")

        ftp_cfg = get_client_ftp_config(client_data)
        if not ftp_cfg:
            st.warning("FTP não configurado. Peça ao admin para configurar no painel.")
        else:
            @st.fragment(run_every=60)
            def _killfeed_pvp(ftp_cfg):
                with st.spinner("Carregando eventos PvP..."):
                    log_text, err = ftp_download_latest_adm(ftp_cfg)

                if err or not log_text:
                    st.warning("Não foi possível carregar o log do servidor.")
                    st.caption(f"Detalhes: {err or 'log vazio'}")
                    return

                eventos = parse_adm_killfeed_pvp(log_text)

                if not eventos:
                    st.info("Nenhum evento PvP registrado no log atual.")
                    st.caption("Os eventos aparecerão aqui assim que ocorrerem no servidor.")
                    return

                st.caption(f"Total de eventos PvP: {len(eventos)} — atualizado a cada 60s")
                st.divider()

                for ev in eventos:
                    col_i, col_d = st.columns([1, 8])
                    with col_i:
                        st.markdown(
                            f"<div style='font-size:28px; text-align:center;'>{ev['icone']}</div>",
                            unsafe_allow_html=True,
                        )
                    with col_d:
                        st.markdown(
                            f"""
                            <div style="background:#1a1a2e; border-radius:8px; padding:10px 14px;
                                        border-left:3px solid #ff4444; margin-bottom:6px;">
                                <span style="font-size:13px; color:#ff6666; font-weight:bold;">
                                    {ev['hora']} — {ev['descricao']}
                                </span><br>
                                <span style="font-size:11px; color:#888;">
                                    🗡️ {ev.get('assassino','?')} → 💀 {ev.get('vitima','?')}
                                    {f" | 🔫 {ev['arma']}" if ev.get('arma') and ev['arma'] != 'Desconhecida' else ""}
                                    {f" | 📍 {ev['posicao']}" if ev.get('posicao') else ""}
                                </span>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

            _killfeed_pvp(ftp_cfg)

    # --- ABA KILLFEED PVE ---
    with tab_pve:
        st.markdown("### 🧟 Killfeed PvE")

        ftp_cfg = get_client_ftp_config(client_data)
        if not ftp_cfg:
            st.warning("FTP não configurado. Peça ao admin para configurar no painel.")
        else:
            filtro_pve = st.radio(
                "Filtrar por tipo:",
                ["Todos", "💀 Apenas Mortes", "🧟 Apenas Hits"],
                horizontal=True,
                key="filtro_pve",
            )

            @st.fragment(run_every=60)
            def _killfeed_pve(ftp_cfg, filtro_pve):
                with st.spinner("Carregando eventos PvE..."):
                    log_text, err = ftp_download_latest_adm(ftp_cfg)

                if err or not log_text:
                    st.warning("Não foi possível carregar o log do servidor.")
                    st.caption(f"Detalhes: {err or 'log vazio'}")
                    return

                eventos = parse_adm_killfeed_pve(log_text)

                if filtro_pve == "💀 Apenas Mortes":
                    eventos = [e for e in eventos if e["tipo"] == "morte_pve"]
                elif filtro_pve == "🧟 Apenas Hits":
                    eventos = [e for e in eventos if e["tipo"] == "hit_pve"]

                if not eventos:
                    st.info("Nenhum evento PvE encontrado com esse filtro.")
                    return

                st.caption(f"Total de eventos: {len(eventos)} — atualizado a cada 60s")
                st.divider()

                for ev in eventos:
                    cor_borda = "#ff4444" if ev["tipo"] == "morte_pve" else "#ff8800"
                    col_i, col_d = st.columns([1, 8])
                    with col_i:
                        st.markdown(
                            f"<div style='font-size:28px; text-align:center;'>{ev['icone']}</div>",
                            unsafe_allow_html=True,
                        )
                    with col_d:
                        st.markdown(
                            f"""
                            <div style="background:#1a1a2e; border-radius:8px; padding:10px 14px;
                                        border-left:3px solid {cor_borda}; margin-bottom:6px;">
                                <span style="font-size:13px; color:#ffaa44; font-weight:bold;">
                                    {ev['hora']} — {ev['jogador']}
                                </span><br>
                                <span style="font-size:12px; color:#ccc;">
                                    {ev['descricao']}
                                </span>
                                {f"<br><span style='font-size:11px; color:#888;'>📍 {ev['posicao']}</span>" if ev.get('posicao') else ""}
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

            _killfeed_pve(ftp_cfg, filtro_pve)

    # --- ABA CONEXÃO ---
    with tab_conexao:
        st.markdown("### 🔌 Conexões & Desconexões")

        ftp_cfg = get_client_ftp_config(client_data)
        if not ftp_cfg:
            st.warning("FTP não configurado. Peça ao admin para configurar no painel.")
        else:
            filtro_con = st.radio(
                "Filtrar por tipo:",
                ["Todos", "🟢 Conectou", "🔴 Desconectou"],
                horizontal=True,
                key="filtro_con",
            )

            @st.fragment(run_every=60)
            def _conexoes(ftp_cfg, filtro_con):
                with st.spinner("Carregando eventos de conexão..."):
                    log_text, err = ftp_download_latest_adm(ftp_cfg)

                if err or not log_text:
                    st.warning("Não foi possível carregar o log do servidor.")
                    st.caption(f"Detalhes: {err or 'log vazio'}")
                    return

                eventos = parse_adm_conexoes(log_text)

                if filtro_con == "🟢 Conectou":
                    eventos = [e for e in eventos if e["tipo"] == "connected"]
                elif filtro_con == "🔴 Desconectou":
                    eventos = [e for e in eventos if e["tipo"] == "disconnected"]

                if not eventos:
                    st.info("Nenhum evento de conexão encontrado.")
                    return

                st.caption(f"Total de eventos: {len(eventos)} — atualizado a cada 60s")
                st.divider()

                for ev in eventos:
                    cor_borda = (
                        "#00ff88" if ev["tipo"] == "connected"
                        else "#ff4444" if ev["tipo"] == "disconnected"
                        else "#888888"
                    )
                    col_i, col_d = st.columns([1, 8])
                    with col_i:
                        st.markdown(
                            f"<div style='font-size:24px; text-align:center;'>{ev['icone']}</div>",
                            unsafe_allow_html=True,
                        )
                    with col_d:
                        st.markdown(
                            f"""
                            <div style="background:#1a1a2e; border-radius:8px; padding:10px 14px;
                                        border-left:3px solid {cor_borda}; margin-bottom:6px;">
                                <span style="font-size:13px; color:#00d4ff; font-weight:bold;">
                                    {ev['hora']} — {ev['jogador']}
                                </span><br>
                                <span style="font-size:12px; color:#ccc;">
                                    {ev['descricao']}
                                </span>
                            </div>
                            """,
                            unsafe_allow_html=True,
                        )

            _conexoes(ftp_cfg, filtro_con)

if __name__ == "__main__":
    main()
