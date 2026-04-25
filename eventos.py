import streamlit as st
st.set_page_config(
    page_title="Titan Cloud Pro",
    page_icon="🎮",
    layout="wide",
)
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
from email.message import EmailMessage
from datetime import datetime, timedelta, timezone
from streamlit_javascript import st_javascript


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

# DEBUG TEMPORÁRIO PARA VER OS ARQUIVOS USADOS
st.write("DEBUG DB_USERS path:", DB_USERS)
st.write("DEBUG DB_CLIENTS path:", DB_CLIENTS)

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
    "Pro": 10,
    "Enterprise": 999,
}

# Caminhos padrão do types.xml por mapa no servidor DayZ
TYPES_REMOTE_PATHS = {
    "Chernarus": "mpmissions/dayzOffline.chernarusplus/db",
    "Livonia": "mpmissions/dayzOffline.enoch/db",
}

# --- BANCO DE DADOS (JSON) / UPLOADS ---
UPLOAD_DIR = "uploads"
if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)


# =========================================================
# 2. FUNÇÕES UTILITÁRIAS / INFRA
# =========================================================

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
            url = "https://titan-cloud-dayz.onrender.com"
            requests.get(url, timeout=10)
        except Exception:
            pass
        time.sleep(600)


threading.Thread(target=manter_vivo, daemon=True).start()


def load_db(file, default_data):
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
    # Só bloqueia se data for None; dicionários vazios ainda podem ser salvos
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

# ---------- HELPERS TYPES.XML (ECONOMIA) ----------

def parse_types_xml(xml_bytes):
    """
    Recebe bytes de um types.xml e devolve:
    - tree: objeto ET.ElementTree
    - root: elemento raiz
    - df: DataFrame com colunas principais para edição
    """
    tree = ET.ElementTree(ET.fromstring(xml_bytes))
    root = tree.getroot()
    rows = []

    for t in root.findall("type"):
        name = t.get("name", "")
        cat = None
        cat_elem = t.find("category")
        if cat_elem is not None:
            cat = cat_elem.get("name")

        def _get_int(tag, default=None):
            elem = t.find(tag)
            if elem is not None and elem.text is not None and elem.text.strip() != "":
                try:
                    return int(elem.text.strip())
                except:
                    return default
            return default

        nominal = _get_int("nominal", 0)
        min_v = _get_int("min", 0)
        lifetime = _get_int("lifetime", 0)

        rows.append(
            {
                "name": name,
                "category": cat,
                "nominal": nominal,
                "min": min_v,
                "lifetime": lifetime,
            }
        )

    df = pd.DataFrame(rows)
    return tree, root, df  # [web:65]

def apply_df_to_types_xml(tree, root, df):
    """
    Aplica as alterações do DataFrame de volta no XML
    e devolve bytes do novo types.xml.
    """
    df_indexed = df.set_index("name")

    for t in root.findall("type"):
        name = t.get("name", "")
        if name not in df_indexed.index:
            continue
        row = df_indexed.loc[name]

        def _set_int(tag, value):
            if pd.isna(value):
                return
            elem = t.find(tag)
            if elem is None:
                elem = ET.SubElement(t, tag)
            elem.text = str(int(value))

        _set_int("nominal", row.get("nominal"))
        _set_int("min", row.get("min"))
        _set_int("lifetime", row.get("lifetime"))

    xml_bytes = ET.tostring(root, encoding="utf-8", method="xml")
    header = b'<?xml version="1.0" encoding="utf-8"?>\n'
    return header + xml_bytes

def disparar_ftp_pro(client_id, acao, filename, local_path, mapa_path):
    db_atual = load_db(DB_CLIENTS, {})
    if client_id not in db_atual:
        return False, "Erro"

    conf = db_atual[client_id]["ftp"]
    try:
        ftp = ftplib.FTP()
        ftp.connect(conf["host"], int(conf["port"]), timeout=15)
        ftp.login(conf["user"], conf["pass"])
        ftp.cwd(mapa_path)
        if acao == "UPLOAD":
            with open(local_path, "rb") as f:
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

def enviar_types_via_ftp(client_id, local_path, mapa):
    """
    Envia o arquivo types.xml já salvo em local_path
    para o caminho correto no servidor, de acordo com o mapa.
    """
    db_atual = load_db(DB_CLIENTS, {})
    if client_id not in db_atual:
        return False, "Cliente não encontrado"

    conf = db_atual[client_id]["ftp"]
    remote_dir = TYPES_REMOTE_PATHS.get(mapa)
    if not remote_dir:
        return False, f"Caminho remoto não configurado para o mapa {mapa}"

    try:
        ftp = ftplib.FTP()
        ftp.connect(conf["host"], int(conf["port"]), timeout=15)
        ftp.login(conf["user"], conf["pass"])
        ftp.cwd(remote_dir)

        filename = "types.xml"  # nome padrão no servidor
        with open(local_path, "rb") as f:
            ftp.storbinary(f"STOR {filename}", f)

        ftp.quit()
        return True, "Sucesso"
    except Exception as e:
        return False, str(e)

def enviar_globals_via_ftp(client_id, local_path, mapa):
    """
    Envia o arquivo globals.xml já salvo em local_path
    para o caminho correto no servidor, de acordo com o mapa.
    """
    db_atual = load_db(DB_CLIENTS, {})
    if client_id not in db_atual:
        return False, "Cliente não encontrado"

    conf = db_atual[client_id]["ftp"]
    remote_dir = TYPES_REMOTE_PATHS.get(mapa)
    if not remote_dir:
        return False, f"Caminho remoto não configurado para o mapa {mapa}"

    try:
        ftp = ftplib.FTP()
        ftp.connect(conf["host"], int(conf["port"]), timeout=15)
        ftp.login(conf["user"], conf["pass"])
        ftp.cwd(remote_dir)

        filename = "globals.xml"  # nome padrão no servidor
        with open(local_path, "rb") as f:
            ftp.storbinary(f"STOR {filename}", f)

        ftp.quit()
        return True, "Sucesso"
    except Exception as e:
        return False, str(e)

def pro_worker():
    while True:
        try:
            now = get_hora_brasilia()
            hoje, agora = now.strftime("%d/%m/%Y"), now.strftime("%H:%M")
            db_all = load_db(DB_CLIENTS, {})
            mudou = False
            for c_id, c_info in db_all.items():
                for ag in c_info.get("agendas", []):
                    if (
                        ag["data"] == hoje
                        and ag["in"] == agora
                        and ag.get("status") == "Aguardando"
                    ):
                        success, _ = disparar_ftp_pro(
                            c_id, "UPLOAD", ag["file"], ag["local_path"], ag["path"]
                        )
                        ag["status"] = "Ativo" if success else "Erro"
                        mudou = True
                    if (
                        ag["data"] == hoje
                        and ag["out"] == agora
                        and ag.get("status") == "Ativo"
                    ):
                        disparar_ftp_pro(
                            c_id, "DELETE", ag["file"], ag["local_path"], ag["path"]
                        )
                        ag["status"] = "Finalizado"
                        mudou = True
            if mudou:
                save_db(DB_CLIENTS, db_all)
        except Exception:
            pass

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

if "db_clients" not in st.session_state:
    st.session_state.db_clients = load_db(DB_CLIENTS, {})

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "user_key" not in st.session_state:
    st.session_state.user_key = None
if "view_mode" not in st.session_state:
    st.session_state.view_mode = "admin"


# =========================================================
# 4. TELA DE LOGIN
# =========================================================

if not st.session_state.authenticated:
    st.title("🔑 Titan Cloud - Login")

    dados_geo = buscar_localizacao_cliente()
    login_key = st.text_input("Insira sua KeyUser", type="password")

    if st.button("Entrar no Painel", use_container_width=True):
        ok, cargo = validar_acesso(login_key)

        if ok:
            token_sessao = secrets.token_hex(8)

            if dados_geo:
                local_final = f"{dados_geo['cidade']} - {dados_geo['estado']}"
            else:
                local_final = "Localização não capturada"

            # Se for cliente, atualiza informações de acesso no users_db
            if cargo == "client":
                # Garante que db_users está carregado
                db_users = st.session_state.db_users

                # Recupera dados da key e o server_id associado
                key_data = db_users.get("keys", {}).get(login_key, {})
                server_id = key_data.get("server_id")

                if not server_id:
                    st.error(
                        "Esta KeyUser não possui um ID de servidor associado (server_id).\n"
                        "Registre ou atualize o cliente pelo painel de administração."
                    )
                    st.stop()

                # Atualiza logs de acesso
                db_users["keys"][login_key]["last_session"] = token_sessao
                db_users["keys"][login_key]["local"] = local_final
                db_users["keys"][login_key]["last_login"] = (
                    get_hora_brasilia().strftime("%d/%m/%Y %H:%M:%S")
                )
                save_db(DB_USERS, db_users)

                # Guarda server_id na sessão (ponte para clients_data.json)
                st.session_state.server_id = server_id

            # Para admin, pode não haver server_id direto
            st.session_state.authenticated = True
            st.session_state.user_key = login_key
            st.session_state.role = cargo
            st.session_state.session_token = token_sessao
            st.session_state.view_mode = "admin" if cargo == "admin" else "client"

            st.rerun()
        else:
            st.error(cargo)

    st.stop()


# =========================================================
# 5. ÁREA DO ADMINISTRADOR
# =========================================================

if st.session_state.role == "admin" and st.session_state.view_mode == "admin":
    with st.sidebar:
        st.subheader("🛡️ Menu Admin")
        if st.button("🚀 Usar Sistema (Modo Teste)", use_container_width=True):
            st.session_state.view_mode = "client"
            st.rerun()
        if st.button("🔴 Logout (Admin)", use_container_width=True):
            st.session_state.authenticated = False
            st.rerun()

    st.title("🛡️ Painel de Controle - Administrador")

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
                        #    Se tiver ID Nitrado, usa ele; senão gera aleatório
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
                    if st.button(
                        "🚫 Banir Acesso (Expirar Key)",
                        key=f"ban_{k}",
                        type="primary",
                        use_container_width=True,
                    ):
                        v["expires"] = (
                            get_hora_brasilia() - timedelta(days=1)
                        ).strftime("%d/%m/%Y")
                        save_db(DB_USERS, st.session_state.db_users)
                        st.warning(f"O acesso de {v['server']} foi bloqueado.")
                        st.rerun()

                st.divider()

                c_edit1, c_edit2 = st.columns(2)
                with c_edit1:
                    st.markdown("#### 📝 Informações e Plano")
                    new_n = st.text_input(
                        "Editar Nome", value=v["server"], key=f"n_{k}"
                    )
                    new_p = st.selectbox(
                        "Trocar Plano",
                        list(PLANOS.keys()),
                        index=list(PLANOS.keys()).index(v.get("plano", "Starter")),
                        key=f"p_{k}",
                    )
                    new_lim = st.number_input(
                        "Ajustar Limite",
                        min_value=1,
                        value=int(limite_final),
                        key=f"lim_{k}",
                    )

                    st.markdown("#### 📧 Contatos de Notificação")
                    new_mail = st.text_input(
                        "E-mail do Cliente", value=v.get("email", ""), key=f"mail_{k}"
                    )
                    new_wa = st.text_input(
                        "WhatsApp (com DDD)",
                        value=v.get("whatsapp", ""),
                        key=f"wa_{k}",
                    )

                    st.markdown("#### 🎮 Integração Discord")
                    new_guild = st.text_input(
                        "ID do Servidor Discord (Guild ID)",
                        value=v.get("discord_guild_id", ""),
                        key=f"guild_{k}",
                        help=(
                            "ID numérico do servidor Discord do administrador. "
                            "Para obter: Discord > Configurações > Avançado > Modo desenvolvedor ativo. "
                            "Depois clique com botão direito no servidor > Copiar ID do servidor."
                        ),
                    )

                    if st.button(
                        "💾 Salvar Alterações",
                        key=f"bn_{k}",
                        use_container_width=True,
                    ):
                        st.session_state.db_users["keys"][k]["server"] = new_n
                        st.session_state.db_users["keys"][k]["plano"] = new_p
                        st.session_state.db_users["keys"][k]["limite_extra"] = new_lim
                        st.session_state.db_users["keys"][k]["email"] = new_mail
                        st.session_state.db_users["keys"][k]["whatsapp"] = new_wa
                        st.session_state.db_users["keys"][k]["discord_guild_id"] = new_guild.strip()
                        save_db(DB_USERS, st.session_state.db_users)
                        st.success("Dados atualizados!")
                        st.rerun()

                with c_edit2:
                    st.markdown("#### 📅 Validade do Acesso")
                    st.write(f"**Expira em:** {v['expires']} ({dias_rest} dias)")
                    add_d = st.number_input(
                        "Adicionar dias", min_value=1, value=30, key=f"d_{k}"
                    )
                    if st.button(
                        "➕ Estender/Renovar",
                        key=f"bd_{k}",
                        use_container_width=True,
                    ):
                        nova_data = (
                            dt_exp_check + timedelta(days=add_d)
                        ).strftime("%d/%m/%Y")
                        st.session_state.db_users["keys"][k]["expires"] = nova_data
                        save_db(DB_USERS, st.session_state.db_users)
                        st.success(f"Estendido para {nova_data}!")
                        st.rerun()

                st.divider()

                if st.button(
                    "🗑️ EXCLUIR CLIENTE PERMANENTEMENTE",
                    key=f"del_{k}",
                    type="primary",
                    use_container_width=True,
                ):
                    del st.session_state.db_users["keys"][k]
                    if k in st.session_state.db_clients:
                        del st.session_state.db_clients[k]
                    save_db(DB_USERS, st.session_state.db_users)
                    save_db(DB_CLIENTS, st.session_state.db_clients)
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
                "🚀 Restaurar Dados Agora", use_container_width=True, type="primary"
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

    st.stop()


# =========================================================
# 6. ÁREA DO CLIENTE
# =========================================================

user_id = st.session_state.user_key

db_disco_clients = load_db(DB_CLIENTS, {})
db_disco_users = load_db(DB_USERS, {"admin_key": "ALEX_ADMIN", "keys": {}})
st.session_state.db_clients = db_disco_clients
st.session_state.db_users = db_disco_users

if user_id not in st.session_state.db_clients:
    st.session_state.db_clients[user_id] = {
        "ftp": {"host": "", "user": "", "pass": "", "port": "21"},
        "agendas": [],
        "logs": [],
        "comunicados": [],
    }
    save_db(DB_CLIENTS, st.session_state.db_clients)

client_data = st.session_state.db_clients[user_id]
user_info = st.session_state.db_users["keys"].get(
    user_id, {"server": "Servidor", "plano": "Starter", "expires": "01/01/2000"}
)

if st.session_state.role == "client":
    token_valido = user_info.get("last_session")
    if st.session_state.get("session_token") != token_valido:
        st.error("⚠️ Sessão Finalizada: Esta conta foi conectada em outro local.")
        if st.button(
            "Fazer Login Novamente", use_container_width=True, key="relogin_btn"
        ):
            st.session_state.authenticated = False
            st.rerun()
        st.stop()

plano_atual = user_info.get("plano", "Starter")
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
    st.write(f"Plano: **{plano_atual}**")
    st.markdown(f"Expira em: **{exp_status}**")

    progresso = min(total_agendas / limite_agendas, 1.0) if limite_agendas > 0 else 0
    st.progress(progresso, text=f"Uso: {total_agendas}/{limite_agendas}")

    st.divider()

    st.subheader("⚙️ Configurações FTP")
    client_data["ftp"]["host"] = st.text_input(
        "Host", value=client_data["ftp"]["host"], key="f_host_main"
    )
    client_data["ftp"]["user"] = st.text_input(
        "Usuário", value=client_data["ftp"]["user"], key="f_user_main"
    )
    client_data["ftp"]["pass"] = st.text_input(
        "Senha", type="password", value=client_data["ftp"]["pass"], key="f_pass_main"
    )
    client_data["ftp"]["port"] = st.text_input(
        "Porta", value=client_data["ftp"]["port"], key="f_port_main"
    )

    col_f1, col_f2 = st.columns(2)
    if col_f1.button("Salvar Dados", use_container_width=True, key="f_save_main"):
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
            st.success("Conexão OK!")
        except Exception as e:
            registrar_log(user_id, f"Teste FTP: Falha ({str(e)})", "erro")
            st.error("Erro FTP")

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
tab1, tab2, tab3, tab4, tab5, tab_cfggameplay, tab6, tab7, tab8 = st.tabs([
    "📅 Agendamentos",
    "📜 Logs",
    "📢 Comunicados",
    "⚙️ Economia (types.xml)",
    "🌍 Ambiente (globals.xml)",
    "🎮 Gameplay (cfggameplay)",
    "🛒 Loja / Trader",
    "👤 Jogadores / Vínculos",
    "🏦 Banco & Carteira",
])

with tab1:
    c1, c2 = st.columns([1, 1.5])

    with c1:
        st.subheader("🚀 Novo Evento")
        if total_agendas >= limite_agendas:
            st.error(f"Limite do plano atingido ({limite_agendas}).")
        else:
            uploader_key = f"uploader_{time.time()}"
            up_file = st.file_uploader(
                "Arquivo", type=["xml", "json"], key=uploader_key
            )

            mapa = st.selectbox(
                "Mapa", ["Chernarus", "Livonia"], key="map_sel_main"
            )
            dt_ev = st.date_input(
                "Data", min_value=get_hora_brasilia(), key="date_sel_main"
            )
            h_in = st.text_input("Entrada", "19:55", key="h_in_main")
            h_out = st.text_input("Saída", "21:55", key="h_out_main")
            rec = st.selectbox(
                "Recorrência", ["Único", "Diário", "Semanal"], key="rec_sel_main"
            )

            if st.button(
                "Confirmar Agendamento",
                use_container_width=True,
                key="conf_btn_main",
            ):
                if up_file:
                    safe_fn = f"{user_id[:5]}_{up_file.name}"
                    path = os.path.join(UPLOAD_DIR, safe_fn)

                    with open(path, "wb") as f:
                        f.write(up_file.getbuffer())

                    nova_agenda = {
                        "id": str(time.time()),
                        "file": up_file.name,
                        "local_path": path,
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
                        user_id, f"Agendado: {up_file.name} ({mapa})", "info"
                    )

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
                        st.write(
                            f"**🔄 Recorrência:** {agenda.get('rec', 'Único')}"
                        )
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
                            a
                            for a in client_data["agendas"]
                            if a["id"] != agenda["id"]
                        ]
                        save_db(DB_CLIENTS, st.session_state.db_clients)
                        registrar_log(
                            user_id, f"Removido: {nome_arquivo}", "info"
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
    st.subheader("⚙️ Editor de Loot (types.xml)")
    st.info("Faça upload do types.xml atual do seu servidor para analisar e ajustar o loot.")

    # 1) Upload do arquivo
    up_types = st.file_uploader("Enviar types.xml", type=["xml"], key="up_types_xml_client")

    if up_types is not None:
        try:
            xml_bytes = up_types.read()
            tree, root, df_types = parse_types_xml(xml_bytes)

            # Guarda no session_state, separado por cliente
            st.session_state[f"types_xml_tree_{user_id}"] = tree
            st.session_state[f"types_xml_root_{user_id}"] = root
            st.session_state[f"types_xml_df_{user_id}"] = df_types

            st.success(f"Arquivo carregado: {up_types.name} ({len(df_types)} itens)")
        except Exception as e:
            st.error(f"Erro ao ler types.xml: {e}")

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
    st.subheader("🌍 Configuração de Ambiente (globals.xml)")
    st.info("Ajuste limites de zumbis/animais, tempos de limpeza e timers de login/logout.")

    # 1) Upload do globals.xml
    up_globals = st.file_uploader("Enviar globals.xml", type=["xml"], key="up_globals_xml_client")

    if up_globals is not None:
        try:
            xml_bytes = up_globals.read()
            g_tree, g_root, g_vars = parse_globals_xml(xml_bytes)

            st.session_state[f"globals_tree_{user_id}"] = g_tree
            st.session_state[f"globals_root_{user_id}"] = g_root
            st.session_state[f"globals_vars_{user_id}"] = g_vars

            st.success(f"globals.xml carregado ({len(g_vars)} variáveis detectadas)")
        except Exception as e:
            st.error(f"Erro ao ler globals.xml: {e}")

    key_gvars = f"globals_vars_{user_id}"
    if key_gvars in st.session_state:
        g_vars = st.session_state[key_gvars]

        st.markdown("### 🧩 Parâmetros principais")

        # Pega valores atuais com fallback
        def get_val(name, default):
            info = g_vars.get(name, None)
            if info is None:
                return default
            return info.get("value", default)

        col1, col2 = st.columns(2)

        with col1:
            animal_max = st.slider(
                "AnimalMaxCount (máx. animais no mapa)",
                min_value=0,
                max_value=1000,
                value=int(get_val("AnimalMaxCount", 200)),
                step=10,
            )
            zombie_max = st.slider(
                "ZombieMaxCount (máx. zumbis no mapa)",
                min_value=0,
                max_value=5000,
                value=int(get_val("ZombieMaxCount", 1000)),
                step=50,
            )
            cleanup_dead = st.slider(
                "CleanupLifetimeDeadPlayer (limpeza corpo jogador, em seg.)",
                min_value=300,
                max_value=6 * 3600,
                value=int(get_val("CleanupLifetimeDeadPlayer", 3600)),
                step=300,
            )

        with col2:
            idle_mode = st.slider(
                "IdleModeCountdown (segundos até modo idle em servidor vazio)",
                min_value=0,
                max_value=24 * 3600,
                value=int(get_val("IdleModeCountdown", 60)),
                step=60,
            )
            time_login = st.slider(
                "TimeLogin (tempo de login, seg.)",
                min_value=5,
                max_value=120,
                value=int(get_val("TimeLogin", 15)),
                step=1,
            )
            time_logout = st.slider(
                "TimeLogout (tempo de logout, seg.)",
                min_value=5,
                max_value=120,
                value=int(get_val("TimeLogout", 15)),
                step=1,
            )

        st.markdown("### 📝 Resumo do ambiente")

        dia_aprox_horas = 24  # informativo
        idle_min = idle_mode // 60
        cleanup_min = cleanup_dead // 60

        st.write(
            f"- Máx. **{zombie_max}** zumbis e **{animal_max}** animais configurados no mapa."
        )  # [web:91]
        st.write(
            f"- Corpos de jogadores ficam por ~**{cleanup_min} minutos** antes de serem limpos."
        )  # [web:91]
        st.write(
            f"- Servidor entra em modo idle após **{idle_min} minutos** sem jogadores (IdleModeCountdown)."
        )  # [web:90][web:103]
        st.write(
            f"- Tempo de login: **{time_login} s**, tempo de logout: **{time_logout} s**."
        )  # [web:97]

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
                    
with tab_cfggameplay:
    st.subheader("🎮 Configuração de Gameplay (cfggameplay.json)")
    st.info(
        "Faça upload do cfggameplay.json do seu servidor, ajuste os parâmetros de gameplay "
        "e depois baixe o arquivo ou envie via FTP."
    )

    # 1) Upload do cfggameplay.json (igual types/globals: baseado em upload + sessão)
    up_cfg = st.file_uploader(
        "Enviar cfggameplay.json",
        type=["json"],
        key=f"up_cfggameplay_{user_id}",
    )

    cfg_session_key = f"cfggameplay_cfg_{user_id}"

    if up_cfg is not None:
        try:
            raw_bytes = up_cfg.read()
            cfg = json.loads(raw_bytes.decode("utf-8"))
            st.session_state[cfg_session_key] = cfg
            st.success(f"Arquivo carregado: {up_cfg.name}")
        except Exception as e:
            st.error(f"Erro ao ler cfggameplay.json: {e}")

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
                max_value=500.0,
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
            stamina_penalty = st.number_input(
                "Penalidade kg → % (staminaKgToStaminaPercentPenalty)",
                min_value=0.0,
                max_value=10.0,
                step=0.05,
                value=float(stamina.get("staminaKgToStaminaPercentPenalty", 1.75)),
            )
            sprint_sta_mod_erc = st.number_input(
                "Sprint em pé (sprintStaminaModifierErc)",
                min_value=0.1,
                max_value=5.0,
                step=0.1,
                value=float(stamina.get("sprintStaminaModifierErc", 1.0)),
            )
            sprint_sta_mod_cro = st.number_input(
                "Sprint abaixado (sprintStaminaModifierCro)",
                min_value=0.1,
                max_value=5.0,
                step=0.1,
                value=float(stamina.get("sprintStaminaModifierCro", 1.0)),
            )

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
            time_to_sprint = st.number_input(
                "Tempo para sprint (timeToSprint)",
                min_value=0.0,
                max_value=5.0,
                step=0.05,
                value=float(movement.get("timeToSprint", 0.45)),
            )
            rot_speed_jog = st.number_input(
                "Rotação correndo (rotationSpeedJog)",
                min_value=0.0,
                max_value=2.0,
                step=0.05,
                value=float(movement.get("rotationSpeedJog", 0.3)),
            )
        with col_m2:
            rot_speed_sprint = st.number_input(
                "Rotação sprint (rotationSpeedSprint)",
                min_value=0.0,
                max_value=2.0,
                step=0.05,
                value=float(movement.get("rotationSpeedSprint", 0.15)),
            )
            allow_sta_inertia = st.checkbox(
                "Stamina afeta inércia (allowStaminaAffectInertia)",
                value=movement.get("allowStaminaAffectInertia", True),
            )

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
            use_3d_map = st.checkbox(
                "Usar mapa 3D (use3DMap)",
                value=ui_data.get("use3DMap", False),
            )

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
                stamina.update(
                    {
                        "staminaMax": stamina_max,
                        "staminaMinCap": stamina_min_cap,
                        "staminaWeightLimitThreshold": stamina_weight_threshold,
                        "staminaKgToStaminaPercentPenalty": stamina_penalty,
                        "sprintStaminaModifierErc": sprint_sta_mod_erc,
                        "sprintStaminaModifierCro": sprint_sta_mod_cro,
                    }
                )
                player["StaminaData"] = stamina

                # Shock
                shock.update(
                    {
                        "shockRefillSpeedConscious": shock_refill_con,
                        "shockRefillSpeedUnconscious": shock_refill_uncon,
                        "allowRefillSpeedModifier": allow_refill_mod,
                    }
                )
                player["ShockHandlingData"] = shock

                # Movement
                movement.update(
                    {
                        "timeToSprint": time_to_sprint,
                        "rotationSpeedJog": rot_speed_jog,
                        "rotationSpeedSprint": rot_speed_sprint,
                        "allowStaminaAffectInertia": allow_sta_inertia,
                    }
                )
                player["MovementData"] = movement

                cfg["PlayerData"] = player

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

with tab6:
    st.subheader("🛒 Loja / Trader")
    st.info("Configure aqui o catálogo de itens da loja do seu servidor.")

    # Garante estrutura de loja no client_data
    loja = load_loja_for_client(client_data)

    st.markdown("### ⚙️ Configurações gerais da Loja")

    col_conf1, col_conf2 = st.columns(2)
    with col_conf1:
        loja_mapa_padrao = st.selectbox(
            "Mapa padrão da Loja",
            ["Chernarus", "Livonia"],
            index=["Chernarus", "Livonia"].index(loja.get("mapa_padrao", "Chernarus")),
            key="loja_mapa_padrao",
        )
    with col_conf2:
        loja_posicao_padrao = st.text_input(
            "Coordenadas padrão de entrega (opcional)",
            value=loja.get("posicao_padrao", ""),
            help=(
                "Opcional. Use um mapa como dayz.xam.nu ou iZurvive, clique no local desejado, "
                "copie as coordenadas (ex: 2432.34/4353.87) ou a descrição e cole aqui. "
                "O player poderá informar outra posição na página de compra."
            ),
            key="loja_posicao_padrao",
        )

    st.markdown("### 📦 Itens da Loja")

    # Converte itens para DataFrame editável
    df_loja_key = f"df_loja_{user_id}"
    if df_loja_key not in st.session_state:
        st.session_state[df_loja_key] = loja_itens_to_df(loja)

    df_loja = st.session_state[df_loja_key]

    st.info(
        "Colunas: id (ordem de exibição), nome (visível para o player), "
        "classe (nome do item no DayZ, ex: M4A1), categoria, preço (DzCoins), quantidade por compra, ativo."
    )

    edited_df_loja = st.data_editor(
        df_loja,
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "id": st.column_config.NumberColumn(
                "ID (ordem)",
                help="Ordem do item na lista / identificador para compras.",
                min_value=1,
                step=1,
            ),
            "nome": "Nome (exibido na loja)",
            "classe": "Classe DayZ (ex: M4A1)",
            "categoria": "Categoria (Armas, Kits, etc.)",
            "preco": st.column_config.NumberColumn(
                "Preço (DzCoins)",
                min_value=0,
                step=1,
            ),
            "quantidade": st.column_config.NumberColumn(
                "Quantidade",
                min_value=1,
                step=1,
            ),
            "ativo": st.column_config.CheckboxColumn(
                "Ativo",
                default=True,
                help="Se desmarcado, o item não aparece para os jogadores.",
            ),
        },
    )  # [web:67]

    st.markdown("### 💾 Salvar catálogo")

    col_loja1, col_loja2, col_loja3 = st.columns(3)

    with col_loja1:
        if st.button("Aplicar alterações na sessão (Loja)", use_container_width=True):
            # Atualiza o DataFrame na sessão
            st.session_state[df_loja_key] = edited_df_loja
            st.success("Alterações aplicadas na sessão da Loja.")

    with col_loja2:
        if st.button("Salvar Loja no Titan Cloud", use_container_width=True):
            # Converte DF para lista de itens e salva em client_data + disco
            itens_atualizados = df_to_loja_itens(edited_df_loja)
            loja["mapa_padrao"] = loja_mapa_padrao
            loja["posicao_padrao"] = loja_posicao_padrao
            loja["itens"] = itens_atualizados

            client_data["loja"] = loja
            st.session_state.db_clients[user_id] = client_data
            save_db(DB_CLIENTS, st.session_state.db_clients)

            st.success("Catálogo da Loja salvo com sucesso no Titan Cloud!")

    with col_loja3:
        if st.button("⬇️ Baixar Loja (JSON)", use_container_width=True):
            itens_atualizados = df_to_loja_itens(edited_df_loja)
            loja_preview = {
                "servidor": user_info.get("server", "Servidor"),
                "mapa_padrao": loja_mapa_padrao,
                "posicao_padrao": loja_posicao_padrao,
                "itens": itens_atualizados,
            }
            loja_json = json.dumps(loja_preview, indent=4, ensure_ascii=False)

            st.download_button(
                label="Baixar arquivo Loja_Titan.json",
                data=loja_json.encode("utf-8"),
                file_name="Loja_Titan.json",
                mime="application/json",
                use_container_width=True,
            )

with tab7:
    st.subheader("👤 Jogadores / Vínculos")
    st.info(
        "Gerencie aqui o vínculo entre Gamertag dos jogadores e suas informações básicas. "
        "Esses dados serão usados pela Loja, Banco DzCoins e estatísticas."
    )

    # Garante que o cliente está autenticado e possui um server_id vinculado
    server_id = st.session_state.get("server_id")
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

    # (Opcional) debug para ver o dict bruto de players
    # st.write("DEBUG players raw:", players)

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

            # Atualiza o client_data e o dicionário carregado do arquivo
            client_data["players"] = players_atualizados
            db_clients[server_id] = client_data

            # Salva em disco
            save_db(DB_CLIENTS, db_clients)

            # Mantém o DF da sessão sincronizado com o que foi salvo
            st.session_state[df_players_key] = edited_df_players

            st.success("Vínculos de jogadores salvos com sucesso no Titan Cloud!")

with tab8:
    st.subheader("🏦 Banco & Carteira")

    st.info(
        "Gerencie aqui o saldo de DzCoins dos jogadores: carteira (com o jogador) e banco (guardado). "
        "Use esta tela para bônus de evento, correções e ajustes manuais."
    )

    # 1) Garante que há um servidor válido na sessão
    server_id = st.session_state.get("server_id")
    if not server_id:
        st.error(
            "Nenhum servidor vinculado a este login.\n"
            "Faça login com uma KeyUser válida (gerada no painel de administração)."
        )
        st.stop()

    # 2) Carrega dados do servidor
    clients_data = load_db(DB_CLIENTS, {})
    if not clients_data:
        st.warning("Nenhum cliente/servidor cadastrado em clients_data.json.")
        st.stop()

    if server_id not in clients_data:
        st.error(
            f"O servidor com ID {server_id} não foi encontrado em clients_data.json.\n"
            "Verifique se o server_id está correto."
        )
        st.stop()

    client_data = clients_data[server_id]

    # Garante estruturas básicas
    players = client_data.get("players", {})
    if "wallets" not in client_data:
        client_data["wallets"] = {}
    if "bank" not in client_data:
        client_data["bank"] = {}

    wallets = client_data["wallets"]
    bank = client_data["bank"]

    # 3) Selecionar jogador
    st.markdown("### 👤 Selecionar jogador")

    if not players:
        st.warning("Nenhum jogador vinculado ainda. Use a aba 'Jogadores / Vínculos'.")
        st.stop()

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

                # Atualiza variáveis locais para refletir o novo saldo
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

    historico_comb = []

    for linha in wallet_reg.get("historico", []):
        historico_comb.append(f"[CARTEIRA] {linha}")
    for linha in bank_reg.get("historico", []):
        historico_comb.append(f"[BANCO] {linha}")

    if historico_comb:
        for linha in reversed(historico_comb[-50:]):
            st.write(linha)
    else:
        st.info("Ainda não há movimentações registradas para este jogador.")

# --- INÍCIO DO WORKER DE AUTOMAÇÃO ---
if "worker_started" not in st.session_state:
    threading.Thread(target=pro_worker, daemon=True).start()
    st.session_state["worker_started"] = True

nav = navigation(
    {
        "Portais": [
            Page("pages/evento.py", "Portal do Administrador", icon="🛡️"),
            Page("pages/player_portal.py", "Portal do Player", icon="🎮"),
        ]
    }
)

nav.run()
