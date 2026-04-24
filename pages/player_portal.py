import streamlit as st
import json
import os
from datetime import datetime
import requests
import urllib.parse

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


def trocar_code_por_token(code: str) -> dict | None:
    """
    Troca o code retornado pelo Discord pelo access_token.
    Retorna o dict com user_info e guilds, ou None em caso de erro.
    """
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
        st.error(f"Erro de conexão com o Discord: {e}")
        return None

    if token_resp.status_code != 200:
        st.error(f"Erro ao autenticar com Discord (status {token_resp.status_code}): {token_resp.text}")
        return None

    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        st.error("Discord não retornou access_token.")
        return None

    auth_header = {"Authorization": f"Bearer {access_token}"}

    try:
        user_resp = requests.get(f"{DISCORD_API_BASE}/users/@me", headers=auth_header, timeout=10)
        guilds_resp = requests.get(f"{DISCORD_API_BASE}/users/@me/guilds", headers=auth_header, timeout=10)
    except Exception as e:
        st.error(f"Erro ao buscar dados do Discord: {e}")
        return None

    if user_resp.status_code != 200:
        st.error("Não foi possível obter informações do usuário Discord.")
        return None

    user_info = user_resp.json()
    guilds = guilds_resp.json() if guilds_resp.status_code == 200 else []

    return {
        "discord_id": user_info.get("id"),
        "discord_name": user_info.get("username"),
        "guilds": guilds,
    }

# =========================================================
# 4. UI PRINCIPAL
# =========================================================

def main():
    st.set_page_config(
        page_title="Titan Cloud Pro - Portal do Jogador",
        page_icon="🎮",
        layout="centered",
    )

    st.title("🎮 Titan Cloud Pro - Portal do Jogador")
    st.markdown("Vincule sua Gamertag ao servidor para liberar acesso à loja, banco e economia.")

    # ---------------------------------------------------------
    # 4.1 PROCESSAR RETORNO DO DISCORD (code na URL)
    # Isso precisa acontecer ANTES de qualquer st.stop()
    # ---------------------------------------------------------
    query_params = st.query_params

    # No Streamlit atual, query_params.get() retorna string direta
    code = query_params.get("code")

    if code and not st.session_state.get("portal_discord_id"):
        with st.spinner("Autenticando com o Discord..."):
            resultado = trocar_code_por_token(code)

        if resultado:
            st.session_state.portal_discord_id = resultado["discord_id"]
            st.session_state.portal_discord_name = resultado["discord_name"]
            st.session_state.portal_discord_guilds = resultado["guilds"]

            # Limpa o code da URL para não reprocessar no próximo rerun
            st.query_params.clear()
            st.rerun()
        else:
            # Limpa URL mesmo em caso de erro para não ficar em loop
            st.query_params.clear()

    # ---------------------------------------------------------
    # 4.2 SEÇÃO DE LOGIN COM DISCORD
    # ---------------------------------------------------------
    st.markdown("### 🔑 Conecte-se com o Discord")

    if not st.session_state.get("portal_discord_id"):
        if not DISCORD_CLIENT_ID or not DISCORD_REDIRECT_URI:
            st.warning("Login com Discord ainda não está configurado. Contate o administrador.")
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
                <a href="{auth_url}" target="_self" style="
                    display: inline-block;
                    background-color: #5865F2;
                    color: white;
                    padding: 10px 20px;
                    border-radius: 8px;
                    text-decoration: none;
                    font-weight: bold;
                    font-size: 16px;
                ">
                👾 Entrar com Discord
                </a>
                """,
                unsafe_allow_html=True,
            )
        st.stop()

    else:
        col_disc, col_logout = st.columns([3, 1])
        with col_disc:
            st.success(
                f"✅ Conectado como **{st.session_state.get('portal_discord_name')}** "
                f"(ID: `{st.session_state.get('portal_discord_id')}`)"
            )
        with col_logout:
            if st.button("🚪 Sair", use_container_width=True):
                for k in ["portal_discord_id", "portal_discord_name", "portal_discord_guilds",
                          "portal_server_id", "portal_server_nome", "portal_gamertag"]:
                    st.session_state.pop(k, None)
                st.rerun()

    # ---------------------------------------------------------
    # 4.3 CARREGA BANCOS DE DADOS
    # ---------------------------------------------------------
    users_db = load_db(DB_USERS, {"keys": {}})
    clients_db = load_db(DB_CLIENTS, {})

    if not users_db.get("keys"):
        st.error("Nenhum servidor disponível no momento.")
        return

    # ---------------------------------------------------------
    # 4.4 SELEÇÃO DO SERVIDOR
    # ---------------------------------------------------------
    st.markdown("### 🏷️ Escolha o servidor")

    nome_para_server_id = {}
    for keyuser, data in users_db.get("keys", {}).items():
        server_name = str(data.get("server", "")).strip()
        server_id = str(data.get("server_id", "")).strip() or keyuser
        if server_name and server_id:
            nome_para_server_id[server_name.lower()] = server_id

    if not nome_para_server_id:
        st.error("Não há servidores configurados. Contate o administrador.")
        return

    nome_servidor_input = st.text_input("Nome do servidor (exatamente como informado pelo admin)", "")

    if st.button("Confirmar servidor"):
        nome_limpo = nome_servidor_input.strip().lower()
        if not nome_limpo:
            st.error("Preencha o nome do servidor.")
        else:
            sid = nome_para_server_id.get(nome_limpo)
            if not sid:
                st.error("Servidor não encontrado. Verifique o nome com o administrador.")
            elif sid not in clients_db:
                st.error("Servidor ainda não configurado no sistema. Avise o administrador.")
            else:
                st.session_state.portal_server_id = sid
                st.session_state.portal_server_nome = nome_limpo.title()
                st.success(f"Servidor encontrado: **{nome_limpo.title()}**")

    server_id = st.session_state.get("portal_server_id")
    if not server_id:
        st.stop()

    client_data = clients_db.get(server_id, {})
    players = load_players_for_client(client_data)

    # ---------------------------------------------------------
    # 4.5 VALIDAÇÃO DE MEMBERSHIP NO DISCORD DO SERVIDOR
    # ---------------------------------------------------------

    # Busca o guild_id configurado pelo admin para este servidor
    discord_guild_id = None
    for key_data in users_db.get("keys", {}).values():
        if str(key_data.get("server_id", "")) == str(server_id):
            discord_guild_id = key_data.get("discord_guild_id", "")
            break

    portal_guilds = st.session_state.get("portal_discord_guilds", [])

    if discord_guild_id:
        membro_validado = validar_membro_discord(portal_guilds, discord_guild_id)
        if not membro_validado:
            st.error(
                "❌ Seu Discord não está no servidor oficial deste administrador.\n\n"
                "Entre no servidor Discord e tente novamente."
            )
            st.stop()
        else:
            st.success("✅ Discord validado — você é membro do servidor oficial!")
    else:
        st.info("ℹ️ Este servidor não exige validação de Discord, mas recomendamos conectar.")

    # ---------------------------------------------------------
    # 4.6 FORMULÁRIO DE VÍNCULO DE GAMERTAG
    # ---------------------------------------------------------
    st.markdown("### 🎮 Vincular sua Gamertag")
    st.markdown("Preencha sua Gamertag exatamente como aparece no console.")

    with st.form("form_vinculo"):
        gamertag = st.text_input("Gamertag (exatamente como aparece no console)", "")
        apelido = st.text_input("Apelido / Nome no Discord (opcional)", "")
        observacoes = st.text_area("Observações (opcional)", "")
        submitted = st.form_submit_button("Vincular")

    if submitted:
        gamertag_clean = gamertag.strip()
        if not gamertag_clean:
            st.error("Preencha a Gamertag.")
        else:
            players[gamertag_clean] = {
                "gamertag": gamertag_clean,
                "apelido": apelido.strip(),
                "discord_id": st.session_state.get("portal_discord_id", ""),
                "observacoes": observacoes.strip(),
            }
            client_data["players"] = players
            clients_db[server_id] = client_data
            save_db(DB_CLIENTS, clients_db)
            st.session_state.portal_gamertag = gamertag_clean
            st.success(
                f"✅ Gamertag **{gamertag_clean}** vinculada com sucesso ao servidor "
                f"**{st.session_state.get('portal_server_nome', server_id)}**!"
            )

    # ---------------------------------------------------------
    # 4.7 MEU BANCO
    # ---------------------------------------------------------
    st.markdown("---")
    st.markdown("## 💼 Meu Banco")

    gamertag_banco = st.session_state.get("portal_gamertag", "")
    gamertag_banco_input = st.text_input(
        "Gamertag para acessar o banco",
        value=gamertag_banco,
        help="Deixe em branco para usar a Gamertag vinculada nesta sessão."
    )
    gamertag_banco_clean = gamertag_banco_input.strip()

    if not gamertag_banco_clean:
        st.info("Informe uma Gamertag vinculada para ver o banco.")
        return

    if gamertag_banco_clean not in players:
        st.warning("Esta Gamertag não está vinculada a este servidor. Faça o vínculo acima primeiro.")
        return

    if "wallets" not in client_data:
        client_data["wallets"] = {}
    if "bank" not in client_data:
        client_data["bank"] = {}

    wallets = client_data["wallets"]
    bank = client_data["bank"]

    wallet_reg = wallets.get(gamertag_banco_clean, {"balance": 0, "historico": []})
    bank_reg = bank.get(gamertag_banco_clean, {"balance": 0, "historico": []})

    saldo_carteira = wallet_reg.get("balance", 0)
    saldo_banco = bank_reg.get("balance", 0)

    col_w, col_b = st.columns(2)
    col_w.metric("💰 Carteira", f"{saldo_carteira} DzCoins")
    col_b.metric("🏦 Banco", f"{saldo_banco} DzCoins")

    st.markdown("### 🔁 Transferência")
    col_dep, col_saq = st.columns(2)

    with col_dep:
        st.markdown("#### ➡️ Carteira → Banco")
        valor_dep = st.number_input("Valor", min_value=0, step=100, key="val_dep")
        if st.button("Enviar para o banco", use_container_width=True):
            if valor_dep <= 0:
                st.error("Informe um valor maior que zero.")
            elif valor_dep > saldo_carteira:
                st.error("Saldo insuficiente na carteira.")
            else:
                wallet_reg["balance"] = saldo_carteira - valor_dep
                bank_reg["balance"] = saldo_banco + valor_dep
                hora = datetime.now().strftime("%d/%m/%Y %H:%M")
                wallet_reg.setdefault("historico", []).append(f"[{hora}] TRANSF. → BANCO -{valor_dep}")
                bank_reg.setdefault("historico", []).append(f"[{hora}] TRANSF. ← CARTEIRA +{valor_dep}")
                wallets[gamertag_banco_clean] = wallet_reg
                bank[gamertag_banco_clean] = bank_reg
                client_data["wallets"] = wallets
                client_data["bank"] = bank
                clients_db[server_id] = client_data
                save_db(DB_CLIENTS, clients_db)
                st.success(f"Transferido {valor_dep} DzCoins para o banco!")
                st.rerun()

    with col_saq:
        st.markdown("#### ⬅️ Banco → Carteira")
        valor_saq = st.number_input("Valor", min_value=0, step=100, key="val_saq")
        if st.button("Trazer do banco", use_container_width=True):
            if valor_saq <= 0:
                st.error("Informe um valor maior que zero.")
            elif valor_saq > saldo_banco:
                st.error("Saldo insuficiente no banco.")
            else:
                wallet_reg["balance"] = saldo_carteira + valor_saq
                bank_reg["balance"] = saldo_banco - valor_saq
                hora = datetime.now().strftime("%d/%m/%Y %H:%M")
                bank_reg.setdefault("historico", []).append(f"[{hora}] TRANSF. → CARTEIRA -{valor_saq}")
                wallet_reg.setdefault("historico", []).append(f"[{hora}] TRANSF. ← BANCO +{valor_saq}")
                wallets[gamertag_banco_clean] = wallet_reg
                bank[gamertag_banco_clean] = bank_reg
                client_data["wallets"] = wallets
                client_data["bank"] = bank
                clients_db[server_id] = client_data
                save_db(DB_CLIENTS, clients_db)
                st.success(f"Transferido {valor_saq} DzCoins para a carteira!")
                st.rerun()

    st.markdown("### 📜 Histórico")
    historico_comb = []
    for linha in wallet_reg.get("historico", []):
        historico_comb.append(f"[CARTEIRA] {linha}")
    for linha in bank_reg.get("historico", []):
        historico_comb.append(f"[BANCO] {linha}")

    if historico_comb:
        for linha in reversed(historico_comb[-30:]):
            st.write(linha)
    else:
        st.info("Ainda não há movimentações registradas.")


if __name__ == "__main__":
    main()
