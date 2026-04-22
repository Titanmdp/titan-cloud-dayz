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

# Discord OAuth2 config
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
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_db(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


# =========================================================
# 3. FUNÇÕES DE DOMÍNIO
# =========================================================

def load_players_for_client(client_data_obj):
    if "players" not in client_data_obj:
        client_data_obj["players"] = {}
    return client_data_obj["players"]


# =========================================================
# 4. UI PRINCIPAL DO PORTAL DO JOGADOR
# =========================================================

def main():
    # --- CONFIG PÁGINA ---
    st.set_page_config(
        page_title="Titan Cloud Pro - Portal do Jogador",
        page_icon="🎮",
        layout="centered",
    )

    st.title("🎮 Titan Cloud Pro - Portal do Jogador")
    st.write(
        "Vincule sua Gamertag ao servidor para liberar acesso à loja, banco e economia."
    )

    # --- PROCESSA RETORNO DO DISCORD (code) ---
    query_params = st.query_params  # novo API do Streamlit
    code_list = query_params.get("code", [])

    if code_list and DISCORD_CLIENT_ID and DISCORD_CLIENT_SECRET and DISCORD_REDIRECT_URI:
        code = code_list[0]

        data = {
            "client_id": DISCORD_CLIENT_ID,
            "client_secret": DISCORD_CLIENT_SECRET,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": DISCORD_REDIRECT_URI,
            "scope": DISCORD_SCOPE,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        token_resp = requests.post(DISCORD_TOKEN_URL, data=data, headers=headers)
        if token_resp.status_code == 200:
            token_data = token_resp.json()
            access_token = token_data["access_token"]

            # /users/@me
            user_resp = requests.get(
                f"{DISCORD_API_BASE}/users/@me",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            # /users/@me/guilds
            guilds_resp = requests.get(
                f"{DISCORD_API_BASE}/users/@me/guilds",
                headers={"Authorization": f"Bearer {access_token}"},
            )

            if user_resp.status_code == 200:
                user_info = user_resp.json()
                st.session_state.portal_discord_id = user_info.get("id")
                st.session_state.portal_discord_name = user_info.get("username")

            if guilds_resp.status_code == 200:
                st.session_state.portal_discord_guilds = guilds_resp.json()

            st.success(
                f"Conectado com Discord como "
                f"{st.session_state.get('portal_discord_name', 'Usuário')}"
            )
        else:
            st.error("Falha ao autenticar com o Discord. Tente novamente.")

    # --- BOTÃO / LINK DE LOGIN COM DISCORD ---
    if not st.session_state.get("portal_discord_id"):
        st.markdown("### 🔑 Conecte-se com o Discord")

        if not DISCORD_CLIENT_ID or not DISCORD_REDIRECT_URI:
            st.warning(
                "Login com Discord ainda não está configurado corretamente. "
                "Contate o administrador."
            )
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
                f"[👉 Entrar com Discord]({auth_url})",
                unsafe_allow_html=True,
            )
    else:
        st.info(
            f"Você está conectado como **{st.session_state.get('portal_discord_name', 'Usuário')}** "
            f"(ID: `{st.session_state.get('portal_discord_id')}`)."
        )

    # --- CARREGA BANCOS ---
    users_db = load_db(DB_USERS, {"keys": {}})
    clients_db = load_db(DB_CLIENTS, {})

    if not users_db.get("keys"):
        st.error("Nenhum servidor disponível no momento.")
        return

    # -----------------------------------------------------
    # 4.1 SELEÇÃO DO SERVIDOR
    # -----------------------------------------------------
    nome_para_server_id = {}
    for keyuser, data in users_db.get("keys", {}).items():
        server_name = str(data.get("server", "")).strip()
        server_id = str(data.get("server_id", "")).strip() or keyuser
        if server_name and server_id:
            nome_para_server_id[server_name.lower()] = server_id

    if not nome_para_server_id:
        st.error("Não há servidores com nome configurado. Contate o administrador.")
        return

    st.markdown("### 🏷️ Escolha o servidor")

    st.write(
        "Digite o nome do servidor exatamente como informado pelo administrador "
        "ou como aparece nas mensagens oficiais do servidor."
    )

    nome_servidor_input = st.text_input("Nome do servidor", "")

    if "portal_server_id" not in st.session_state:
        st.session_state.portal_server_id = None
    if "portal_server_nome" not in st.session_state:
        st.session_state.portal_server_nome = None

    if st.button("Confirmar servidor"):
        nome_limpo = nome_servidor_input.strip().lower()
        if not nome_limpo:
            st.error("Por favor, preencha o nome do servidor.")
        else:
            server_id = nome_para_server_id.get(nome_limpo)
            if not server_id:
                st.error(
                    "Servidor não encontrado. Verifique o nome informado com o administrador."
                )
            else:
                if server_id not in clients_db:
                    st.error(
                        "Servidor ainda não está configurado no sistema. "
                        "Avise o administrador."
                    )
                else:
                    st.session_state.portal_server_id = server_id
                    st.session_state.portal_server_nome = [
                        nome for nome, sid in nome_para_server_id.items() if sid == server_id
                    ][0].title()
                    st.success(
                        f"Servidor encontrado: {st.session_state.portal_server_nome}"
                    )

    server_id = st.session_state.get("portal_server_id")
    if server_id and server_id in clients_db:
        client_data = clients_db[server_id]
    else:
        client_data = None

    # -----------------------------------------------------
    # 4.2 VÍNCULO DE GAMERTAG AO SERVIDOR
    # -----------------------------------------------------
    if server_id and client_data:
        players = load_players_for_client(client_data)

        st.markdown("### 🔗 Vincular Gamertag")
        st.info(
            f"Servidor selecionado: **{st.session_state.portal_server_nome or server_id}**"
        )

        with st.form("form_vinculo"):
            gamertag = st.text_input(
                "Gamertag (exatamente como aparece no console)", ""
            )
            apelido = st.text_input("Apelido / Nome no Discord (opcional)", "")
            observacoes = st.text_area("Observações (opcional)", "")

            submitted = st.form_submit_button("Vincular")

        if submitted:
            gamertag_clean = gamertag.strip()
            if not gamertag_clean:
                st.error("Por favor, preencha a Gamertag.")
                return

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
                f"Gamertag **{gamertag_clean}** vinculada com sucesso ao servidor "
                f"**{st.session_state.portal_server_nome or server_id}**!"
            )
            st.info(
                "No futuro, você poderá usar a Loja e o Banco deste servidor quando essas funções estiverem ativas."
            )

        # -------------------------------------------------
        # 4.3 MEU BANCO (JOGADOR)
        # -------------------------------------------------
        st.markdown("---")
        st.markdown("## 💼 Meu Banco")

        # Decide qual Gamertag usar
        if "portal_gamertag" in st.session_state:
            gamertag_banco = st.session_state.portal_gamertag
            st.info(
                f"Usando Gamertag vinculada na sessão: **{gamertag_banco}**.\n\n"
                "Se quiser usar outra Gamertag vinculada a este servidor, informe abaixo."
            )
        else:
            gamertag_banco = ""

        gamertag_banco_input = st.text_input(
            "Gamertag vinculada para acessar o banco (deixe em branco para usar a da sessão)",
            value=gamertag_banco,
        )

        gamertag_banco_clean = gamertag_banco_input.strip()
        if not gamertag_banco_clean:
            st.warning("Informe uma Gamertag vinculada para ver o banco.")
        else:
            if gamertag_banco_clean not in players:
                st.error(
                    "Esta Gamertag não está vinculada a este servidor.\n"
                    "Use o formulário acima para fazer o vínculo primeiro."
                )
            else:
                if "wallets" not in client_data:
                    client_data["wallets"] = {}
                if "bank" not in client_data:
                    client_data["bank"] = {}

                wallets = client_data["wallets"]
                bank = client_data["bank"]

                wallet_reg = wallets.get(
                    gamertag_banco_clean, {"balance": 0, "historico": []}
                )
                bank_reg = bank.get(
                    gamertag_banco_clean, {"balance": 0, "historico": []}
                )

                saldo_carteira = wallet_reg.get("balance", 0)
                saldo_banco = bank_reg.get("balance", 0)

                st.info(
                    f"Jogador **{gamertag_banco_clean}**\n\n"
                    f"- Carteira: **{saldo_carteira} DzCoins**\n"
                    f"- Banco: **{saldo_banco} DzCoins**"
                )

                st.markdown("### 🔁 Transferência entre carteira e banco")

                col_dep, col_saq = st.columns(2)

                # Carteira -> Banco
                with col_dep:
                    st.markdown("#### ➡️ Carteira → Banco")
                    valor_dep = st.number_input(
                        "Valor para enviar ao banco",
                        min_value=0,
                        step=100,
                        key="valor_dep_carteira_banco",
                    )
                    if st.button("Enviar para o banco", use_container_width=True):
                        if valor_dep <= 0:
                            st.error("Informe um valor maior que zero.")
                        elif valor_dep > saldo_carteira:
                            st.error("Você não tem esse valor na carteira.")
                        else:
                            saldo_carteira_novo = saldo_carteira - valor_dep
                            saldo_banco_novo = saldo_banco + valor_dep

                            wallet_reg["balance"] = saldo_carteira_novo
                            bank_reg["balance"] = saldo_banco_novo

                            hora = datetime.now().strftime("%d/%m/%Y %H:%M")
                            wallet_reg.setdefault("historico", []).append(
                                f"[{hora}] TRANSF. → BANCO -{valor_dep}"
                            )
                            bank_reg.setdefault("historico", []).append(
                                f"[{hora}] TRANSF. ← CARTEIRA +{valor_dep}"
                            )

                            wallets[gamertag_banco_clean] = wallet_reg
                            bank[gamertag_banco_clean] = bank_reg
                            client_data["wallets"] = wallets
                            client_data["bank"] = bank
                            clients_db[server_id] = client_data
                            save_db(DB_CLIENTS, clients_db)

                            st.success(
                                f"Transferido {valor_dep} DzCoins da carteira para o banco com sucesso."
                            )

                # Banco -> Carteira
                with col_saq:
                    st.markdown("#### ⬅️ Banco → Carteira")
                    valor_saq = st.number_input(
                        "Valor para trazer do banco",
                        min_value=0,
                        step=100,
                        key="valor_saq_banco_carteira",
                    )
                    if st.button("Trazer do banco", use_container_width=True):
                        if valor_saq <= 0:
                            st.error("Informe um valor maior que zero.")
                        elif valor_saq > saldo_banco:
                            st.error("Você não tem esse valor no banco.")
                        else:
                            saldo_carteira_novo = saldo_carteira + valor_saq
                            saldo_banco_novo = saldo_banco - valor_saq

                            wallet_reg["balance"] = saldo_carteira_novo
                            bank_reg["balance"] = saldo_banco_novo

                            hora = datetime.now().strftime("%d/%m/%Y %H:%M")
                            bank_reg.setdefault("historico", []).append(
                                f"[{hora}] TRANSF. → CARTEIRA -{valor_saq}"
                            )
                            wallet_reg.setdefault("historico", []).append(
                                f"[{hora}] TRANSF. ← BANCO +{valor_saq}"
                            )

                            wallets[gamertag_banco_clean] = wallet_reg
                            bank[gamertag_banco_clean] = bank_reg
                            client_data["wallets"] = wallets
                            client_data["bank"] = bank
                            clients_db[server_id] = client_data
                            save_db(DB_CLIENTS, clients_db)

                            st.success(
                                f"Transferido {valor_saq} DzCoins do banco para a carteira com sucesso."
                            )

                st.markdown("### 📜 Histórico de movimentações")

                historico_comb = []
                for linha in wallet_reg.get("historico", []):
                    historico_comb.append(f"[CARTEIRA] {linha}")
                for linha in bank_reg.get("historico", []):
                    historico_comb.append(f"[BANCO] {linha}")

                if historico_comb:
                    for linha in reversed(historico_comb[-30:]):
                        st.write(linha)
                else:
                    st.info("Ainda não há movimentações registradas para este jogador.")
    else:
        st.info(
            "Nenhum servidor selecionado ainda. Informe o nome do servidor e clique em "
            "'Confirmar servidor'."
        )


if __name__ == "__main__":
    main()
