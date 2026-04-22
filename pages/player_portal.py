import streamlit as st
import json
import os
from datetime import datetime

# =========================================================
# 1. CONFIG / AMBIENTE / CONSTANTES
# =========================================================

# --- DETECÇÃO DE AMBIENTE E PERSISTÊNCIA DE DADOS ---
IS_DEV = os.environ.get("IS_DEV", "False") == "True"

# Se existir disk montado em /var/data (Render), usa sempre ele
if os.path.exists("/var/data"):
    DB_USERS = "/var/data/users_db.json"
    DB_CLIENTS = "/var/data/clients_data.json"
# Senão, usa arquivos locais na pasta do projeto (para rodar no PC)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    if IS_DEV:
        DB_USERS = os.path.join(BASE_DIR, "users_db_dev.json")
        DB_CLIENTS = os.path.join(BASE_DIR, "clients_data_dev.json")
    else:
        DB_USERS = os.path.join(BASE_DIR, "users_db.json")
        DB_CLIENTS = os.path.join(BASE_DIR, "clients_data.json")


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
# 3. FUNÇÕES DE DOMÍNIO (PLAYERS, BANCO, ETC.)
# =========================================================

def load_players_for_client(client_data_obj):
    if "players" not in client_data_obj:
        client_data_obj["players"] = {}
    return client_data_obj["players"]


def ensure_wallets_and_bank(client_data_obj):
    """Garante que existam estruturas de wallets e bank no client_data."""
    if "wallets" not in client_data_obj:
        client_data_obj["wallets"] = {}
    if "bank" not in client_data_obj:
        client_data_obj["bank"] = {}
    return client_data_obj["wallets"], client_data_obj["bank"]


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

    # --- CARREGA BANCOS ---
    users_db = load_db(DB_USERS, {"keys": {}})
    clients_db = load_db(DB_CLIENTS, {})

    if not users_db.get("keys"):
        st.error("Nenhum servidor disponível no momento.")
        return

    # -----------------------------------------------------
    # 4.1 SELEÇÃO DO SERVIDOR
    # -----------------------------------------------------
    # Monta mapa nome_do_servidor (lower) -> server_id
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

    # Estado de seleção de servidor
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
                # Verifica se este server_id existe em clients_db
                if server_id not in clients_db:
                    st.error(
                        "Servidor ainda não está configurado no sistema. "
                        "Avise o administrador."
                    )
                else:
                    st.session_state.portal_server_id = server_id
                    st.session_state.portal_server_nome = [
                        nome for nome, sid in nome_para_server_id.items() if sid == server_id
                    ][0].title()  # pega o nome original (ajuste simples)
                    st.success(f"Servidor encontrado: {st.session_state.portal_server_nome}")

    # Recupera seleção válida
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
                "discord_id": "",
                "observacoes": observacoes.strip(),
            }

            client_data["players"] = players
            clients_db[server_id] = client_data
            save_db(DB_CLIENTS, clients_db)

            # Guarda gamertag atual na sessão para uso futuro (Banco, Loja, etc.)
            st.session_state.portal_gamertag = gamertag_clean

            st.success(
                f"Gamertag **{gamertag_clean}** vinculada com sucesso ao servidor "
                f"**{st.session_state.portal_server_nome or server_id}**!"
            )
            st.info(
                "No futuro, você poderá usar a Loja e o Banco deste servidor quando essas funções estiverem ativas."
            )

        # -------------------------------------------------
        # 4.3 SEÇÃO BASE PARA 'MEU BANCO' (estrutura pronta)
        # -------------------------------------------------
        st.markdown("---")
        st.markdown("## 💼 Meu Banco (em breve)")

        st.info(
            "Em breve, aqui você poderá ver seu saldo de DzCoins em carteira e no banco, "
            "e transferir entre eles. Por enquanto, apenas o vínculo de Gamertag está ativo."
        )

        # Estrutura de leitura já pronta (sem expor UI de transferência ainda):
        # wallets, bank = ensure_wallets_and_bank(client_data)
        # gamertag_banco = st.session_state.get('portal_gamertag') ou campo manual
        # ... (vamos completar essa parte nos próximos passos)

    else:
        st.info(
            "Nenhum servidor selecionado ainda. Informe o nome do servidor e clique em "
            "'Confirmar servidor'."
        )


# =========================================================
# 5. PONTO DE ENTRADA
# =========================================================

if __name__ == "__main__":
    main()
