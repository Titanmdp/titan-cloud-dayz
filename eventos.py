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
import base64
from datetime import datetime, timedelta, timezone
from streamlit_javascript import st_javascript


# =========================================================
# 1. CONFIG / AMBIENTE / CONSTANTES
# =========================================================

IS_DEV = os.environ.get("IS_DEV", "False") == "True"

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Titan Cloud PRO", layout="wide", page_icon="🚀")

# --- FUSO HORÁRIO (BRASÍLIA) ---
FUSO_BR = timezone(timedelta(hours=-3))

def get_hora_brasilia():
    return datetime.now(FUSO_BR)

# --- BANCO DE DADOS ---
if os.environ.get("RENDER"):
    MOUNT_PATH = "/var/data"
else:
    MOUNT_PATH = "."

DB_USERS   = os.path.join(MOUNT_PATH, "users_db.json")
DB_CLIENTS = os.path.join(MOUNT_PATH, "clients_data.json")
UPLOAD_DIR = os.path.join(MOUNT_PATH, "uploads")

os.makedirs(UPLOAD_DIR, exist_ok=True)

# --- PLANOS ---
PLANOS = {
    "Starter":    2,
    "Pro":        10,
    "Enterprise": 999,
}


# =========================================================
# 2. FUNÇÕES UTILITÁRIAS
# =========================================================

def str_to_time(data_str, hora_str):
    try:
        return datetime.strptime(f"{data_str} {hora_str}", "%d/%m/%Y %H:%M").replace(tzinfo=FUSO_BR)
    except Exception:
        return None


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
    if not data or (
        isinstance(data, dict)
        and "admin_key" not in data
        and file == DB_USERS
    ):
        return
    try:
        if os.path.exists(file):
            shutil.copy(file, file + ".bak")
        with open(file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        st.error(f"Erro ao salvar banco de dados: {e}")


def registrar_log(client_id, mensagem, tipo="info"):
    db_disco = load_db(DB_CLIENTS, {})
    if client_id not in db_disco:
        db_disco[client_id] = {
            "ftp": {"host": "", "user": "", "pass": "", "port": "21"},
            "agendas": [], "logs": [],
        }
    if "logs" not in db_disco[client_id]:
        db_disco[client_id]["logs"] = []

    timestamp = get_hora_brasilia().strftime("%H:%M:%S")
    icone = "🟢" if tipo == "sucesso" else "🔴" if tipo == "erro" else "📡"
    db_disco[client_id]["logs"].insert(0, f"[{timestamp}] {icone} {mensagem}")
    db_disco[client_id]["logs"] = db_disco[client_id]["logs"][:50]
    save_db(DB_CLIENTS, db_disco)


def buscar_localizacao_cliente():
    try:
        result = None
        if result is None:
            return {"cidade": "Desconhecido", "regiao": "Desconhecido", "pais": "Desconhecido"}
        return {
            "cidade": result.get("city", "Desconhecido"),
            "regiao": result.get("region", "Desconhecido"),
            "pais":   result.get("country", "Desconhecido"),
        }
    except Exception as e:
        print(f"Erro ao buscar geolocalização: {e}")
        return {"cidade": "Desconhecido", "regiao": "Desconhecido", "pais": "Desconhecido"}


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


# =========================================================
# 3. FTP + WORKER  (definidos ANTES de qualquer uso)
# =========================================================

def disparar_ftp_pro(client_id, acao, filename, local_path, mapa_path):
    db_atual = load_db(DB_CLIENTS, {})
    if client_id not in db_atual:
        return False, "Cliente não encontrado"

    conf = db_atual[client_id]["ftp"]
    try:
        # Verifica se o arquivo existe antes de tentar upload
        if acao == "UPLOAD" and not os.path.exists(local_path):
            print(f"[FTP] ERRO: arquivo local não encontrado: {local_path}")
            registrar_log(client_id, f"Arquivo não encontrado: {filename} — refaça o agendamento", "erro")
            return False, "Arquivo local não encontrado"

        ftp = ftplib.FTP()
        ftp.connect(conf["host"], int(conf["port"]), timeout=15)
        ftp.login(conf["user"], conf["pass"])

        # Log do diretório atual antes do cwd (ajuda diagnóstico)
        print(f"[FTP] Conectado. Navegando para: {mapa_path}")
        ftp.cwd(mapa_path)
        print(f"[FTP] Diretório atual: {ftp.pwd()}")

        if acao == "UPLOAD":
            with open(local_path, "rb") as f:
                ftp.storbinary(f"STOR {filename}", f)
            print(f"[FTP] UPLOAD OK: {filename} → {mapa_path}")

        elif acao == "DELETE":
            try:
                ftp.delete(filename)
                print(f"[FTP] DELETE OK: {filename}")
            except Exception as e:
                print(f"[FTP] DELETE falhou (arquivo pode não existir): {e}")

        ftp.quit()
        return True, "Sucesso"

    except Exception as e:
        print(f"[FTP] ERRO geral ({acao}): {e}")
        registrar_log(client_id, f"Erro FTP ({acao}): {str(e)}", "erro")
        return False, str(e)


def pro_worker():
    while True:
        try:
            now    = get_hora_brasilia()
            db_all = load_db(DB_CLIENTS, {})
            mudou  = False

            for c_id, c_info in db_all.items():
                for ag in c_info.get("agendas", []):
                    hora_entrada = str_to_time(ag["data"], ag["in"])
                    hora_saida   = str_to_time(ag["data"], ag["out"])

                    # ---- UPLOAD ----
                    if (
                        hora_entrada
                        and now >= hora_entrada
                        and ag.get("status") == "Aguardando"
                    ):
                        # Tenta recriar o arquivo a partir do base64 salvo (sobrevive a redeploy)
                        if not os.path.exists(ag["local_path"]) and ag.get("file_content"):
                            try:
                                os.makedirs(os.path.dirname(ag["local_path"]), exist_ok=True)
                                with open(ag["local_path"], "wb") as f:
                                    f.write(base64.b64decode(ag["file_content"]))
                                print(f"[worker] Arquivo recriado do base64: {ag['local_path']}")
                            except Exception as e:
                                print(f"[worker] Erro ao recriar arquivo: {e}")

                        if not os.path.exists(ag["local_path"]):
                            ag["status"] = "Erro"
                            registrar_log(c_id, f"Arquivo perdido e sem backup: {ag['file']} — recadastre", "erro")
                            mudou = True
                        else:
                            success, msg = disparar_ftp_pro(
                                c_id, "UPLOAD", ag["file"], ag["local_path"], ag["path"]
                            )
                            ag["status"] = "Ativo" if success else "Erro"
                            registrar_log(
                                c_id,
                                f"UPLOAD {ag['file']}: {'OK' if success else msg}",
                                "sucesso" if success else "erro",
                            )
                            mudou = True

                    # ---- DELETE ----
                    if (
                        hora_saida
                        and now >= hora_saida
                        and ag.get("status") == "Ativo"
                    ):
                        success, msg = disparar_ftp_pro(
                            c_id, "DELETE", ag["file"], ag["local_path"], ag["path"]
                        )
                        registrar_log(
                            c_id,
                            f"DELETE {ag['file']}: {'OK' if success else msg}",
                            "sucesso" if success else "erro",
                        )
                        if ag.get("rec") == "Diário":
                            ag["data"]   = (now + timedelta(days=1)).strftime("%d/%m/%Y")
                            ag["status"] = "Aguardando"
                        elif ag.get("rec") == "Semanal":
                            ag["data"]   = (now + timedelta(days=7)).strftime("%d/%m/%Y")
                            ag["status"] = "Aguardando"
                        else:
                            ag["status"] = "Finalizado"
                        mudou = True

            if mudou:
                save_db(DB_CLIENTS, db_all)

        except Exception as e:
            print(f"[pro_worker] erro: {e}")

        time.sleep(30)


# --- Controle de thread única ---
_WORKER_STARTED = False

def start_worker_once():
    global _WORKER_STARTED
    if not _WORKER_STARTED:
        _WORKER_STARTED = True
        threading.Thread(target=pro_worker, daemon=True).start()
        print("[worker] Thread iniciada.")


# --- Keep-alive Render ---
def manter_vivo():
    while True:
        try:
            requests.get("https://titan-cloud-dayz.onrender.com", timeout=10)
        except Exception:
            pass
        time.sleep(600)

threading.Thread(target=manter_vivo, daemon=True).start()


# =========================================================
# 4. INICIALIZAÇÃO DE ESTADO
# =========================================================

if "db_users" not in st.session_state:
    st.session_state.db_users = load_db(DB_USERS, {"admin_key": "ALEX_ADMIN", "keys": {}})

if "db_clients" not in st.session_state:
    st.session_state.db_clients = load_db(DB_CLIENTS, {})

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "user_key" not in st.session_state:
    st.session_state.user_key = None
if "view_mode" not in st.session_state:
    st.session_state.view_mode = "admin"

if IS_DEV:
    st.sidebar.warning("⚠️ AMBIENTE DE DESENVOLVIMENTO (TESTES)")


# =========================================================
# 5. TELA DE LOGIN
# =========================================================

if not st.session_state.authenticated:
    st.title("🔑 Titan Cloud - Login")

    dados_geo  = buscar_localizacao_cliente()
    login_key  = st.text_input("Insira sua KeyUser", type="password")

    if st.button("Entrar no Painel", use_container_width=True):
        ok, cargo = validar_acesso(login_key)

        if ok:
            token_sessao = secrets.token_hex(8)

            if dados_geo and isinstance(dados_geo, dict):
                cidade      = dados_geo.get("cidade", "Desconhecido")
                estado      = dados_geo.get("estado", dados_geo.get("regiao", "Desconhecido"))
                local_final = f"{cidade} - {estado}"
            else:
                local_final = "Localização não capturada"

            if cargo == "client":
                st.session_state.db_users["keys"][login_key]["last_session"] = token_sessao
                st.session_state.db_users["keys"][login_key]["local"]        = local_final
                st.session_state.db_users["keys"][login_key]["last_login"]   = (
                    get_hora_brasilia().strftime("%d/%m/%Y %H:%M:%S")
                )
                save_db(DB_USERS, st.session_state.db_users)

            st.session_state.authenticated  = True
            st.session_state.user_key       = login_key
            st.session_state.role           = cargo
            st.session_state.session_token  = token_sessao
            st.session_state.view_mode      = "admin" if cargo == "admin" else "client"

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
            st.session_state.authenticated = False
            st.rerun()

    st.title("🛡️ Painel de Controle - Administrador")

    tab_adm1, tab_adm2, tab_adm3, tab_adm4, tab_adm5 = st.tabs([
        "➕ Gerar Chaves",
        "👥 Gestão de Clientes",
        "⚙️ Configurar Planos",
        "💾 Backup/Restore",
        "📢 Comunicados",
    ])

    with tab_adm1:
        with st.expander("Gerador de Chaves", expanded=True):
            col_gen1, col_gen2 = st.columns([2, 1])
            with col_gen1:
                srv_name  = st.text_input("Nome do Servidor / Cliente")
                plano_sel = st.selectbox("Escolha o Plano", list(PLANOS.keys()))
                if "temp_key" not in st.session_state:
                    st.session_state.temp_key = ""
                ck1, ck2 = st.columns([3, 1])
                new_k = ck1.text_input("KeyUser", value=st.session_state.temp_key)
                if ck2.button("🎲 Gerar"):
                    st.session_state.temp_key = "".join(
                        secrets.choice(string.ascii_uppercase + string.digits) for _ in range(12)
                    )
                    st.rerun()
            with col_gen2:
                dias_v = st.number_input("Dias de validade", min_value=1, value=30)
                if st.button("🚀 Registrar e Ativar", use_container_width=True):
                    if srv_name and new_k:
                        data_exp = (get_hora_brasilia() + timedelta(days=dias_v)).strftime("%d/%m/%Y")
                        st.session_state.db_users["keys"][new_k] = {
                            "server":  srv_name,
                            "expires": data_exp,
                            "plano":   plano_sel,
                        }
                        save_db(DB_USERS, st.session_state.db_users)
                        st.session_state.temp_key = ""
                        st.success(f"Chave para '{srv_name}' ativada!")
                        st.rerun()

    with tab_adm2:
        st.subheader("👥 Gestão de Clientes Ativos")
        if not st.session_state.db_users["keys"]:
            st.info("Nenhum cliente cadastrado no momento.")

        for k, v in list(st.session_state.db_users["keys"].items()):
            dt_exp_check   = datetime.strptime(v["expires"], "%d/%m/%Y").date()
            dias_rest      = (dt_exp_check - get_hora_brasilia().date()).days
            cor_status     = "🟢" if dias_rest > 0 else "🔴"
            limites_globais = st.session_state.db_users.get("config_planos", PLANOS)
            uso_atual      = len(st.session_state.db_clients.get(k, {}).get("agendas", []))
            limite_padrao  = limites_globais.get(v.get("plano", "Starter"), 2)
            limite_final   = v.get("limite_extra", limite_padrao)

            with st.expander(f"{cor_status} {v['server']} | {v.get('plano','Starter')} ({uso_atual}/{limite_final})"):
                st.markdown("### 🔑 Credenciais de Acesso")
                st.code(k)
                st.divider()

                st.markdown("#### 🌐 Monitoramento e Segurança")
                col_mon1, col_mon2 = st.columns(2)
                with col_mon1:
                    st.write(f"**📍 Localização:** {v.get('local','Nenhum acesso registrado')}")
                    st.write(f"**🖥️ IP:** {v.get('last_ip','0.0.0.0')}")
                with col_mon2:
                    st.write(f"**🕒 Último Login:** {v.get('last_login','---')}")
                    if st.button("🚫 Banir Acesso (Expirar Key)", key=f"ban_{k}", type="primary", use_container_width=True):
                        v["expires"] = (get_hora_brasilia() - timedelta(days=1)).strftime("%d/%m/%Y")
                        save_db(DB_USERS, st.session_state.db_users)
                        st.warning(f"O acesso de {v['server']} foi bloqueado.")
                        st.rerun()

                st.divider()
                c_edit1, c_edit2 = st.columns(2)
                with c_edit1:
                    st.markdown("#### 📝 Informações e Plano")
                    new_n   = st.text_input("Editar Nome", value=v["server"], key=f"n_{k}")
                    new_p   = st.selectbox("Trocar Plano", list(PLANOS.keys()),
                                           index=list(PLANOS.keys()).index(v.get("plano","Starter")), key=f"p_{k}")
                    new_lim = st.number_input("Ajustar Limite", min_value=1, value=int(limite_final), key=f"lim_{k}")
                    st.markdown("#### 📧 Contatos de Notificação")
                    new_mail = st.text_input("E-mail do Cliente", value=v.get("email",""), key=f"mail_{k}")
                    new_wa   = st.text_input("WhatsApp (com DDD)", value=v.get("whatsapp",""), key=f"wa_{k}")
                    if st.button("💾 Salvar Alterações", key=f"bn_{k}", use_container_width=True):
                        st.session_state.db_users["keys"][k]["server"]      = new_n
                        st.session_state.db_users["keys"][k]["plano"]       = new_p
                        st.session_state.db_users["keys"][k]["limite_extra"] = new_lim
                        st.session_state.db_users["keys"][k]["email"]       = new_mail
                        st.session_state.db_users["keys"][k]["whatsapp"]    = new_wa
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
                if st.button("🗑️ EXCLUIR CLIENTE PERMANENTEMENTE", key=f"del_{k}", type="primary", use_container_width=True):
                    del st.session_state.db_users["keys"][k]
                    if k in st.session_state.db_clients:
                        del st.session_state.db_clients[k]
                    save_db(DB_USERS, st.session_state.db_users)
                    save_db(DB_CLIENTS, st.session_state.db_clients)
                    st.rerun()

    with tab_adm3:
        st.subheader("⚙️ Configuração Global de Limites")
        if "config_planos" not in st.session_state.db_users:
            st.session_state.db_users["config_planos"] = PLANOS.copy()
        conf_planos = st.session_state.db_users["config_planos"]
        col_p1, col_p2, col_p3 = st.columns(3)
        with col_p1:
            novo_starter = st.number_input("Starter",    min_value=1, value=conf_planos.get("Starter", 2),   key="conf_starter")
        with col_p2:
            novo_pro     = st.number_input("Pro",        min_value=1, value=conf_planos.get("Pro", 10),       key="conf_pro")
        with col_p3:
            novo_ent     = st.number_input("Enterprise", min_value=1, value=conf_planos.get("Enterprise",999), key="conf_ent")
        if st.button("🚀 Aplicar Limites Globais", use_container_width=True):
            st.session_state.db_users["config_planos"] = {
                "Starter": novo_starter, "Pro": novo_pro, "Enterprise": novo_ent
            }
            save_db(DB_USERS, st.session_state.db_users)
            st.success("Limites globais atualizados!")
            time.sleep(1)
            st.rerun()

    with tab_adm4:
        st.subheader("📦 Central de Migração de Dados")
        st.info("Faça backup antes de atualizar e restaure logo após o deploy.")
        col_back, col_rest = st.columns(2)
        with col_back:
            st.markdown("### ⬇️ Exportar Backup")
            dados_totais = {"users": st.session_state.db_users, "clients": st.session_state.db_clients}
            json_string  = json.dumps(dados_totais, indent=4, ensure_ascii=False)
            st.download_button(
                label="💾 Baixar Backup Geral (JSON)",
                data=json_string,
                file_name=f"backup_titan_{get_hora_brasilia().strftime('%d_%m_%Y')}.json",
                mime="application/json",
                use_container_width=True,
            )
        with col_rest:
            st.markdown("### ⬆️ Importar/Restaurar")
            arquivo_upload = st.file_uploader("Selecione o arquivo de backup", type="json")
            if st.button("🚀 Restaurar Dados Agora", use_container_width=True, type="primary"):
                if arquivo_upload is not None:
                    try:
                        backup_data = json.load(arquivo_upload)
                        if "users" in backup_data and "clients" in backup_data:
                            st.session_state.db_users   = backup_data["users"]
                            st.session_state.db_clients = backup_data["clients"]
                            save_db(DB_USERS,   st.session_state.db_users)
                            save_db(DB_CLIENTS, st.session_state.db_clients)
                            st.success("✅ Restauração concluída!")
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error("❌ Arquivo inválido!")
                    except Exception as e:
                        st.error(f"❌ Erro: {e}")

    with tab_adm5:
        st.subheader("📢 Enviar Comunicado Oficial")
        col_c1, col_c2 = st.columns([1, 2])
        with col_c1:
            opcoes_clientes = {v["server"]: k for k, v in st.session_state.db_users["keys"].items()}
            alvos = st.multiselect("Enviar para:", options=["Todos"] + list(opcoes_clientes.keys()), default="Todos")
            st.write("**Enviar via:**")
            send_sys  = st.checkbox("Painel (Sistema)", value=True, disabled=True)
            send_mail = st.checkbox("E-mail")
            send_wa   = st.checkbox("WhatsApp")
            send_disc = st.checkbox("Discord (Webhook do Cliente)")
        with col_c2:
            titulo_com = st.text_input("Título do Comunicado", placeholder="Ex: Manutenção Programada", key="tit_com")
            corpo_com  = st.text_area("Mensagem", height=200, placeholder="Escreva aqui os detalhes...", key="msg_com")
            if st.button("🚀 Disparar Comunicado", use_container_width=True, type="primary"):
                if titulo_com and corpo_com:
                    st.session_state.db_users   = load_db(DB_USERS,   {"admin_key": "ALEX_ADMIN", "keys": {}})
                    st.session_state.db_clients = load_db(DB_CLIENTS, {})
                    destinatarios = (
                        list(st.session_state.db_users["keys"].keys())
                        if "Todos" in alvos
                        else [opcoes_clientes[nome] for nome in alvos]
                    )
                    comunicado_obj = {
                        "id":       str(time.time()),
                        "data":     get_hora_brasilia().strftime("%d/%m/%Y %H:%M"),
                        "titulo":   titulo_com,
                        "mensagem": corpo_com,
                        "lido":     False,
                    }
                    sucesso_ext = falha_ext = 0
                    for d_id in destinatarios:
                        if d_id not in st.session_state.db_clients:
                            st.session_state.db_clients[d_id] = {
                                "ftp": {"host":"","user":"","pass":"","port":"21"},
                                "agendas": [], "logs": [], "comunicados": [],
                            }
                        if "comunicados" not in st.session_state.db_clients[d_id]:
                            st.session_state.db_clients[d_id]["comunicados"] = []
                        st.session_state.db_clients[d_id]["comunicados"].insert(0, comunicado_obj)
                        if send_disc:
                            webhook_url = st.session_state.db_clients.get(d_id, {}).get("discord_webhook")
                            if webhook_url:
                                try:
                                    requests.post(webhook_url, json={"embeds": [{
                                        "title": f"📢 {titulo_com}",
                                        "description": corpo_com,
                                        "color": 16711680,
                                    }]}, timeout=5)
                                    sucesso_ext += 1
                                except Exception:
                                    falha_ext += 1
                    save_db(DB_CLIENTS, st.session_state.db_clients)
                    st.success(f"✅ Enviado para {len(destinatarios)} clientes!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Preencha o título e a mensagem.")

    st.stop()


# =========================================================
# 7. ÁREA DO CLIENTE
# =========================================================

user_id = st.session_state.user_key

# Sincroniza sempre do disco
st.session_state.db_clients = load_db(DB_CLIENTS, {})
st.session_state.db_users   = load_db(DB_USERS,   {"admin_key": "ALEX_ADMIN", "keys": {}})

if user_id not in st.session_state.db_clients:
    st.session_state.db_clients[user_id] = {
        "ftp": {"host": "", "user": "", "pass": "", "port": "21"},
        "agendas": [], "logs": [], "comunicados": [],
    }
    save_db(DB_CLIENTS, st.session_state.db_clients)

client_data = st.session_state.db_clients[user_id]
user_info   = st.session_state.db_users["keys"].get(
    user_id, {"server": "Servidor", "plano": "Starter", "expires": "01/01/2000"}
)

# Trava de sessão simultânea
if st.session_state.role == "client":
    token_valido = user_info.get("last_session")
    if st.session_state.get("session_token") != token_valido:
        st.error("⚠️ Sessão Finalizada: Esta conta foi conectada em outro local.")
        if st.button("Fazer Login Novamente", use_container_width=True, key="relogin_btn"):
            st.session_state.authenticated = False
            st.rerun()
        st.stop()

plano_atual    = user_info.get("plano", "Starter")
limite_agendas = int(user_info.get(
    "limite_extra",
    st.session_state.db_users.get("config_planos", PLANOS).get(plano_atual, 2),
))
total_agendas = len(client_data.get("agendas", []))

if st.session_state.role == "admin":
    exp_status = "Ilimitado (Admin)"
else:
    try:
        dt_exp_obj     = datetime.strptime(user_info["expires"], "%d/%m/%Y").date()
        dias_restantes = (dt_exp_obj - get_hora_brasilia().date()).days
        exp_status     = f"{max(0, dias_restantes)} dias"
    except Exception:
        exp_status = "Erro na data"

# --- SIDEBAR ---
with st.sidebar:
    st.title("👤 Minha Conta")

    if st.session_state.role == "admin":
        if st.button("⚙️ VOLTAR AO PAINEL ADMIN", type="primary", use_container_width=True, key="back_to_adm_main"):
            st.session_state.view_mode = "admin"
            st.rerun()

    st.write(f"Servidor: **{user_info['server']}**")
    st.write(f"Plano: **{plano_atual}**")
    st.markdown(f"Expira em: **{exp_status}**")

    progresso = min(total_agendas / limite_agendas, 1.0) if limite_agendas > 0 else 0
    st.progress(progresso, text=f"Uso: {total_agendas}/{limite_agendas}")

    st.divider()
    st.subheader("⚙️ Configurações FTP")
    client_data["ftp"]["host"] = st.text_input("Host",  value=client_data["ftp"]["host"], key="f_host_main")
    client_data["ftp"]["user"] = st.text_input("Usuário", value=client_data["ftp"]["user"], key="f_user_main")
    client_data["ftp"]["pass"] = st.text_input("Senha", type="password", value=client_data["ftp"]["pass"], key="f_pass_main")
    client_data["ftp"]["port"] = st.text_input("Porta", value=client_data["ftp"]["port"], key="f_port_main")

    col_f1, col_f2 = st.columns(2)
    if col_f1.button("Salvar Dados", use_container_width=True, key="f_save_main"):
        save_db(DB_CLIENTS, st.session_state.db_clients)
        st.success("Salvo!")
        registrar_log(user_id, "Configurações FTP atualizadas.")

    if col_f2.button("⚡ Testar", use_container_width=True, key="f_test_main"):
        try:
            ftp_t = ftplib.FTP()
            ftp_t.connect(client_data["ftp"]["host"], int(client_data["ftp"]["port"]), timeout=10)
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


# --- TABS PRINCIPAIS ---
st.title(f"🎮 {user_info['server']}")
tab1, tab2, tab3 = st.tabs(["📅 Agendamentos", "📜 Logs", "📢 Comunicados"])

with tab1:
    c1, c2 = st.columns([1, 1.5])

    with c1:
        st.subheader("🚀 Novo Evento")
        if total_agendas >= limite_agendas:
            st.error(f"Limite do plano atingido ({limite_agendas}).")
        else:
            up_file = st.file_uploader("Arquivo", type=["xml", "json"], key="file_agenda")

            # Mantém o arquivo entre reruns
            if up_file is not None:
                st.session_state["last_uploaded_file"] = up_file
            elif "last_uploaded_file" in st.session_state:
                up_file = st.session_state["last_uploaded_file"]

            mapa   = st.selectbox("Mapa", ["Chernarus", "Livonia"], key="map_sel_main")
            dt_ev  = st.date_input("Data", min_value=get_hora_brasilia(), key="date_sel_main")
            h_in   = st.text_input("Entrada", "19:55", key="h_in_main")
            h_out  = st.text_input("Saída",   "21:55", key="h_out_main")
            rec    = st.selectbox("Recorrência", ["Único", "Diário", "Semanal"], key="rec_sel_main")

            if st.button("Confirmar Agendamento", use_container_width=True, key="conf_btn_main"):
                if up_file is None:
                    st.warning("Selecione um arquivo antes de confirmar.")
                else:
                    safe_fn = f"{user_id[:5]}_{up_file.name}"
                    path    = os.path.join(UPLOAD_DIR, safe_fn)

                    # Salva o arquivo em disco
                    try:
                        with open(path, "wb") as f:
                            f.write(up_file.getbuffer())
                    except Exception as e:
                        st.error(f"Erro ao salvar arquivo em disco: {e}")
                        st.stop()

                    # Salva conteúdo em base64 para sobreviver a redeployss
                    with open(path, "rb") as f:
                        file_content_b64 = base64.b64encode(f.read()).decode("utf-8")

                    nova_agenda = {
                        "id":           str(time.time()),
                        "file":         up_file.name,
                        "local_path":   path,
                        "file_content": file_content_b64,
                        "mapa":         mapa,
                        "path":         "/dayzxb_missions/dayzOffline.chernarusplus/custom"
                                        if mapa == "Chernarus"
                                        else "/dayzxb_missions/dayzOffline.enoch/custom",
                        "data":   dt_ev.strftime("%d/%m/%Y"),
                        "in":     h_in,
                        "out":    h_out,
                        "rec":    rec,
                        "status": "Aguardando",
                    }
                    client_data["agendas"].append(nova_agenda)
                    save_db(DB_CLIENTS, st.session_state.db_clients)
                    registrar_log(user_id, f"Agendado: {up_file.name} ({mapa})", "info")

                    st.session_state.pop("last_uploaded_file", None)
                    st.success("Evento agendado com sucesso!")
                    time.sleep(0.5)
                    st.rerun()

    with c2:
        st.subheader("📋 Lista de Execução")
        agendas_lista = client_data.get("agendas", [])

        if not agendas_lista:
            st.info("Nenhum evento agendado.")
        else:
            for agenda in agendas_lista:
                status_atual = agenda.get("status", "Aguardando")
                cor = {"Aguardando": "🔵", "Ativo": "🟢", "Finalizado": "⚪"}.get(status_atual, "🔴")

                with st.expander(f"{cor} {agenda['file']} | 📅 {agenda['data']} | 🗺️ {agenda['mapa']}"):
                    inf1, inf2 = st.columns(2)
                    with inf1:
                        st.write(f"**📄 Arquivo:** `{agenda['file']}`")
                        st.write(f"**🗺️ Mapa:** {agenda['mapa']}")
                        st.write(f"**🔄 Recorrência:** {agenda.get('rec','Único')}")
                    with inf2:
                        st.write(f"**⏰ Janela:** {agenda['in']} > {agenda['out']}")
                        st.write(f"**📌 Status:** {status_atual}")

                    st.divider()
                    if st.button("Remover Agendamento", key=f"rem_main_{agenda['id']}", use_container_width=True, type="secondary"):
                        nome_arquivo = agenda["file"]
                        client_data["agendas"] = [a for a in client_data["agendas"] if a["id"] != agenda["id"]]
                        save_db(DB_CLIENTS, st.session_state.db_clients)
                        registrar_log(user_id, f"Removido: {nome_arquivo}", "info")
                        st.toast(f"Evento {nome_arquivo} removido!")
                        st.rerun()

with tab2:
    st.subheader("📜 Histórico de Atividades")
    db_fresco    = load_db(DB_CLIENTS, {})
    logs_frescos = db_fresco.get(user_id, {}).get("logs", [])
    if not logs_frescos:
        st.info("Sem logs registrados.")
    else:
        if st.button("Limpar Histórico", key="clear_logs_btn"):
            db_fresco[user_id]["logs"] = []
            save_db(DB_CLIENTS, db_fresco)
            st.rerun()
        for log in logs_frescos:
            if "🔴" in log:   st.error(log)
            elif "🟢" in log: st.success(log)
            elif "📡" in log: st.warning(log)
            else:             st.info(log)

with tab3:
    st.subheader("📢 Comunicados Oficiais")
    comunicados = client_data.get("comunicados", [])
    if not comunicados:
        st.info("Nenhum comunicado disponível.")
    else:
        col_c, col_btn = st.columns([2.5, 1])
        with col_btn:
            if st.button("🗑️ Limpar Histórico", use_container_width=True):
                client_data["comunicados"] = []
                save_db(DB_CLIENTS, st.session_state.db_clients)
                st.toast("Histórico limpo com sucesso!")
                st.rerun()
        st.divider()
        for idx, m in enumerate(comunicados):
            with st.expander(f"📌 {m['titulo']} - {m['data']}"):
                st.write(m["mensagem"])
                st.divider()
                if st.button("Remover aviso", key=f"del_msg_{idx}", type="secondary"):
                    client_data["comunicados"].pop(idx)
                    save_db(DB_CLIENTS, st.session_state.db_clients)
                    st.rerun()


# =========================================================
# 8. INICIA O WORKER (garante que rode mesmo sem login)
# =========================================================
start_worker_once()
