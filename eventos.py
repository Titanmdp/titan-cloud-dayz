import streamlit as st
import ftplib
import os
import json
import time
import threading
import secrets
import string
from datetime import datetime, timedelta, timezone

# --- CONFIGURAÇÃO DE FUSO HORÁRIO (BRASÍLIA) ---
FUSO_BR = timezone(timedelta(hours=-3))

def get_hora_brasilia():
    return datetime.now(FUSO_BR)

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Titan Cloud PRO", layout="wide", page_icon="🚀")

# --- BANCO DE DADOS (JSON) ---
DB_USERS = "users_db.json"
DB_CLIENTS = "clients_data.json"
UPLOAD_DIR = "uploads"

if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)

def load_db(file, default_data):
    if os.path.exists(file):
        try:
            with open(file, "r") as f:
                return json.loads(f.read())
        except: pass
    return default_data

def save_db(file, data):
    with open(file, "w") as f:
        json.dump(data, f, indent=4)

# Inicialização dos dados
if 'db_users' not in st.session_state:
    st.session_state.db_users = load_db(DB_USERS, {"admin_key": "ALEX_ADMIN", "keys": {}})

if 'db_clients' not in st.session_state:
    st.session_state.db_clients = load_db(DB_CLIENTS, {})

# --- LÓGICA DE ACESSO ---
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'user_key' not in st.session_state:
    st.session_state.user_key = None

def validar_acesso(key):
    if key == st.session_state.db_users["admin_key"]:
        return True, "admin"
    keys = st.session_state.db_users["keys"]
    if key in keys:
        validade = datetime.strptime(keys[key]["expires"], "%d/%m/%Y")
        if validade.date() >= get_hora_brasilia().date():
            return True, "client"
        return False, "Sua KeyUser expirou!"
    return False, "KeyUser inválida!"

# --- TELA DE LOGIN ---
if not st.session_state.authenticated:
    st.title("🔑 Titan Cloud - Login")
    login_key = st.text_input("Insira sua KeyUser para acessar o sistema", type="password")
    if st.button("Entrar no Painel", use_container_width=True):
        ok, cargo = validar_acesso(login_key)
        if ok:
            st.session_state.authenticated = True
            st.session_state.user_key = login_key
            st.session_state.role = cargo
            st.rerun()
        else:
            st.error(cargo)
    st.stop()

# --- ÁREA DO ADMINISTRADOR ---
if st.session_state.role == "admin":
    with st.sidebar:
        if st.button("🔴 Logout (Admin)"):
            st.session_state.authenticated = False
            st.rerun()

    st.title("🛡️ Painel de Controle - Administrador")
    
    tab_adm1, tab_adm2 = st.tabs(["➕ Gerar Chaves", "👥 Gestão de Clientes"])

    with tab_adm1:
        with st.expander("Gerador de Chaves", expanded=True):
            col_gen1, col_gen2 = st.columns([2, 1])
            with col_gen1:
                server_name = st.text_input("Nome do Servidor / Cliente", placeholder="Ex: BR Last World")
                if 'temp_key' not in st.session_state: st.session_state.temp_key = ""
                c1, c2 = st.columns([3, 1])
                new_k = c1.text_input("KeyUser", value=st.session_state.temp_key)
                if c2.button("🎲 Gerar"):
                    alphabet = string.ascii_uppercase + string.digits
                    st.session_state.temp_key = ''.join(secrets.choice(alphabet) for i in range(12))
                    st.rerun()
            with col_gen2:
                dias_val = st.number_input("Dias de validade", min_value=1, value=30)
                if st.button("🚀 Registrar e Ativar", use_container_width=True):
                    if server_name and new_k:
                        data_exp = (get_hora_brasilia() + timedelta(days=dias_val)).strftime("%d/%m/%Y")
                        st.session_state.db_users["keys"][new_k] = {
                            "server": server_name,
                            "expires": data_exp,
                            "created_at": get_hora_brasilia().strftime("%d/%m/%Y %H:%M")
                        }
                        save_db(DB_USERS, st.session_state.db_users)
                        st.session_state.temp_key = "" 
                        st.success(f"Chave para '{server_name}' ativada!")
                        st.rerun()

    with tab_adm2:
        st.subheader("Gestão de Clientes Ativos")
        
        for k, v in list(st.session_state.db_users["keys"].items()):
            dt_exp_check = datetime.strptime(v["expires"], "%d/%m/%Y").date()
            dias_rest = (dt_exp_check - get_hora_brasilia().date()).days
            cor_status = "🟢" if dias_rest > 0 else "🔴"
            
            with st.expander(f"{cor_status} {v['server']} | Key: {k}"):
                col_edit1, col_edit2 = st.columns(2)
                
                with col_edit1:
                    new_name = st.text_input("Editar Nome do Servidor", value=v['server'], key=f"edit_n_{k}")
                    if st.button("Salvar Novo Nome", key=f"btn_n_{k}"):
                        st.session_state.db_users["keys"][k]['server'] = new_name
                        save_db(DB_USERS, st.session_state.db_users)
                        st.success("Nome atualizado!")
                        st.rerun()
                
                with col_edit2:
                    st.write(f"**Expiração atual:** {v['expires']} ({dias_rest} dias)")
                    add_dias = st.number_input("Adicionar dias de acesso", min_value=1, value=30, key=f"add_d_{k}")
                    if st.button("➕ Renovar/Estender", key=f"btn_d_{k}"):
                        nova_data = (dt_exp_check + timedelta(days=add_dias)).strftime("%d/%m/%Y")
                        st.session_state.db_users["keys"][k]['expires'] = nova_data
                        save_db(DB_USERS, st.session_state.db_users)
                        st.success("Acesso estendido!")
                        st.rerun()
                
                st.divider()
                if st.button("🗑️ EXCLUIR CLIENTE PERMANENTEMENTE", key=f"del_cli_{k}", type="primary", use_container_width=True):
                    # Remove das chaves e limpa dados de FTP/Agendas
                    del st.session_state.db_users["keys"][k]
                    if k in st.session_state.db_clients:
                        del st.session_state.db_clients[k]
                    save_db(DB_USERS, st.session_state.db_users)
                    save_db(DB_CLIENTS, st.session_state.db_clients)
                    st.warning("Cliente removido do sistema.")
                    st.rerun()

    if st.button("Voltar ao meu painel de uso"):
        st.session_state.role = "client"
        st.rerun()
    st.stop()

# --- ÁREA DO CLIENTE ---
user_id = st.session_state.user_key
if user_id not in st.session_state.db_clients:
    st.session_state.db_clients[user_id] = {"ftp": {"host": "", "user": "", "pass": "", "port": "21"}, "agendas": []}

client_data = st.session_state.db_clients[user_id]
server_info = st.session_state.db_users["keys"].get(user_id, {}).get("server", "Meu Servidor")
exp_date_str = st.session_state.db_users["keys"].get(user_id, {}).get("expires", "01/01/2099")
dt_exp_obj = datetime.strptime(exp_date_str, "%d/%m/%Y").date()
dias_restantes = (dt_exp_obj - get_hora_brasilia().date()).days

# --- MOTOR DE AUTOMAÇÃO ---
def disparar_ftp_pro(client_id, acao, filename, local_path, mapa_path):
    db_atual = load_db(DB_CLIENTS, {})
    if client_id not in db_atual: return False, "Cliente não encontrado"
    conf = db_atual[client_id]["ftp"]
    try:
        ftp = ftplib.FTP()
        ftp.connect(conf["host"], int(conf["port"]), timeout=15)
        ftp.login(conf["user"], conf["pass"])
        ftp.cwd(mapa_path)
        if acao == "UPLOAD" and os.path.exists(local_path):
            with open(local_path, 'rb') as f:
                ftp.storbinary(f'STOR {filename}', f)
        elif acao == "DELETE":
            try: ftp.delete(filename)
            except: pass
        ftp.quit()
        return True, "Sucesso"
    except Exception as e:
        return False, str(e)

def pro_worker():
    while True:
        now = get_hora_brasilia()
        hoje = now.strftime("%d/%m/%Y")
        agora = now.strftime("%H:%M")
        db_all = load_db(DB_CLIENTS, {})
        mudou_global = False
        for c_id, c_info in db_all.items():
            for agenda in c_info["agendas"]:
                if agenda["data"] == hoje and agenda["in"] == agora and agenda.get("status") == "Aguardando":
                    success, err = disparar_ftp_pro(c_id, "UPLOAD", agenda["file"], agenda["local_path"], agenda["path"])
                    agenda["status"] = "Ativo" if success else f"Erro: {err}"
                    mudou_global = True
                if agenda["data"] == hoje and agenda["out"] == agora and agenda.get("status") == "Ativo":
                    success, err = disparar_ftp_pro(c_id, "DELETE", agenda["file"], agenda["local_path"], agenda["path"])
                    if success:
                        if agenda["rec"] == "Único": agenda["status"] = "Finalizado"
                        else:
                            dt = datetime.strptime(agenda["data"], "%d/%m/%Y")
                            if agenda["rec"] == "Diário": dt += timedelta(days=1)
                            if agenda["rec"] == "Semanal": dt += timedelta(weeks=1)
                            agenda["data"] = dt.strftime("%d/%m/%Y")
                            agenda["status"] = "Aguardando"
                    else:
                        agenda["status"] = f"Erro Out: {err}"
                    mudou_global = True
        if mudou_global: save_db(DB_CLIENTS, db_all)
        time.sleep(15)

if 'worker_started' not in st.session_state:
    threading.Thread(target=pro_worker, daemon=True).start()
    st.session_state['worker_started'] = True

# --- INTERFACE DO CLIENTE ---
with st.sidebar:
    st.title("👤 Minha Conta")
    st.write(f"Servidor: **{server_info}**")
    cor_dias = "green" if dias_restantes > 5 else "red"
    st.markdown(f"Acesso expira em: :{cor_dias}[**{dias_restantes} dias**]")
    if st.button("🚪 Sair"):
        st.session_state.authenticated = False
        st.rerun()
    st.divider()
    st.subheader("⚙️ Configurações FTP")
    client_data["ftp"]["host"] = st.text_input("Host", value=client_data["ftp"]["host"])
    client_data["ftp"]["user"] = st.text_input("Usuário", value=client_data["ftp"]["user"])
    client_data["ftp"]["pass"] = st.text_input("Senha", type="password", value=client_data["ftp"]["pass"])
    client_data["ftp"]["port"] = st.text_input("Porta", value=client_data["ftp"]["port"])
    col_save, col_test = st.columns(2)
    with col_save:
        if st.button("Salvar Dados"):
            save_db(DB_CLIENTS, st.session_state.db_clients)
            st.success("Salvo!")
    with col_test:
        if st.button("⚡ Testar"):
            try:
                with st.spinner("Testando..."):
                    ftp_test = ftplib.FTP()
                    ftp_test.connect(client_data["ftp"]["host"], int(client_data["ftp"]["port"]), timeout=10)
                    ftp_test.login(client_data["ftp"]["user"], client_data["ftp"]["pass"])
                    ftp_test.quit()
                    st.success("Conexão OK!")
            except Exception as e:
                st.error(f"Erro: {e}")
    st.divider()
    @st.fragment(run_every="1s")
    def sidebar_clock():
        st.metric(label="🕒 Brasília", value=get_hora_brasilia().strftime("%H:%M:%S"))
    sidebar_clock()

st.title(f"🎮 {server_info}")
tab1, tab2 = st.tabs(["📅 Agendamentos", "📜 Logs"])

with tab1:
    c1, c2 = st.columns([1, 1.5])
    with c1:
        st.subheader("🚀 Novo Evento")
        if 'uploader_id' not in st.session_state: st.session_state.uploader_id = 0
        up_file = st.file_uploader("Arquivo", type=["xml", "json"], key=f"up_{st.session_state.uploader_id}")
        mapa = st.selectbox("Mapa", ["Chernarus", "Livonia"])
        caminhos = {"Chernarus": "/dayzxb_missions/dayzOffline.chernarusplus/custom", "Livonia": "/dayzxb_missions/dayzOffline.enoch/custom"}
        dt_ev = st.date_input("Data", min_value=get_hora_brasilia())
        h_in = st.text_input("Entrada", "19:55")
        h_out = st.text_input("Saída", "21:55")
        rec = st.selectbox("Recorrência", ["Único", "Diário", "Semanal"])
        if st.button("Confirmar Agendamento", use_container_width=True):
            if up_file:
                safe_filename = f"{user_id[:5]}_{up_file.name}"
                path = os.path.join(UPLOAD_DIR, safe_filename)
                with open(path, "wb") as f: f.write(up_file.getbuffer())
                nova = {"id": str(time.time()), "file": up_file.name, "local_path": path, "mapa": mapa, "path": caminhos[mapa], "data": dt_ev.strftime("%d/%m/%Y"), "in": h_in, "out": h_out, "rec": rec, "status": "Aguardando"}
                client_data["agendas"].append(nova)
                save_db(DB_CLIENTS, st.session_state.db_clients)
                st.session_state.uploader_id += 1
                st.success("Agendado!")
                time.sleep(1)
                st.rerun()

    with c2:
        st.subheader("📋 Lista de Execução")
        if not client_data["agendas"]:
            st.info("Nenhum agendamento pendente.")
        for agenda in client_data["agendas"]:
            cor = {"Aguardando": "🔵", "Ativo": "🟢", "Finalizado": "⚪"}.get(agenda['status'], "🔴")
            with st.expander(f"{cor} {agenda['file']} - {agenda['mapa']}"):
                st.markdown(f"**Janela:** {agenda['in']} > {agenda['out']}")
                st.markdown(f"**Data:** {agenda['data']} ({agenda['rec']})")
                st.divider()
                if st.button("Remover Agendamento", key=f"del_{agenda['id']}", type="secondary", use_container_width=True):
                    if os.path.exists(agenda['local_path']):
                        try: os.remove(agenda['local_path'])
                        except: pass
                    client_data["agendas"] = [a for a in client_data["agendas"] if a["id"] != agenda["id"]]
                    save_db(DB_CLIENTS, st.session_state.db_clients)
                    st.rerun()

with tab2:
    st.subheader("Console")
    st.code(f"Motor Ativo | {get_hora_brasilia().strftime('%H:%M:%S')}")
    for agenda in client_data["agendas"]:
        st.text(f"Arquivo: {agenda['file']} | Status: {agenda['status']}")
