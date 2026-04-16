import streamlit as st
import ftplib
import os
import json
import time
import threading
from datetime import datetime, timedelta, timezone # Adicionado timezone

# --- CONFIGURAÇÃO DE FUSO HORÁRIO (BRASÍLIA) ---
# Criamos um objeto de fuso horário fixo para UTC-3
FUSO_BR = timezone(timedelta(hours=-3))

def get_hora_brasilia():
    """Retorna o datetime atual no fuso de Brasília"""
    return datetime.now(FUSO_BR)

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Titan Cloud", layout="wide", page_icon="🚀")

# --- BANCO DE DADOS LOCAL ---
CONFIG_FILE = "data_saas.json"
UPLOAD_DIR = "uploads"

if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)

def load_data():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                content = f.read()
                return json.loads(content) if content else {"ftp": {"host": "", "user": "", "pass": "", "port": "21"}, "agendas": []}
        except: pass
    return {"ftp": {"host": "", "user": "", "pass": "", "port": "21"}, "agendas": []}

def save_data(data_to_save):
    with open(CONFIG_FILE, "w") as f:
        json.dump(data_to_save, f, indent=4)

# --- MOTOR DE AUTOMAÇÃO ---
def disparar_ftp(acao, filename, local_path):
    conf = load_data()["ftp"]
    try:
        ftp = ftplib.FTP()
        ftp.connect(conf["host"], int(conf["port"]), timeout=15)
        ftp.login(conf["user"], conf["pass"])
        ftp.cwd("/dayzxb_missions/dayzOffline.chernarusplus/custom")
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

def automatic_worker():
    while True:
        # USA HORA DE BRASÍLIA PARA O MOTOR
        now = get_hora_brasilia()
        hoje = now.strftime("%d/%m/%Y")
        agora = now.strftime("%H:%M")
        
        current_data = load_data()
        mudou = False
        for agenda in current_data["agendas"]:
            if agenda["data"] == hoje and agenda["in"] == agora and agenda.get("status") == "Aguardando":
                success, err = disparar_ftp("UPLOAD", agenda["file"], agenda["local_path"])
                agenda["status"] = "Ativo" if success else f"Erro In: {err}"
                mudou = True
            if agenda["data"] == hoje and agenda["out"] == agora and agenda.get("status") == "Ativo":
                success, err = disparar_ftp("DELETE", agenda["file"], agenda["local_path"])
                if success:
                    if agenda["rec"] == "Único":
                        agenda["status"] = "Finalizado"
                    else:
                        fmt = "%d/%m/%Y"
                        dt_obj = datetime.strptime(agenda["data"], fmt)
                        if agenda["rec"] == "Diário": dt_obj += timedelta(days=1)
                        if agenda["rec"] == "Semanal": dt_obj += timedelta(weeks=1)
                        agenda["data"] = dt_obj.strftime(fmt)
                        agenda["status"] = "Aguardando"
                else:
                    agenda["status"] = f"Erro Out: {err}"
                mudou = True
        if mudou: save_data(current_data)
        time.sleep(15)

if 'worker_started' not in st.session_state:
    threading.Thread(target=automatic_worker, daemon=True).start()
    st.session_state['worker_started'] = True

# --- INTERFACE ---
data = load_data()

with st.sidebar:
    st.title("⚙️ Configurações FTP")
    data["ftp"]["host"] = st.text_input("Host", value=data["ftp"]["host"])
    data["ftp"]["user"] = st.text_input("Usuário", value=data["ftp"]["user"])
    data["ftp"]["pass"] = st.text_input("Senha", type="password", value=data["ftp"]["pass"])
    data["ftp"]["port"] = st.text_input("Porta", value=data["ftp"]["port"])
    
    col_save, col_test = st.columns(2)
    with col_save:
        if st.button("Salvar Dados"):
            save_data(data)
            st.success("Salvo!")
    with col_test:
        if st.button("⚡ Testar"):
            try:
                ftp_test = ftplib.FTP()
                ftp_test.connect(data["ftp"]["host"], int(data["ftp"]["port"]), timeout=10)
                ftp_test.login(data["ftp"]["user"], data["ftp"]["pass"])
                ftp_test.quit()
                st.success("OK!")
            except Exception as e:
                st.error(f"Erro: {e}")

st.title("🎮 BR THE LAST WORLD - Painel")

# --- RELÓGIO AJUSTADO PARA BRASÍLIA ---
@st.fragment(run_every="1s")
def show_clock():
    # USA HORA DE BRASÍLIA PARA O DISPLAY
    st.metric(label="🕒 Hora de Brasília", value=get_hora_brasilia().strftime("%H:%M:%S"))

show_clock()

tab1, tab2 = st.tabs(["📅 Agendamentos", "📜 Logs"])

with tab1:
    c1, c2 = st.columns([1, 1.5])
    with c1:
        st.subheader("🚀 Novo Evento")
        up_file = st.file_uploader("Arquivo XML", type=["xml"])
        # Data de hoje também ajustada para o fuso brasileiro no calendário
        dt_ev = st.date_input("Data", min_value=get_hora_brasilia())
        h_in = st.text_input("Entrada (HH:MM)", "19:55")
        h_out = st.text_input("Saída (HH:MM)", "21:55")
        rec = st.selectbox("Recorrência", ["Único", "Diário", "Semanal"])
        
        if st.button("Confirmar Agendamento", use_container_width=True):
            if up_file:
                path = os.path.join(UPLOAD_DIR, up_file.name)
                with open(path, "wb") as f: f.write(up_file.getbuffer())
                nova = {"id": str(time.time()), "file": up_file.name, "local_path": path, "data": dt_ev.strftime("%d/%m/%Y"), "in": h_in, "out": h_out, "rec": rec, "status": "Aguardando"}
                data["agendas"].append(nova)
                save_data(data)
                st.success("Agendado!")
                st.rerun()

    with c2:
        st.subheader("📋 Lista de Execução")
        for i, agenda in enumerate(data["agendas"]):
            cor = {"Aguardando": "🔵", "Ativo": "🟢", "Finalizado": "⚪"}.get(agenda['status'], "🔴")
            with st.expander(f"{cor} {agenda['file']} - {agenda['data']}"):
                st.write(f"Janela: {agenda['in']} > {agenda['out']} | Status: {agenda['status']}")
                if st.button("Remover", key=f"del_{agenda['id']}"):
                    if os.path.exists(agenda['local_path']):
                        try: os.remove(agenda['local_path'])
                        except: pass
                    data["agendas"] = [a for a in data["agendas"] if a["id"] != agenda["id"]]
                    save_data(data)
                    st.rerun()

with tab2:
    st.subheader("Console de Monitoramento")
    # Log de monitoramento também com hora de Brasília
    st.code(f"[{get_hora_brasilia().strftime('%H:%M:%S')}] Motor Ativo (Fuso: Brasília).")
    for agenda in data["agendas"]:
        st.text(f"Arquivo: {agenda['file']} | Status: {agenda['status']} | Data: {agenda['data']}")
