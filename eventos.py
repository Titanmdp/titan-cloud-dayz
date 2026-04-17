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
from datetime import datetime, timedelta, timezone
from streamlit_javascript import st_javascript

# --- CONFIGURAÇÃO DE FUSO HORÁRIO (BRASÍLIA) ---
FUSO_BR = timezone(timedelta(hours=-3))
def get_hora_brasilia():
    return datetime.now(FUSO_BR)
    
def buscar_localizacao_cliente():
    # Este script roda no navegador do seu cliente
    url_api = "https://ipapi.co/json/"
    js_code = f"await fetch('{url_api}').then(res => res.json())"
    
    result = st_javascript(js_code)
    
    if result:
        return {
            "cidade": result.get("city", "Desconhecido"),
            "estado": result.get("region", "---")
        }
    return None

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Titan Cloud PRO", layout="wide", page_icon="🚀")

# --- DEFINIÇÃO DE LIMITES POR PLANO (Edite aqui para mudar os valores globais) ---
PLANOS = {
    "Starter": 2,      # Mude o 2 para o valor desejado
    "Pro": 10,         # Mude o 10 para o valor desejado
    "Enterprise": 999  # Mude o 999 para o valor desejado
}

# --- BANCO DE DADOS (JSON) ---
DB_USERS = "users_db.json"
DB_CLIENTS = "clients_data.json"
UPLOAD_DIR = "uploads"

if not os.path.exists(UPLOAD_DIR):
    os.makedirs(UPLOAD_DIR)

def load_db(file, default_data):
    if os.path.exists(file):
        try:
            with open(file, "r", encoding="utf-8") as f:
                conteudo = f.read()
                if not conteudo.strip(): # Se o arquivo existir mas estiver totalmente vazio
                    return default_data
                return json.loads(conteudo)
        except Exception as e:
            # Se der erro na leitura (ex: arquivo corrompido), ele tenta carregar o backup
            backup = file + ".bak"
            if os.path.exists(backup):
                try:
                    with open(backup, "r", encoding="utf-8") as f:
                        return json.loads(f.read())
                except: pass
            print(f"Erro ao carregar {file}: {e}")
    return default_data

def save_db(file, data):
    # Proteção Crítica: Nunca salva se os dados estiverem vazios 
    # (isso evita apagar o arquivo real por um erro de sessão do Streamlit)
    if not data or (isinstance(data, dict) and "admin_key" not in data and file == DB_USERS):
        return 

    try:
        # Cria um backup do arquivo atual antes de sobrescrever
        if os.path.exists(file):
            shutil.copy(file, file + ".bak")
            
        with open(file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        st.error(f"Erro ao salvar banco de dados: {e}")

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
if 'view_mode' not in st.session_state:
    st.session_state.view_mode = "admin"
    
def registrar_log(client_id, mensagem, tipo="info"):
    # Lê o que está no arquivo físico agora
    db_disco = load_db(DB_CLIENTS, {})
    
    # Se o ID do cliente não estiver no arquivo (comum em novos registros)
    if client_id not in db_disco:
        db_disco[client_id] = {"ftp": {"host": "", "user": "", "pass": "", "port": "21"}, "agendas": [], "logs": []}
    
    if "logs" not in db_disco[client_id]: 
        db_disco[client_id]["logs"] = []
    
    timestamp = get_hora_brasilia().strftime("%H:%M:%S")
    icone = "🟢" if tipo == "sucesso" else "🔴" if tipo == "erro" else "📡"
    db_disco[client_id]["logs"].insert(0, f"[{timestamp}] {icone} {mensagem}")
    
    # Mantém apenas os últimos 50 logs para não pesar o arquivo
    db_disco[client_id]["logs"] = db_disco[client_id]["logs"][:50]
    
    # Salva de volta no arquivo imediatamente
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
        # Busca dados baseados no IP do visitante
        response = requests.get('http://ip-api.com/json/', timeout=5).json()
        if response['status'] == 'success':
            return {
                "ip": response['query'],
                "cidade": response['city'],
                "estado": response['regionName'],
                "pais": response['country']
            }
    except:
        return {"ip": "0.0.0.0", "cidade": "Desconhecido", "estado": "---", "pais": "---"}

# --- TELA DE LOGIN ---
if not st.session_state.authenticated:
    st.title("🔑 Titan Cloud - Login")
    
    # 1. Inicia a captura da localização via JavaScript (Roda no navegador do cliente)
    dados_geo = buscar_localizacao_cliente()
    
    login_key = st.text_input("Insira sua KeyUser", type="password")
    
    if st.button("Entrar no Painel", use_container_width=True):
        ok, cargo = validar_acesso(login_key)
        
        if ok:
            # 2. Gera um Token Único para esta sessão (Trava de acesso simultâneo)
            token_sessao = secrets.token_hex(8)
            
            # 3. Formata a localização capturada (ou define como "Não detectado" se o JS falhar/demorar)
            if dados_geo:
                local_final = f"{dados_geo['cidade']} - {dados_geo['estado']}"
            else:
                local_final = "Localização não capturada"
            
            # 4. Se for cliente, atualiza o monitoramento no banco de dados (JSON)
            if cargo == "client":
                st.session_state.db_users["keys"][login_key]["last_session"] = token_sessao
                # Nota: Mantemos o campo 'local' com os dados reais capturados pelo navegador
                st.session_state.db_users["keys"][login_key]["local"] = local_final
                st.session_state.db_users["keys"][login_key]["last_login"] = get_hora_brasilia().strftime("%d/%m/%Y %H:%M:%S")
                
                # Opcional: Se quiser guardar o IP do servidor (Oregon) apenas como registro técnico
                # st.session_state.db_users["keys"][login_key]["last_ip"] = "IP_SERVIDOR" 
                
                save_db(DB_USERS, st.session_state.db_users)

            # 5. Define as variáveis de estado da sessão
            st.session_state.authenticated = True
            st.session_state.user_key = login_key
            st.session_state.role = cargo
            st.session_state.session_token = token_sessao # Guarda o token para validar a trava simultânea
            st.session_state.view_mode = "admin" if cargo == "admin" else "client"
            
            st.rerun()
        else:
            st.error(cargo)
    st.stop()

# --- ÁREA DO ADMINISTRADOR ---
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
    
    # Criando as 3 abas: Gerar, Gerenciar e a nova aba de Configurar Planos
    tab_adm1, tab_adm2, tab_adm3 = st.tabs(["➕ Gerar Chaves", "👥 Gestão de Clientes", "⚙️ Configurar Planos"])

    with tab_adm1:
        with st.expander("Gerador de Chaves", expanded=True):
            col_gen1, col_gen2 = st.columns([2, 1])
            with col_gen1:
                srv_name = st.text_input("Nome do Servidor / Cliente")
                plano_sel = st.selectbox("Escolha o Plano", list(PLANOS.keys()))
                if 'temp_key' not in st.session_state: st.session_state.temp_key = ""
                ck1, ck2 = st.columns([3, 1])
                new_k = ck1.text_input("KeyUser", value=st.session_state.temp_key)
                if ck2.button("🎲 Gerar"):
                    st.session_state.temp_key = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for i in range(12))
                    st.rerun()
            with col_gen2:
                dias_v = st.number_input("Dias de validade", min_value=1, value=30)
                if st.button("🚀 Registrar e Ativar", use_container_width=True):
                    if srv_name and new_k:
                        data_exp = (get_hora_brasilia() + timedelta(days=dias_v)).strftime("%d/%m/%Y")
                        st.session_state.db_users["keys"][new_k] = {
                            "server": srv_name,
                            "expires": data_exp,
                            "plano": plano_sel
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
            # --- TUDO ABAIXO DEVE ESTAR IDENTADO (RECUADO) ---
            dt_exp_check = datetime.strptime(v["expires"], "%d/%m/%Y").date()
            dias_rest = (dt_exp_check - get_hora_brasilia().date()).days
            cor_status = "🟢" if dias_rest > 0 else "🔴"
            
            # Busca limites globais configurados ou usa o padrão inicial
            limites_globais = st.session_state.db_users.get('config_planos', PLANOS)
            uso_atual = len(st.session_state.db_clients.get(k, {}).get("agendas", []))
            limite_padrao = limites_globais.get(v.get('plano', 'Starter'), 2)
            limite_final = v.get('limite_extra', limite_padrao)
            
            with st.expander(f"{cor_status} {v['server']} | {v.get('plano', 'Starter')} ({uso_atual}/{limite_final})"):
                st.markdown("### 🔑 Credenciais de Acesso")
                st.code(k) 
                st.divider()

                # --- MONITORAMENTO DE ACESSOS ---
                st.markdown("#### 🌐 Monitoramento e Segurança")
                col_mon1, col_mon2 = st.columns(2)
                with col_mon1:
                    st.write(f"**📍 Localização:** {v.get('local', 'Nenhum acesso registrado')}")
                    st.write(f"**🖥️ IP:** {v.get('last_ip', '0.0.0.0')}")
                with col_mon2:
                    st.write(f"**🕒 Último Login:** {v.get('last_login', '---')}")
                    if st.button("🚫 Banir Acesso (Expirar Key)", key=f"ban_{k}", type="primary", use_container_width=True):
                        v['expires'] = (get_hora_brasilia() - timedelta(days=1)).strftime("%d/%m/%Y")
                        save_db(DB_USERS, st.session_state.db_users)
                        st.warning(f"O acesso de {v['server']} foi bloqueado.")
                        st.rerun()
                
                st.divider()
                
                c_edit1, c_edit2 = st.columns(2)
                with c_edit1:
                    st.markdown("#### 📝 Informações e Plano")
                    new_n = st.text_input("Editar Nome do Servidor", value=v['server'], key=f"n_{k}")
                    new_p = st.selectbox("Trocar Plano", list(PLANOS.keys()), 
                                         index=list(PLANOS.keys()).index(v.get('plano', 'Starter')), 
                                         key=f"p_{k}")
                    new_lim = st.number_input("Ajustar Limite de Eventos", min_value=1, value=int(limite_final), key=f"lim_{k}")
                    if st.button("💾 Salvar Alterações", key=f"bn_{k}", use_container_width=True):
                        st.session_state.db_users["keys"][k]['server'] = new_n
                        st.session_state.db_users["keys"][k]['plano'] = new_p
                        st.session_state.db_users["keys"][k]['limite_extra'] = new_lim
                        save_db(DB_USERS, st.session_state.db_users)
                        st.success("Dados atualizados!")
                        st.rerun()
                
                with c_edit2:
                    st.markdown("#### 📅 Validade do Acesso")
                    st.write(f"**Expira em:** {v['expires']} ({dias_rest} dias)")
                    add_d = st.number_input("Adicionar dias", min_value=1, value=30, key=f"d_{k}")
                    if st.button("➕ Estender/Renovar", key=f"bd_{k}", use_container_width=True):
                        nova_data = (dt_exp_check + timedelta(days=add_d)).strftime("%d/%m/%Y")
                        st.session_state.db_users["keys"][k]['expires'] = nova_data
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
        st.info("Defina aqui quantos eventos cada plano oferece por padrão.")
        
        # Garante que a configuração exista no banco de dados
        if 'config_planos' not in st.session_state.db_users:
            st.session_state.db_users['config_planos'] = PLANOS.copy()

        conf_planos = st.session_state.db_users['config_planos']
        
        col_p1, col_p2, col_p3 = st.columns(3)
        with col_p1:
            st.metric("Plano", "Starter")
            novo_starter = st.number_input("Qtd. Eventos", min_value=1, value=conf_planos.get('Starter', 2), key="conf_starter")
        with col_p2:
            st.metric("Plano", "Pro")
            novo_pro = st.number_input("Qtd. Eventos", min_value=1, value=conf_planos.get('Pro', 10), key="conf_pro")
        with col_p3:
            st.metric("Plano", "Enterprise")
            novo_ent = st.number_input("Qtd. Eventos", min_value=1, value=conf_planos.get('Enterprise', 999), key="conf_ent")

        if st.button("🚀 Aplicar Limites Globais", use_container_width=True):
            st.session_state.db_users['config_planos'] = {
                "Starter": novo_starter,
                "Pro": novo_pro,
                "Enterprise": novo_ent
            }
            save_db(DB_USERS, st.session_state.db_users)
            st.success("Limites globais atualizados com sucesso!")
            time.sleep(1)
            st.rerun()

    st.stop()
    
    # Adicione isso dentro do bloco 'if st.session_state.role == "admin":'
# Sugestão: Crie uma tab_adm4 nas tabs do admin

with tab_adm4:
        st.subheader("📦 Central de Migração de Dados")
        st.info("Use esta aba para salvar os dados antes de atualizar o código e restaurá-los logo após a atualização.")

        col_back, col_rest = st.columns(2)

        with col_back:
            st.markdown("### ⬇️ Exportar Backup")
            st.write("Baixe o arquivo contendo todos os Clientes, Keys e Agendamentos atuais.")
            
            dados_totais = {
                "users": st.session_state.db_users,
                "clients": st.session_state.db_clients
            }
            
            json_string = json.dumps(dados_totais, indent=4, ensure_ascii=False)
            
            st.download_button(
                label="💾 Baixar Backup Geral (JSON)",
                data=json_string,
                file_name=f"backup_titan_{get_hora_brasilia().strftime('%d_%m_%Y')}.json",
                mime="application/json",
                use_container_width=True
            )

        with col_rest:
            st.markdown("### ⬆️ Importar/Restaurar")
            st.write("Suba um arquivo de backup para restaurar as informações no sistema.")
            
            arquivo_upload = st.file_uploader("Selecione o arquivo de backup", type="json")
            
            if st.button("🚀 Restaurar Dados Agora", use_container_width=True, type="primary"):
                if arquivo_upload is not None:
                    try:
                        backup_data = json.load(arquivo_upload)
                        if "users" in backup_data and "clients" in backup_data:
                            st.session_state.db_users = backup_data["users"]
                            st.session_state.db_clients = backup_data["clients"]
                            
                            save_db(DB_USERS, st.session_state.db_users)
                            save_db(DB_CLIENTS, st.session_state.db_clients)
                            
                            st.success("✅ Restauração concluída com sucesso!")
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error("❌ Arquivo inválido!")
                    except Exception as e:
                        st.error(f"❌ Erro ao processar arquivo: {e}")
                else:
                    st.warning("⚠️ Selecione um arquivo primeiro.")

    # 3. AJUSTE: O st.stop() deve ficar por último, depois de todas as tabs
    st.stop()

# --- ÁREA DO CLIENTE ---
user_id = st.session_state.user_key

# 1. SINCRONIZAÇÃO TOTAL (Evita perda de agendas e garante trava de sessão)
# Forçamos a leitura do disco para que a memória esteja sempre idêntica ao arquivo JSON
db_disco_clients = load_db(DB_CLIENTS, {})
db_disco_users = load_db(DB_USERS, {"admin_key": "ALEX_ADMIN", "keys": {}})

# Atualizamos o estado da sessão com os dados reais do disco
st.session_state.db_clients = db_disco_clients
st.session_state.db_users = db_disco_users

# 2. Inicialização Segura (Cria o perfil do cliente no banco se não existir)
if user_id not in st.session_state.db_clients:
    st.session_state.db_clients[user_id] = {
        "ftp": {"host": "", "user": "", "pass": "", "port": "21"}, 
        "agendas": [],
        "logs": []  
    }
    save_db(DB_CLIENTS, st.session_state.db_clients)

# Apontamos para o conjunto de dados deste cliente específico
client_data = st.session_state.db_clients[user_id]

# Garante que a lista de logs exista (importante para evitar erro de renderização)
if "logs" not in client_data:
    client_data["logs"] = []
    save_db(DB_CLIENTS, st.session_state.db_clients)

# 3. Busca informações de contrato e plano do usuário
user_info = st.session_state.db_users["keys"].get(user_id, {
    "server": "Meu Servidor (Admin)", 
    "plano": "Enterprise", 
    "expires": "31/12/2099"
})

# --- TRAVA DE ACESSO SIMULTÂNEO ---
if st.session_state.role == "client":
    # Verificamos qual foi o último token de login gerado no banco de dados
    token_valido = user_info.get("last_session")
    
    # Se o token deste navegador for diferente do token oficial no disco, desconecta
    if st.session_state.get('session_token') != token_valido:
        st.warning("⚠️ Sessão Finalizada")
        st.error("Esta KeyUser foi conectada em outro dispositivo ou navegador.")
        st.info("Para sua segurança, permitimos apenas um acesso simultâneo por conta.")
        if st.button("Fazer Login Novamente", use_container_width=True):
            st.session_state.authenticated = False
            st.rerun()
        st.stop() # Interrompe a execução aqui para proteger os dados

# --- LÓGICA DE LIMITES (PAINEL DINÂMICO) ---
# Busca limites da Tab 3 ou usa o padrão do topo do código
limites_globais = st.session_state.db_users.get('config_planos', PLANOS)
plano_atual = user_info.get("plano", "Starter")

# Hierarquia: Limite Extra (Tab 2) > Limite Global (Tab 3)
limite_padrao_do_plano = limites_globais.get(plano_atual, 2)
limite_agendas = int(user_info.get("limite_extra", limite_padrao_do_plano))

# Conta total de agendamentos ativos/programados
total_agendas = len(client_data.get("agendas", []))

# --- LÓGICA DE EXPIRAÇÃO ---
if st.session_state.get('role') == "admin":
    exp_status = "Ilimitado (Admin)"
else:
    try:
        dt_exp_obj = datetime.strptime(user_info["expires"], "%d/%m/%Y").date()
        dias_restantes = (dt_exp_obj - get_hora_brasilia().date()).days
        exp_status = f"{max(0, dias_restantes)} dias"
    except Exception:
        exp_status = "Erro na data"

# --- MOTOR DE AUTOMAÇÃO (ROBÔ DE EXECUÇÃO) ---

def disparar_ftp_pro(client_id, acao, filename, local_path, mapa_path):
    """Função central para subir ou deletar arquivos com registro de logs"""
    db_atual = load_db(DB_CLIENTS, {})
    if client_id not in db_atual: 
        return False, "Cliente não encontrado"
    
    conf = db_atual[client_id]["ftp"]
    try:
        ftp = ftplib.FTP()
        ftp.connect(conf["host"], int(conf["port"]), timeout=15)
        ftp.login(conf["user"], conf["pass"])
        ftp.cwd(mapa_path)
        
        if acao == "UPLOAD" and os.path.exists(local_path):
            with open(local_path, 'rb') as f:
                ftp.storbinary(f'STOR {filename}', f)
            registrar_log(client_id, f"Upload concluído: {filename}", "sucesso")
        
        elif acao == "DELETE":
            try: 
                ftp.delete(filename)
                registrar_log(client_id, f"Remoção concluída: {filename}", "sucesso")
            except: 
                pass
        
        ftp.quit()
        return True, "Sucesso"
    except Exception as e:
        registrar_log(client_id, f"Erro no FTP ({acao}): {str(e)}", "erro")
        return False, str(e)

def pro_worker():
    """Loop que verifica agendamentos a cada 15 segundos"""
    while True:
        now = get_hora_brasilia()
        hoje = now.strftime("%d/%m/%Y")
        agora = now.strftime("%H:%M")
        db_all = load_db(DB_CLIENTS, {})
        mudou_global = False
        
        for c_id, c_info in db_all.items():
            for agenda in c_info["agendas"]:
                # --- LÓGICA DE ENTRADA (UPLOAD) ---
                if agenda["data"] == hoje and agenda["in"] == agora and agenda.get("status") == "Aguardando":
                    success, err = disparar_ftp_pro(c_id, "UPLOAD", agenda["file"], agenda["local_path"], agenda["path"])
                    agenda["status"] = "Ativo" if success else f"Erro: {err}"
                    mudou_global = True
                
                # --- LÓGICA DE SAÍDA (DELETE) ---
                if agenda["data"] == hoje and agenda["out"] == agora and agenda.get("status") == "Ativo":
                    success, err = disparar_ftp_pro(c_id, "DELETE", agenda["file"], agenda["local_path"], agenda["path"])
                    if success:
                        if agenda["rec"] == "Único": 
                            agenda["status"] = "Finalizado"
                        else:
                            dt = datetime.strptime(agenda["data"], "%d/%m/%Y")
                            if agenda["rec"] == "Diário": dt += timedelta(days=1)
                            if agenda["rec"] == "Semanal": dt += timedelta(weeks=1)
                            agenda["data"] = dt.strftime("%d/%m/%Y")
                            agenda["status"] = "Aguardando"
                    else:
                        agenda["status"] = f"Erro Out: {err}"
                    mudou_global = True
                    
        if mudou_global:
            save_db(DB_CLIENTS, db_all)
        time.sleep(15)

# Inicia o motor em uma thread separada (se ainda não foi iniciado)
if 'worker_started' not in st.session_state:
    threading.Thread(target=pro_worker, daemon=True).start()
    st.session_state['worker_started'] = True

# --- FIM DO MOTOR DE AUTOMAÇÃO ---

with st.sidebar:
    st.title("👤 Minha Conta")
    
    # Botão de retorno para o Admin (Modo Teste)
    if st.session_state.role == "admin":
        if st.button("⚙️ VOLTAR AO PAINEL ADMIN", type="primary", use_container_width=True):
            st.session_state.view_mode = "admin"
            st.rerun()
            
    st.write(f"Servidor: **{user_info['server']}**")
    st.write(f"Plano: **{plano_atual}**")
    st.markdown(f"Expira em: **{exp_status}**")
    
    # Barra de Progresso do Plano (Uso de slots)
    progresso = min(total_agendas / limite_agendas, 1.0) if limite_agendas > 0 else 0
    st.progress(progresso, text=f"Uso: {total_agendas}/{limite_agendas}")
    
    if st.button("🚪 Sair", use_container_width=True):
        st.session_state.authenticated = False
        st.rerun()
        
    st.divider()
    
    # --- CONFIGURAÇÕES FTP ---
    st.subheader("⚙️ Configurações FTP")
    client_data["ftp"]["host"] = st.text_input("Host", value=client_data["ftp"]["host"])
    client_data["ftp"]["user"] = st.text_input("Usuário", value=client_data["ftp"]["user"])
    client_data["ftp"]["pass"] = st.text_input("Senha", type="password", value=client_data["ftp"]["pass"])
    client_data["ftp"]["port"] = st.text_input("Porta", value=client_data["ftp"]["port"])
    
    c_sv, c_ts = st.columns(2)
    
    # Botão Salvar
    if c_sv.button("Salvar Dados", use_container_width=True):
        save_db(DB_CLIENTS, st.session_state.db_clients)
        st.success("Dados salvos!")
        # Opcional: registrar log de alteração de dados
        registrar_log(user_id, "Configurações FTP atualizadas pelo usuário.")

    # Botão Testar (Com registro de Logs)
    if c_ts.button("⚡ Testar", use_container_width=True):
        try:
            ftp_t = ftplib.FTP()
            # Converte porta para int para evitar erros de conexão
            ftp_t.connect(client_data["ftp"]["host"], int(client_data["ftp"]["port"]), timeout=10)
            ftp_t.login(client_data["ftp"]["user"], client_data["ftp"]["pass"])
            ftp_t.quit()
            
            # Registra o sucesso no histórico do cliente
            registrar_log(user_id, "Conexão FTP testada com sucesso.", "sucesso")
            st.success("Conexão OK!")
            st.rerun() # Recarrega para o log aparecer na aba tab2
            
        except Exception as e:
            # Registra a falha no histórico do cliente
            registrar_log(user_id, f"Falha no teste de conexão: {str(e)}", "erro")
            st.error(f"Erro: {str(e)}")
            st.rerun()

    st.divider()
    
    # Relógio em tempo real
    @st.fragment(run_every="1s")
    def sidebar_clock():
        st.metric(label="🕒 Brasília", value=get_hora_brasilia().strftime("%H:%M:%S"))
    sidebar_clock()

# --- TÍTULO E ABAS ---
st.title(f"🎮 {user_info['server']}")
tab1, tab2 = st.tabs(["📅 Agendamentos", "📜 Logs"])

with tab1:
    c1, c2 = st.columns([1, 1.5])
    
    with c1:
        st.subheader("🚀 Novo Evento")
        if total_agendas >= limite_agendas:
            st.error(f"Limite do plano {plano_atual} atingido ({limite_agendas}).")
        else:
            if 'uploader_id' not in st.session_state: st.session_state.uploader_id = 0
            up_file = st.file_uploader("Arquivo", type=["xml", "json"], key=f"up_{st.session_state.uploader_id}")
            mapa = st.selectbox("Mapa", ["Chernarus", "Livonia"])
            caminhos = {
                "Chernarus": "/dayzxb_missions/dayzOffline.chernarusplus/custom", 
                "Livonia": "/dayzxb_missions/dayzOffline.enoch/custom"
            }
            dt_ev = st.date_input("Data", min_value=get_hora_brasilia())
            h_in = st.text_input("Entrada", "19:55")
            h_out = st.text_input("Saída", "21:55")
            rec = st.selectbox("Recorrência", ["Único", "Diário", "Semanal"])
            
            if st.button("Confirmar Agendamento", use_container_width=True):
                if up_file:
                    safe_filename = f"{user_id[:5]}_{up_file.name}"
                    path = os.path.join(UPLOAD_DIR, safe_filename)
                    with open(path, "wb") as f: 
                        f.write(up_file.getbuffer())
                    
                    nova = {
                        "id": str(time.time()), 
                        "file": up_file.name, 
                        "local_path": path, 
                        "mapa": mapa, 
                        "path": caminhos[mapa], 
                        "data": dt_ev.strftime("%d/%m/%Y"), 
                        "in": h_in, 
                        "out": h_out, 
                        "rec": rec, 
                        "status": "Aguardando"
                    }
                    
                    client_data["agendas"].append(nova)
                    save_db(DB_CLIENTS, st.session_state.db_clients)
                    
                    # --- NOVO: LOG DE CRIAÇÃO ---
                    registrar_log(user_id, f"Novo agendamento criado: {up_file.name} ({mapa})", "info")
                    
                    st.success("Evento agendado com sucesso!")
                    st.session_state.uploader_id += 1
                    st.rerun()
                else:
                    st.warning("Por favor, selecione um arquivo.")

    with c2:
        st.subheader("📋 Lista de Execução")
        
        # Verifica se existem agendas para evitar erros no loop
        if not client_data.get("agendas"):
            st.info("Nenhum evento agendado no momento.")
        else:
            for agenda in client_data["agendas"]:
                # Define a cor do status
                status_atual = agenda.get('status', 'Aguardando')
                cor = {"Aguardando": "🔵", "Ativo": "🟢", "Finalizado": "⚪"}.get(status_atual, "🔴")
                
                with st.expander(f"{cor} {agenda['file']} - {agenda['mapa']}"):
                    st.write(f"**Janela:** {agenda['in']} > {agenda['out']} | **Data:** {agenda['data']}")
                    st.write(f"**Recorrência:** {agenda['rec']} | **Status:** {status_atual}")
                    
                    # Botão de Remoção
                    if st.button("Remover Agendamento", key=f"del_{agenda['id']}", use_container_width=True, type="secondary"):
                        
                        # 1. Armazena o nome do arquivo antes de remover da memória
                        nome_arquivo = agenda['file']
                        
                        # 2. Filtra a lista para remover o item da memória
                        client_data["agendas"] = [a for a in client_data["agendas"] if a["id"] != agenda["id"]]
                        
                        # 3. Salva a lista de agendas atualizada no arquivo físico
                        save_db(DB_CLIENTS, st.session_state.db_clients)
                        
                        # 4. Registra o LOG POR ÚLTIMO 
                        # (A função registrar_log abre o arquivo, adiciona a linha e salva sozinha)
                        registrar_log(user_id, f"Agendamento removido manualmente: {nome_arquivo}", "info")
                        
                        # 5. Feedback visual e recarregamento
                        st.toast(f"Evento {nome_arquivo} removido!")
                        st.rerun()
                    
with tab2:
    st.subheader("📜 Histórico de Atividades")
    
    # FORÇAR RECARREGAMENTO: Lê o banco de dados do arquivo para pegar o que o robô escreveu
    db_fresco = load_db(DB_CLIENTS, {})
    logs_frescos = db_fresco.get(user_id, {}).get("logs", [])
    
    if not logs_frescos:
        st.info("Nenhuma atividade registrada nos logs ainda.")
    else:
        # Botão para limpar logs (opcional, mas ajuda a testar)
        if st.button("Limpar Histórico"):
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
