import streamlit as st

st.set_page_config(
    page_title="Titan Cloud Pro",
    page_icon="🎮",
    layout="wide",
)

pages = {
    "Portais": [
        # Portal do Administrador → novo arquivo portal_admin.py
        st.Page("portal_admin.py", title="Portal do Administrador", icon="🛡️"),
        # Portal do Player → arquivo dentro de pages/
        st.Page("pages/player_portal.py", title="Portal do Player", icon="🎮"),
    ]
}

nav = st.navigation(pages)
nav.run()
