import streamlit as st
import pandas as pd
import altair as alt
from datetime import date

from jira_client import fetch_worklogs_by_jql

st.set_page_config(page_title="Jira Worklog Dashboard", layout="wide")
st.title("Jira Worklog Dashboard")

jira_domain = st.secrets["JIRA_DOMAIN"]
email = st.secrets["JIRA_EMAIL"]
api_token = st.secrets["JIRA_API_TOKEN"]
default_jql = st.secrets.get("DEFAULT_JQL", "project = KAN")

st.sidebar.header("Filtri")
jql = st.sidebar.text_area("JQL", value=default_jql, height=80)

refresh = st.sidebar.button("Aggiorna dati")

@st.cache_data(ttl=300, show_spinner=False)
def load_data(jql: str) -> pd.DataFrame:
    rows = fetch_worklogs_by_jql(jira_domain, email, api_token, jql)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["Ore"] = pd.to_numeric(df["Ore"])
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date
    return df

if refresh:
    st.cache_data.clear()

with st.spinner("Caricamento dati da Jira..."):
    try:
        df = load_data(jql)
    except Exception as e:
        st.error("Errore chiamando Jira. Dettagli:")
        st.code(str(e))
        st.stop()

if df.empty:
    st.info("Nessun dato trovato per la JQL inserita.")
    st.stop()

# =========================
# 1) FILTRO DATE (DA / A)
# =========================
min_d = df["Data"].min()
max_d = df["Data"].max()

# fallback nel caso ci siano NaT/None
if pd.isna(min_d) or pd.isna(max_d):
    st.warning("Non riesco a determinare l'intervallo date dai dati.")
    date_from = st.sidebar.date_input("Dal", value=date.today().replace(day=1))
    date_to = st.sidebar.date_input("Al", value=date.today())
else:
    date_from = st.sidebar.date_input("Dal", value=min_d, min_value=min_d, max_value=max_d)
    date_to = st.sidebar.date_input("Al", value=max_d, min_value=min_d, max_value=max_d)

if date_from > date_to:
    st.sidebar.error("Intervallo non valido: 'Dal' deve essere <= 'Al'.")
    st.stop()

df = df[(df["Data"] >= date_from) & (df["Data"] <= date_to)].copy()

if df.empty:
    st.info("Nessun worklog nell’intervallo selezionato.")
    st.stop()

# =========================
# 2) FILTRI Utente / IssueType (single select)
# =========================
users = ["(tutti)"] + sorted([u for u in df["Utente"].dropna().unique().tolist() if str(u).strip()])
types = ["(tutti)"] + sorted([t for t in df["IssueType"].dropna().unique().tolist() if str(t).strip()])

user_sel = st.sidebar.selectbox("Utente", users)
type_sel = st.sidebar.selectbox("Issue Type", types)

df_view = df.copy()
if user_sel != "(tutti)":
    df_view = df_view[df_view["Utente"] == user_sel]
if type_sel != "(tutti)":
    df_view = df_view[df_view["IssueType"] == type_sel]

df_view = df_view.sort_values(["Data", "Utente", "Issue"])
