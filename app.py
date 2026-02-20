import streamlit as st
import pandas as pd
import altair as alt
from datetime import date

from jira_client import fetch_worklogs_by_jql


# ======================
# STREAMLIT CONFIG
# ======================
st.set_page_config(page_title="Jira Worklog Dashboard", layout="wide")
st.title("Jira Worklog Dashboard")


# ======================
# SECRETS
# ======================
jira_domain = st.secrets["JIRA_DOMAIN"]
email = st.secrets["JIRA_EMAIL"]
api_token = st.secrets["JIRA_API_TOKEN"]
default_jql = st.secrets.get("DEFAULT_JQL", "project = KAN AND created >= \"2026-01-01\"")


# ======================
# SIDEBAR - INPUT
# ======================
st.sidebar.header("Filtri")

jql = st.sidebar.text_area("JQL", value=default_jql, height=80)
refresh = st.sidebar.button("Aggiorna dati")


# ======================
# DATA LOADING
# ======================
@st.cache_data(ttl=300, show_spinner=False)
def load_data(jql: str) -> pd.DataFrame:
    rows = fetch_worklogs_by_jql(
        jira_domain=jira_domain,
        email=email,
        api_token=api_token,
        jql=jql,
    )
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Normalizzazioni
    df["Ore"] = pd.to_numeric(df["Ore"], errors="coerce").fillna(0.0)
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date

    # Elimina righe senza data (worklog malformati)
    df = df.dropna(subset=["Data"])

    return df


if refresh:
    st.cache_data.clear()

with st.spinner("Caricamento dati da Jira..."):
    try:
        df_all = load_data(jql)
    except Exception as e:
        st.error("Errore chiamando Jira. Dettagli tecnici:")
        st.code(str(e))
        st.stop()

if df_all.empty:
    st.info("Nessun dato trovato per la JQL inserita.")
    st.stop()


# ======================
# SIDEBAR - DATE FILTER (DA / A) + OTHER FILTERS
# ======================
min_d = df_all["Data"].min()
max_d = df_all["Data"].max()

# Date picker vincolati ai dati disponibili
date_from = st.sidebar.date_input("Dal", value=min_d, min_value=min_d, max_value=max_d)
date_to = st.sidebar.date_input("Al", value=max_d, min_value=min_d, max_value=max_d)

if date_from > date_to:
    st.sidebar.error("Intervallo non valido: 'Dal' deve essere <= 'Al'.")
    st.stop()

# Applica filtro date
df = df_all[(df_all["Data"] >= date_from) & (df_all["Data"] <= date_to)].copy()

if df.empty:
    st.info("Nessun worklog nell’intervallo selezionato.")
    st.stop()

# Filtri single-select
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

if df_view.empty:
    st.info("Nessun dato dopo l’applicazione dei filtri selezionati.")
    st.stop()


# ======================
# KPI
# ======================
c1, c2, c3 = st.columns(3)
c1.metric("Totale ore", f"{df_view['Ore'].sum():.2f}")
c2.metric("N. worklog", f"{len(df_view)}")
c3.metric("N. issue", f"{df_view['Issue'].nunique()}")

st.divider()


# ======================
# MAIN LAYOUT
# ======================
left, right = st.columns([2, 1])

with left:
    st.subheader("Dettaglio")

    # Formattazione per visualizzazione
    df_show = df_view.copy()
    df_show["Data"] = pd.to_datetime(df_show["Data"]).dt.strftime("%d/%m/%Y")

    # Ordine colonne richiesto (Data + Issue Type incluse)
    df_show = df_show[["Data", "Utente", "IssueType", "Issue", "Summary", "Ore"]]

    st.dataframe(df_show, use_container_width=True, hide_index=True)

    st.download_button(
        "Download CSV",
        data=df_show.to_csv(index=False).encode("utf-8"),
        file_name=f"worklog_{date_from.isoformat()}_{date_to.isoformat()}.csv",
        mime="text/csv",
    )

with right:
    st.subheader("Ore per utente")
    agg = df_view.groupby("Utente", as_index=False)["Ore"].sum().sort_values("Ore", ascending=False)

    chart = (
        alt.Chart(agg)
        .mark_bar()
        .encode(
            x=alt.X("Ore:Q", title="Ore"),
            y=alt.Y("Utente:N", sort="-x", title=""),
            tooltip=["Utente", "Ore"],
        )
        .properties(height=420)
    )
    st.altair_chart(chart, use_container_width=True)
