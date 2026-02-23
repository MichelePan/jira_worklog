import streamlit as st
import pandas as pd
from datetime import date, timedelta
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed

from jira_client import search_issues_jql_v3, get_issue_worklogs_v3


# ======================
# STREAMLIT CONFIG
# ======================
st.set_page_config(page_title="Jira Worklog Dashboard", layout="wide")
st.title("Jira Worklog Dashboard")


# ======================
# SECRETS / CONFIG
# ======================
jira_domain = st.secrets["JIRA_DOMAIN"]
email = st.secrets["JIRA_EMAIL"]
api_token = st.secrets["JIRA_API_TOKEN"]
default_jql = st.secrets.get("DEFAULT_JQL", "project = KAN")

BASE_URL = f"https://{jira_domain}/rest/api/3"
AUTH = HTTPBasicAuth(email, api_token)

# Performance knobs (non in sidebar)
MAX_WORKERS = 10
MARGIN_DAYS = 3  # margine fisso per pre-filtro su updated


# ======================
# SIDEBAR
# ======================
st.sidebar.header("Filtri")

today = date.today()
date_from = st.sidebar.date_input("Dal", value=today - timedelta(days=7))
date_to = st.sidebar.date_input("Al", value=today)

if date_from > date_to:
    st.sidebar.error("Intervallo non valido: 'Dal' deve essere <= 'Al'.")
    st.stop()

if (date_to - date_from).days > 40:
    st.sidebar.warning("Range > ~40 giorni: potrebbe essere lento.")

refresh = st.sidebar.button("Aggiorna dati (svuota cache)")
if refresh:
    st.cache_data.clear()


# ======================
# JQL EFFECTIVE (non mostrata in UI)
# ======================
pref_from = date_from - timedelta(days=MARGIN_DAYS)
pref_to = date_to + timedelta(days=MARGIN_DAYS)

jql_effective = (
    f"({default_jql}) "
    f'AND updated >= "{pref_from.isoformat()}" '
    f'AND updated <= "{pref_to.isoformat()}"'
)


# ======================
# CACHES
# ======================
@st.cache_data(ttl=3600, show_spinner=False)
def cached_search_issues(jql: str):
    return search_issues_jql_v3(
        base_url=BASE_URL,
        auth=AUTH,
        jql=jql,
        fields=["summary", "issuetype", "timetracking"]
    )

@st.cache_data(ttl=3600, show_spinner=False)
def cached_issue_worklogs(issue_key: str):
    return get_issue_worklogs_v3(
        base_url=BASE_URL,
        auth=AUTH,
        issue_key=issue_key,
    )


def _rows_from_worklogs(worklogs, issue_key, summary, issue_type, est_hours, date_from: date, date_to: date):
    out = []
    for wl in worklogs:
        author = (wl.get("author") or {}).get("displayName", "") or ""
        started = wl.get("started", "") or ""
        seconds = wl.get("timeSpentSeconds", 0) or 0
        if not started:
            continue

        wl_day = pd.to_datetime(started[:10], errors="coerce")
        if pd.isna(wl_day):
            continue
        wl_day = wl_day.date()

        if wl_day < date_from or wl_day > date_to:
            continue

        out.append(
            {
                "Data": wl_day,
                "Utente": author,
                "IssueType": issue_type,
                "Issue": issue_key,
                "Summary": summary,
                "Stima": est_hours,
                "Ore": round(seconds / 3600, 2)
            }
        )
    return out


def build_dataframe(issues, date_from: date, date_to: date) -> pd.DataFrame:
    rows = []

    issues_info = []
    for issue in issues:
        key = issue.get("key", "")
        fields = issue.get("fields", {}) or {}
        summary = fields.get("summary", "") or ""
        issue_type = (fields.get("issuetype") or {}).get("name", "") or ""
    
        # Stima (Original Estimate) in secondi -> ore
        tt = fields.get("timetracking") or {}
        est_seconds = tt.get("originalEstimateSeconds")
        if est_seconds is None:
            # fallback: alcuni tenant espongono anche "timeoriginalestimate" direttamente
            est_seconds = fields.get("timeoriginalestimate")
        est_hours = round((est_seconds or 0) / 3600, 2)
        
        if key:
            issues_info.append((key, summary, issue_type, est_hours))

    # Fetch parallelo dei worklog (cache attiva)
    if MAX_WORKERS <= 1:
        for key, summary, issue_type in issues_info:
            worklogs = cached_issue_worklogs(key)
            rows.extend(_rows_from_worklogs(worklogs, key, summary, issue_type, date_from, date_to))
    else:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            future_map = {
                ex.submit(cached_issue_worklogs, key): (key, summary, issue_type, est_hours)
                for (key, summary, issue_type, est_hours) in issues_info
            }

            for fut in as_completed(future_map):
                key, summary, issue_type, est_hours = future_map[fut]
                worklogs = fut.result()
                rows.extend(_rows_from_worklogs(worklogs, key, summary, issue_type, est_hours, date_from, date_to))

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["Ore"] = pd.to_numeric(df["Ore"], errors="coerce").fillna(0.0)
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date
    df = df.dropna(subset=["Data"])
    return df


# ======================
# LOAD DATA
# ======================
with st.spinner("Ricerca issue su Jira..."):
    try:
        issues = cached_search_issues(jql_effective)
    except Exception as e:
        st.error("Errore durante la search Jira:")
        st.code(str(e))
        st.stop()

if not issues:
    st.info("Nessuna issue trovata nel periodo selezionato.")
    st.stop()

with st.spinner("Caricamento worklog (cache attiva)..."):
    try:
        df = build_dataframe(issues, date_from, date_to)
    except Exception as e:
        st.error("Errore durante il download dei worklog:")
        st.code(str(e))
        st.stop()

if df.empty:
    st.info("Nessun worklog nel range selezionato.")
    st.stop()


# ======================
# POST-FILTERS (fast)
# ======================
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
    st.info("Nessun dato dopo l’applicazione dei filtri.")
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
# TABLE + DOWNLOAD
# ======================
st.subheader("Dettaglio")

df_show = df_view.copy()
df_show["Data"] = pd.to_datetime(df_show["Data"]).dt.strftime("%d/%m/%Y")
df_show = df_show[["Data", "Utente", "IssueType", "Issue", "Summary", "Stima", "Ore"]]

st.dataframe(df_show, use_container_width=True, hide_index=True)

st.download_button(
    "Download CSV",
    data=df_show.to_csv(index=False).encode("utf-8"),
    file_name=f"worklog_{date_from.isoformat()}_{date_to.isoformat()}.csv",
    mime="text/csv",
)
