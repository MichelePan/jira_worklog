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
MARGIN_DAYS = 3

# Cache policy
TTL_SEARCH = 30 * 60        # 30 min
TTL_WORKLOG = 12 * 60 * 60  # 12 h


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
    st.sidebar.warning("Range > ~40 giorni: potrebbe essere più lento.")

refresh = st.sidebar.button("Aggiorna dati (svuota cache)")
if refresh:
    st.cache_data.clear()

# Pre-filtro su updated (non mostrato)
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
@st.cache_data(ttl=TTL_SEARCH, show_spinner=False)
def cached_search_issues(jql: str):
    return search_issues_jql_v3(
        base_url=BASE_URL,
        auth=AUTH,
        jql=jql,
        fields=["summary", "issuetype", "timetracking", "status"],
    )

@st.cache_data(ttl=TTL_WORKLOG, show_spinner=False)
def cached_issue_worklogs(issue_key: str):
    return get_issue_worklogs_v3(
        base_url=BASE_URL,
        auth=AUTH,
        issue_key=issue_key,
    )


# ======================
# HELPERS
# ======================
def _issue_estimate_hours(fields: dict) -> float:
    tt = (fields or {}).get("timetracking") or {}
    est_seconds = tt.get("originalEstimateSeconds")
    if est_seconds is None:
        est_seconds = (fields or {}).get("timeoriginalestimate")
    return round((est_seconds or 0) / 3600, 2)

def _issue_status_name(fields: dict) -> str:
    st_obj = (fields or {}).get("status") or {}
    return st_obj.get("name", "") or ""

def _issue_type_name(fields: dict) -> str:
    it = (fields or {}).get("issuetype") or {}
    return it.get("name", "") or ""

def _rows_from_worklogs(
    worklogs,
    issue_key: str,
    summary: str,
    issue_type: str,
    issue_status: str,
    est_hours: float,
    date_from: date,
    date_to: date,
):
    out = []
    for wl in worklogs:
        author_obj = wl.get("author") or {}
        account_id = author_obj.get("accountId", "") or ""
        display_name = author_obj.get("displayName", "") or ""

        started = wl.get("started", "") or ""
        seconds = wl.get("timeSpentSeconds", 0) or 0
        if not started:
            continue

        wl_day_ts = pd.to_datetime(started[:10], errors="coerce")
        if pd.isna(wl_day_ts):
            continue
        wl_day = wl_day_ts.date()

        if wl_day < date_from or wl_day > date_to:
            continue

        out.append(
            {
                "Data": wl_day,
                "Utente": display_name,   # UI label
                "UtenteId": account_id,   # internal key
                "IssueType": issue_type,
                "Issue": issue_key,
                "Summary": summary,
                "StimaOre": est_hours,
                "Ore": round(seconds / 3600, 2),
                "Stato": issue_status,    # <-- richiesto: in tabella lo metteremo in fondo
            }
        )
    return out

def build_dataframe(issues, date_from: date, date_to: date) -> pd.DataFrame:
    rows = []

    issues_info = []
    for issue in issues:
        key = issue.get("key", "")
        fields = issue.get("fields", {}) or {}
        if not key:
            continue

        issues_info.append(
            (
                key,
                fields.get("summary", "") or "",
                _issue_type_name(fields),
                _issue_status_name(fields),
                _issue_estimate_hours(fields),
            )
        )

    if MAX_WORKERS <= 1:
        for key, summary, itype, istatus, est_hours in issues_info:
            wls = cached_issue_worklogs(key)
            rows.extend(_rows_from_worklogs(wls, key, summary, itype, istatus, est_hours, date_from, date_to))
    else:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            future_map = {
                ex.submit(cached_issue_worklogs, key): (key, summary, itype, istatus, est_hours)
                for (key, summary, itype, istatus, est_hours) in issues_info
            }
            for fut in as_completed(future_map):
                key, summary, itype, istatus, est_hours = future_map[fut]
                wls = fut.result()
                rows.extend(_rows_from_worklogs(wls, key, summary, itype, istatus, est_hours, date_from, date_to))

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["Ore"] = pd.to_numeric(df["Ore"], errors="coerce").fillna(0.0)
    df["StimaOre"] = pd.to_numeric(df["StimaOre"], errors="coerce").fillna(0.0)
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.date
    df = df.dropna(subset=["Data"])

    # Alcuni worklog possono non avere accountId (raro), ma evitiamo rogne nei filtri
    df["UtenteId"] = df["UtenteId"].fillna("").astype(str)
    df["Utente"] = df["Utente"].fillna("").astype(str)

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
# Stato
statuses = ["(tutti)"] + sorted([s for s in df["Stato"].dropna().unique().tolist() if str(s).strip()])
status_sel = st.sidebar.selectbox("Stato", statuses)

# IssueType
types = ["(tutti)"] + sorted([t for t in df["IssueType"].dropna().unique().tolist() if str(t).strip()])
type_sel = st.sidebar.selectbox("Issue Type", types)

# Utente (UI pulita)
# name -> list(accountId)
name_to_ids = (
    df.groupby("Utente")["UtenteId"]
    .apply(lambda s: sorted([x for x in s.dropna().unique().tolist() if str(x).strip()]))
    .to_dict()
)

user_option_to_ids = {"(tutti)": None}
for name, ids in sorted(name_to_ids.items(), key=lambda x: x[0]):
    if not str(name).strip():
        continue
    if len(ids) <= 1:
        user_option_to_ids[name] = ids
    else:
        # Non mostriamo accountId: solo un contatore per gestire omonimi
        user_option_to_ids[f"{name} ({len(ids)})"] = ids

user_sel = st.sidebar.selectbox("Utente", list(user_option_to_ids.keys()))

# Applica filtri
df_view = df.copy()

if status_sel != "(tutti)":
    df_view = df_view[df_view["Stato"] == status_sel]

if type_sel != "(tutti)":
    df_view = df_view[df_view["IssueType"] == type_sel]

if user_sel != "(tutti)":
    ids = user_option_to_ids[user_sel] or []
    df_view = df_view[df_view["UtenteId"].isin(ids)]

df_view = df_view.sort_values(["Data", "Utente", "Issue"])

if df_view.empty:
    st.info("Nessun dato dopo l’applicazione dei filtri.")
    st.stop()


# ======================
# KPI (no top 10)
# ======================
c1, c2, c3, c4 = st.columns(4)
c1.metric("Totale ore", f"{df_view['Ore'].sum():.2f}")
c2.metric("N. worklog", f"{len(df_view)}")
c3.metric("N. issue", f"{df_view['Issue'].nunique()}")
c4.metric("N. utenti", f"{df_view['UtenteId'].nunique()}")

st.divider()


# ======================
# VISTA 1: DETTAGLIO
# ======================
st.subheader("Dettaglio worklog")

df_show = df_view.copy()
df_show["Data"] = pd.to_datetime(df_show["Data"]).dt.strftime("%d/%m/%Y")

# Richiesto: Stato in ultima posizione (dopo Ore)
df_show = df_show[["Data", "Utente", "IssueType", "Issue", "Summary", "StimaOre", "Ore", "Stato"]]

st.dataframe(df_show, use_container_width=True, hide_index=True)

st.download_button(
    "Download CSV (dettaglio)",
    data=df_show.to_csv(index=False).encode("utf-8"),
    file_name=f"worklog_dettaglio_{date_from.isoformat()}_{date_to.isoformat()}.csv",
    mime="text/csv",
)

st.divider()


# ======================
# VISTA 2: PIVOT ORE PER GIORNO / UTENTE
# ======================
st.subheader("Pivot: ore per giorno / utente")

pivot = (
    df_view
    .pivot_table(
        index="Data",
        columns="Utente",
        values="Ore",
        aggfunc="sum",
        fill_value=0.0,
    )
    .sort_index()
)

pivot_show = pivot.copy()
pivot_show.index = pd.to_datetime(pivot_show.index).strftime("%d/%m/%Y")

st.dataframe(pivot_show, use_container_width=True)

st.download_button(
    "Download CSV (pivot giorno/utente)",
    data=pivot_show.to_csv(index=True).encode("utf-8"),
    file_name=f"worklog_pivot_giorno_utente_{date_from.isoformat()}_{date_to.isoformat()}.csv",
    mime="text/csv",
)

st.divider()


# ======================
# VISTA 3: ORE PER ISSUE (aggregata)
# ======================
st.subheader("Riepilogo: ore per issue")

per_issue = (
    df_view
    .groupby(["Issue", "Summary", "IssueType", "StimaOre", "Stato"], as_index=False)
    .agg(Ore=("Ore", "sum"))
    .sort_values(["Ore", "Issue"], ascending=[False, True])
)

# Anche qui possiamo mettere Stato in ultima posizione (come preferisci)
per_issue = per_issue[["Issue", "Summary", "IssueType", "StimaOre", "Ore", "Stato"]]

st.dataframe(per_issue, use_container_width=True, hide_index=True)

st.download_button(
    "Download CSV (ore per issue)",
    data=per_issue.to_csv(index=False).encode("utf-8"),
    file_name=f"worklog_ore_per_issue_{date_from.isoformat()}_{date_to.isoformat()}.csv",
    mime="text/csv",
)
