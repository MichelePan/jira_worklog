import streamlit as st
import pandas as pd
from datetime import date, timedelta
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ======================
# AUTH
# ======================

def check_auth():
    if "authenticated" not in st.session_state:
        st.session_state["authenticated"] = False

    if not st.session_state["authenticated"]:
        st.title("Login")

        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if st.button("Login"):
            if (
                username == st.secrets["auth"]["username"]
                and password == st.secrets["auth"]["password"]
            ):
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Credenziali non valide")

        st.stop()

check_auth()
# ======================
# END AUTH
# ======================

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

# Se nel tuo Jira (company-managed) l'epica è in un custom field "Epic Link",
# metti l'id qui (es: "customfield_10014") in secrets:
# EPIC_LINK_FIELD_ID: customfield_10014
EPIC_LINK_FIELD_ID = st.secrets.get("EPIC_LINK_FIELD_ID", None)

BASE_URL = f"https://{jira_domain}/rest/api/3"
AUTH = HTTPBasicAuth(email, api_token)

HEADERS_GET = {"Accept": "application/json"}

# Performance knobs (non in sidebar)
MAX_WORKERS = 10
MARGIN_DAYS = 3

# Cache policy
TTL_SEARCH = 30 * 60  # 30 min
TTL_WORKLOG = 1 * 60 * 60  # 1 h
TTL_EPIC = 6 * 60 * 60  # 6 h (nomi epiche cambiano raramente)

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
    f'AND updated >= "{pref_from.isoformat()}"'
)

# ======================
# CACHES
# ======================
@st.cache_data(ttl=TTL_SEARCH, show_spinner=False)
def cached_search_issues(jql: str):
    fields = ["summary", "issuetype", "timetracking", "status", "assignee", "parent"]
    if EPIC_LINK_FIELD_ID:
        fields.append(EPIC_LINK_FIELD_ID)

    return search_issues_jql_v3(
        base_url=BASE_URL,
        auth=AUTH,
        jql=jql,
        fields=fields,
    )

@st.cache_data(ttl=TTL_WORKLOG, show_spinner=False)
def cached_issue_worklogs(issue_key: str):
    return get_issue_worklogs_v3(
        base_url=BASE_URL,
        auth=AUTH,
        issue_key=issue_key,
    )

@st.cache_data(ttl=TTL_EPIC, show_spinner=False)
def cached_issue_summary(issue_key: str) -> str:
    """
    Recupera la summary di una issue (qui usata per risolvere EpicName).
    Usa Jira REST v3: GET /issue/{issueKey}?fields=summary
    """
    url = f"{BASE_URL}/issue/{issue_key}"
    params = {"fields": "summary"}
    resp = requests.get(url, params=params, headers=HEADERS_GET, auth=AUTH, timeout=60)

    if not resp.ok:
        # Non blocchiamo la dashboard se un'epic non è accessibile:
        # ritorniamo stringa vuota (ma logghiamo in UI in modo soft).
        return ""

    data = resp.json() or {}
    fields = data.get("fields", {}) or {}
    return fields.get("summary", "") or ""

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

def _issue_owner_name(fields: dict) -> str:
    # "Owner" interpretato come Assignee
    a = (fields or {}).get("assignee") or {}
    return a.get("displayName", "") or ""

def _issue_parent_key(fields: dict) -> str:
    parent = (fields or {}).get("parent") or {}
    return parent.get("key", "") or ""

def _issue_epic_key(fields: dict) -> str:
    """
    Strategia:
    1) Se EPIC_LINK_FIELD_ID (company-managed) è configurato, prova a leggere fields[customfield].
       Spesso è direttamente la key dell'epic (stringa).
    2) Fallback su parent.key (team-managed: Story/Task -> Epic).
       Nota: per sub-task, parent.key è tipicamente Story/Task (non epic).
    """
    if EPIC_LINK_FIELD_ID:
        v = (fields or {}).get(EPIC_LINK_FIELD_ID)
        if isinstance(v, str) and v.strip():
            return v.strip()

    return _issue_parent_key(fields)

def _rows_from_worklogs(
    worklogs,
    issue_key: str,
    summary: str,
    owner: str,
    issue_type: str,
    issue_status: str,
    est_hours: float,
    epic_key: str,
    epic_name: str,
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
                "Utente": display_name,  # UI label
                "UtenteId": account_id,  # internal key
                "IssueType": issue_type,
                "Issue": issue_key,
                "Summary": summary,
                "Owner": owner,  # Assignee
                "EpicKey": epic_key,
                "EpicName": epic_name,
                "StimaOre": est_hours,
                "Ore": round(seconds / 3600, 2),
                "Stato": issue_status,
            }
        )
    return out

def _resolve_epic_names(epic_keys):
    """
    Dato un set/list di EpicKey, ritorna dict EpicKey -> EpicName (summary).
    Esegue fetch parallelo con cache per chiave.
    """
    epic_keys = [k for k in sorted(set(epic_keys)) if str(k).strip()]
    if not epic_keys:
        return {}

    out = {}

    # Limita parallelismo: reuse MAX_WORKERS ma non più di 10 per non stressare
    workers = min(max(MAX_WORKERS, 1), 10)

    if workers <= 1:
        for k in epic_keys:
            out[k] = cached_issue_summary(k)
        return out

    with ThreadPoolExecutor(max_workers=workers) as ex:
        fut_map = {ex.submit(cached_issue_summary, k): k for k in epic_keys}
        for fut in as_completed(fut_map):
            k = fut_map[fut]
            try:
                out[k] = fut.result() or ""
            except Exception:
                out[k] = ""
    return out

def build_dataframe(issues, date_from: date, date_to: date) -> pd.DataFrame:
    rows = []

    issues_info = []
    epic_keys_seen = []

    for issue in issues:
        key = issue.get("key", "")
        fields = issue.get("fields", {}) or {}
        if not key:
            continue

        epic_key = _issue_epic_key(fields)
        epic_keys_seen.append(epic_key)

        issues_info.append(
            (
                key,
                fields.get("summary", "") or "",
                _issue_owner_name(fields),
                _issue_type_name(fields),
                _issue_status_name(fields),
                _issue_estimate_hours(fields),
                epic_key,
            )
        )

    # Risolvi EpicName (summary) con chiamata dedicata per le EpicKey
    epic_map = _resolve_epic_names(epic_keys_seen)

    if MAX_WORKERS <= 1:
        for key, summary, owner, itype, istatus, est_hours, epic_key in issues_info:
            wls = cached_issue_worklogs(key)
            rows.extend(
                _rows_from_worklogs(
                    wls,
                    key,
                    summary,
                    owner,
                    itype,
                    istatus,
                    est_hours,
                    epic_key,
                    epic_map.get(epic_key, "") or "",
                    date_from,
                    date_to,
                )
            )
    else:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            future_map = {
                ex.submit(cached_issue_worklogs, key): (key, summary, owner, itype, istatus, est_hours, epic_key)
                for (key, summary, owner, itype, istatus, est_hours, epic_key) in issues_info
            }
            for fut in as_completed(future_map):
                key, summary, owner, itype, istatus, est_hours, epic_key = future_map[fut]
                wls = fut.result()
                rows.extend(
                    _rows_from_worklogs(
                        wls,
                        key,
                        summary,
                        owner,
                        itype,
                        istatus,
                        est_hours,
                        epic_key,
                        epic_map.get(epic_key, "") or "",
                        date_from,
                        date_to,
                    )
                )

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

    # Owner potrebbe essere vuoto se l'issue non ha assignee
    df["Owner"] = df["Owner"].fillna("").astype(str)

    # Epic fields
    df["EpicKey"] = df["EpicKey"].fillna("").astype(str)
    df["EpicName"] = df["EpicName"].fillna("").astype(str)

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

# Epic filter (name se disponibile, altrimenti key)
if df["EpicName"].astype(str).str.strip().any():
    epics = ["(tutte)"] + sorted([e for e in df["EpicName"].dropna().unique().tolist() if str(e).strip()])
    epic_sel = st.sidebar.selectbox("Epic", epics)
    epic_mode = "name"
else:
    epics = ["(tutte)"] + sorted([e for e in df["EpicKey"].dropna().unique().tolist() if str(e).strip()])
    epic_sel = st.sidebar.selectbox("Epic (key)", epics)
    epic_mode = "key"

# Utente (UI pulita)
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
        user_option_to_ids[f"{name} ({len(ids)})"] = ids

user_sel = st.sidebar.selectbox("Utente", list(user_option_to_ids.keys()))

# Applica filtri
df_view = df.copy()

if status_sel != "(tutti)":
    df_view = df_view[df_view["Stato"] == status_sel]

if type_sel != "(tutti)":
    df_view = df_view[df_view["IssueType"] == type_sel]

if epic_sel != "(tutte)":
    if epic_mode == "name":
        df_view = df_view[df_view["EpicName"] == epic_sel]
    else:
        df_view = df_view[df_view["EpicKey"] == epic_sel]

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

df_show = df_show[
    ["Data", "Utente", "IssueType", "Issue", "Summary", "EpicKey", "EpicName", "StimaOre", "Ore", "Stato"]
]

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
    df_view.pivot_table(
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
# VISTA 3: ORE PER ISSUE (aggregata) + EPIC
# ======================
st.subheader("Riepilogo: ore per issue (con epica)")

per_issue = (
    df_view.groupby(
        ["EpicKey", "EpicName", "Issue", "Summary", "Owner", "IssueType", "StimaOre", "Stato"],
        as_index=False,
    )
    .agg(Ore=("Ore", "sum"))
    .sort_values(["Ore", "Issue"], ascending=[False, True])
)

per_issue = per_issue[
    ["EpicKey", "EpicName", "Issue", "Summary", "Owner", "IssueType", "StimaOre", "Ore", "Stato"]
]

st.dataframe(per_issue, use_container_width=True, hide_index=True)

st.download_button(
    "Download CSV (ore per issue con epica)",
    data=per_issue.to_csv(index=False).encode("utf-8"),
    file_name=f"worklog_ore_per_issue_con_epica_{date_from.isoformat()}_{date_to.isoformat()}.csv",
    mime="text/csv",
)
