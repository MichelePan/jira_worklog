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

from jira_client import search_issues_jql_v3, get_issue_worklogs_v3

# ======================
# STREAMLIT CONFIG
# ======================
st.set_page_config(page_title="Jira Worklog Dashboard", layout="wide")
st.title("Jira Worklog Dashboard")

# ======================
# CONFIG
# ======================
jira_domain = st.secrets["JIRA_DOMAIN"]
email = st.secrets["JIRA_EMAIL"]
api_token = st.secrets["JIRA_API_TOKEN"]
default_jql = st.secrets.get("DEFAULT_JQL", "project = KAN")
EPIC_LINK_FIELD_ID = st.secrets.get("EPIC_LINK_FIELD_ID", None)

BASE_URL = f"https://{jira_domain}/rest/api/3"
AUTH = HTTPBasicAuth(email, api_token)

HEADERS_GET = {"Accept": "application/json"}

MAX_WORKERS = 10
MARGIN_DAYS = 3

TTL_SEARCH = 30 * 60
TTL_WORKLOG = 60 * 60
TTL_EPIC = 6 * 60 * 60

# ======================
# SIDEBAR
# ======================
st.sidebar.header("Filtri")

today = date.today()
date_from = st.sidebar.date_input("Dal", value=today - timedelta(days=7))
date_to = st.sidebar.date_input("Al", value=today)

if date_from > date_to:
    st.sidebar.error("Intervallo non valido")
    st.stop()

refresh = st.sidebar.button("Aggiorna dati")
if refresh:
    st.cache_data.clear()

pref_from = date_from - timedelta(days=MARGIN_DAYS)

jql_effective = (
    f"({default_jql}) "
    f'AND updated >= "{pref_from.isoformat()}"'
)

# ======================
# CACHE
# ======================
@st.cache_data(ttl=TTL_SEARCH)
def cached_search_issues(jql):
    fields = ["summary", "issuetype", "timetracking", "status", "assignee", "parent"]
    if EPIC_LINK_FIELD_ID:
        fields.append(EPIC_LINK_FIELD_ID)

    return search_issues_jql_v3(BASE_URL, AUTH, jql, fields)


@st.cache_data(ttl=TTL_WORKLOG)
def cached_issue_worklogs(issue_key):
    return get_issue_worklogs_v3(BASE_URL, AUTH, issue_key)


@st.cache_data(ttl=TTL_EPIC)
def cached_issue_summary(issue_key):
    url = f"{BASE_URL}/issue/{issue_key}"
    params = {"fields": "summary"}
    r = requests.get(url, params=params, headers=HEADERS_GET, auth=AUTH)
    if not r.ok:
        return ""
    return r.json().get("fields", {}).get("summary", "")


# ======================
# HELPERS
# ======================
def _issue_estimate_hours(fields):
    tt = fields.get("timetracking") or {}
    sec = tt.get("originalEstimateSeconds") or fields.get("timeoriginalestimate") or 0
    return round(sec / 3600, 2)


def build_dataframe(issues, dfrom, dto):
    rows = []

    for issue in issues:
        key = issue["key"]
        f = issue["fields"]

        est = _issue_estimate_hours(f)

        wls = cached_issue_worklogs(key)

        for wl in wls:
            d = pd.to_datetime(wl["started"][:10]).date()
            if d < dfrom or d > dto:
                continue

            rows.append({
                "Data": d,
                "Issue": key,
                "Summary": f.get("summary"),
                "StimaOre": est,
                "Ore": round(wl.get("timeSpentSeconds", 0) / 3600, 2),
            })

    return pd.DataFrame(rows)


# ======================
# LOAD MAIN DATA
# ======================
issues = cached_search_issues(jql_effective)
df = build_dataframe(issues, date_from, date_to)

if df.empty:
    st.stop()

# ======================
# KPI
# ======================
c1, c2 = st.columns(2)
c1.metric("Ore totali", f"{df['Ore'].sum():.2f}")
c2.metric("Issue", df["Issue"].nunique())

st.divider()

# ======================
# DETTAGLIO
# ======================
st.subheader("Dettaglio")

st.dataframe(df)

# ======================
# AGGREGAZIONE PER ISSUE
# ======================
per_issue = (
    df.groupby(["Issue", "Summary", "StimaOre"], as_index=False)
    .agg(Ore=("Ore", "sum"))
)

st.subheader("Ore per issue")
st.dataframe(per_issue)

st.divider()

# =========================================================
# 🚀 NUOVA SEZIONE: EFFICIENZA (FEBBRAIO → OGGI)
# =========================================================

st.subheader("Efficienza progetto (da febbraio ad oggi)")

february_start = date(today.year, 2, 1)

jql_efficiency = (
    f"({default_jql}) "
    f'AND updated >= "{(february_start - timedelta(days=MARGIN_DAYS)).isoformat()}"'
)

issues_eff = cached_search_issues(jql_efficiency)
df_eff = build_dataframe(issues_eff, february_start, today)

if df_eff.empty:
    st.info("Nessun dato per efficienza")
    st.stop()

# aggregazione per issue
eff = (
    df_eff.groupby(["Issue", "Summary", "StimaOre"], as_index=False)
    .agg(OreEffettive=("Ore", "sum"))
)

eff = eff[(eff["OreEffettive"] > 0) & (eff["StimaOre"] > 0)]

# calcolo efficienza
eff["Efficienza"] = eff["StimaOre"] / eff["OreEffettive"]
eff["Efficienza_%"] = (eff["Efficienza"] * 100).round(1)

# efficienza globale (pesata)
tot_stimato = eff["StimaOre"].sum()
tot_eff = eff["OreEffettive"].sum()

eff_glob = tot_stimato / tot_eff if tot_eff > 0 else 0

# KPI
c1, c2, c3 = st.columns(3)
c1.metric("Ore stimate", f"{tot_stimato:.2f}")
c2.metric("Ore effettive", f"{tot_eff:.2f}")
c3.metric("Efficienza globale", f"{eff_glob*100:.1f}%")

# tabella
st.dataframe(
    eff.sort_values("Efficienza", ascending=False),
    use_container_width=True
)

# download
st.download_button(
    "Download CSV efficienza",
    data=eff.to_csv(index=False).encode("utf-8"),
    file_name="efficienza_febbraio_oggi.csv",
    mime="text/csv",
)
