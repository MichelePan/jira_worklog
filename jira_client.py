import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime, date
from typing import Dict, List, Optional

HEADERS_JSON = {"Accept": "application/json", "Content-Type": "application/json"}
HEADERS_GET = {"Accept": "application/json"}


def search_issues_jql_v3(
    base_url: str,
    auth: HTTPBasicAuth,
    jql: str,
    fields: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Jira Cloud REST v3: POST /rest/api/3/search/jql con nextPageToken.
    """
    if fields is None:
        fields = ["summary", "issuetype"]

    url = f"{base_url}/search/jql"
    issues: List[Dict] = []
    next_page_token = None

    while True:
        payload = {"jql": jql, "fields": fields}
        if next_page_token:
            payload["nextPageToken"] = next_page_token

        resp = requests.post(url, json=payload, headers=HEADERS_JSON, auth=auth, timeout=60)

        if not resp.ok:
            # Errore leggibile (utile su Streamlit Cloud logs/UI)
            try:
                details = resp.json()
            except Exception:
                details = resp.text
            raise RuntimeError(
                f"Jira API error on /search/jql | status={resp.status_code} | details={details}"
            )

        data = resp.json()
        batch = data.get("issues", []) or []
        issues.extend(batch)

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    return issues


def get_issue_worklogs_v3(base_url: str, auth: HTTPBasicAuth, issue_key: str) -> List[Dict]:
    """
    Jira: GET /rest/api/3/issue/{issueKey}/worklog
    Nota: se ci sono tanti worklog, può essere necessario paginare.
    Qui gestiamo paginazione startAt/maxResults.
    """
    url = f"{base_url}/issue/{issue_key}/worklog"
    start_at = 0
    max_results = 100
    out: List[Dict] = []

    while True:
        params = {"startAt": start_at, "maxResults": max_results}
        resp = requests.get(url, params=params, headers=HEADERS_GET, auth=auth, timeout=60)

        if not resp.ok:
            try:
                details = resp.json()
            except Exception:
                details = resp.text
            raise RuntimeError(
                f"Jira API error on /issue/{issue_key}/worklog | status={resp.status_code} | details={details}"
            )

        data = resp.json()
        wls = data.get("worklogs", []) or []
        out.extend(wls)

        start_at += len(wls)
        total = data.get("total", 0) or 0
        if not wls or start_at >= total:
            break

    return out


def fetch_worklogs_by_jql(
    jira_domain: str,
    email: str,
    api_token: str,
    jql: str,
) -> List[Dict]:
    """
    Ritorna righe worklog con:
    Data, Utente, IssueType, Issue, Summary, Ore
    """
    base_url = f"https://{jira_domain}/rest/api/3"
    auth = HTTPBasicAuth(email, api_token)

    issues = search_issues_jql_v3(base_url, auth, jql, fields=["summary", "issuetype"])

    rows: List[Dict] = []

    for issue in issues:
        issue_key = issue.get("key", "")
        fields = issue.get("fields", {}) or {}
        summary = fields.get("summary", "") or ""
        issue_type = (fields.get("issuetype") or {}).get("name", "") or ""

        if not issue_key:
            continue

        worklogs = get_issue_worklogs_v3(base_url, auth, issue_key)

        for wl in worklogs:
            author = (wl.get("author") or {}).get("displayName", "") or ""
            started = wl.get("started", "") or ""
            seconds = wl.get("timeSpentSeconds", 0) or 0

            wl_date = datetime.strptime(started[:10], "%Y-%m-%d").date() if started else None
            hours = round(seconds / 3600, 2)

            rows.append(
                {
                    "Data": wl_date,  # date vera (poi la formattiamo in UI)
                    "Utente": author,
                    "IssueType": issue_type,
                    "Issue": issue_key,
                    "Summary": summary,
                    "Ore": hours,
                }
            )

    return rows
