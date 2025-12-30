import io
import os
import re
import tempfile
import traceback
import time
from typing import Optional, Tuple, List, Any
from urllib.parse import urlparse, parse_qs

import requests
import streamlit as st
import pandas as pd
from openai import OpenAI

# --- Notebook execution deps (install via requirements.txt) ---
try:
    import nbformat
    from nbconvert.preprocessors import ExecutePreprocessor
except Exception:
    nbformat = None
    ExecutePreprocessor = None


# =============================================================================
# ✅ OPENAI + STREAMLIT (CHAT)
# =============================================================================

def get_openai_key() -> Optional[str]:
    key = None
    try:
        key = st.secrets.get("OPENAI_API_KEY", None)
    except Exception:
        key = None
    if not key:
        key = os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_SECRET_KEY")
    if not key:
        key = st.session_state.get("_openai_key")
    return key


def make_client() -> Optional[OpenAI]:
    key = get_openai_key()
    if not key:
        return None
    return OpenAI(api_key=key)


def llm_answer(client: OpenAI, model: str, system: str, user: str) -> str:
    try:
        resp = client.responses.create(
            model=model,
            input=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        out = getattr(resp, "output_text", None)
        if out:
            return out.strip()
        return str(resp)
    except Exception:
        comp = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=0.2,
        )
        return (comp.choices[0].message.content or "").strip()


# =============================================================================
# ✅ HELPERS (NUMERIC PARSING / % CALC)
# =============================================================================

_NUM_RE = re.compile(r"[-+]?\d{1,3}(?:,\d{3})*(?:\.\d+)?|[-+]?\d+(?:\.\d+)?")


def _to_float_maybe(x: Any) -> Optional[float]:
    """
    Extract first number from cell value. Returns float or None.
    Handles:  "1,234", "12.5", "123 (45%)", etc.
    """
    if x is None:
        return None
    if isinstance(x, (int, float)) and pd.notna(x):
        try:
            return float(x)
        except Exception:
            return None
    s = str(x).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    m = _NUM_RE.search(s)
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except Exception:
        return None


def _has_percent_sign(x: Any) -> bool:
    if x is None:
        return False
    return "%" in str(x)


def _safe_pct(numer: Optional[float], denom: Optional[float]) -> Optional[float]:
    if numer is None or denom is None:
        return None
    if denom == 0:
        return None
    return (numer / denom) * 100.0


def _pct_str(v: Optional[float], decimals: int = 2) -> str:
    if v is None:
        return ""
    return f"{v:.{decimals}f}%"


def _percent_intent(q: str) -> bool:
    qq = (q or "").lower()
    return ("%" in qq) or ("percent" in qq) or ("percentage" in qq) or ("share" in qq) or ("ratio" in qq)


# =============================================================================
# ✅ LOADERS
# =============================================================================

@st.cache_data(show_spinner=False)
def load_summary_df(xlsx_bytes: bytes, sheet: str = "summary") -> pd.DataFrame:
    bio = io.BytesIO(xlsx_bytes)
    df = pd.read_excel(bio, sheet_name=sheet, engine="openpyxl")
    if df.columns.size > 0 and str(df.columns[0]).lower().startswith("unnamed"):
        df = df.rename(columns={df.columns[0]: "Metric"})
    if "Metric" not in df.columns:
        df = df.rename(columns={df.columns[0]: "Metric"})
    df["Metric"] = df["Metric"].astype(str).str.strip()
    return df


@st.cache_data(show_spinner=False)
def load_raw_df(xlsx_bytes: bytes, sheet: str = "Query result") -> pd.DataFrame:
    try:
        bio = io.BytesIO(xlsx_bytes)
        df = pd.read_excel(bio, sheet_name=sheet, engine="openpyxl")
        return df
    except Exception:
        return pd.DataFrame()


# =============================================================================
# ✅ QUESTION PARSING (WEEK / ZONE)
# =============================================================================

def _normalize_week_label(week_num: int) -> str:
    return f"W-{int(week_num)}"


def extract_week(question: str) -> Optional[str]:
    q = (question or "").strip()

    m = re.search(r"\bW\s*[-_ ]\s*(\d{1,2})\b", q, flags=re.IGNORECASE)
    if m:
        return _normalize_week_label(int(m.group(1)))

    m = re.search(r"\bweek\s*[-_ ]*\s*(\d{1,2})\b", q, flags=re.IGNORECASE)
    if m:
        return _normalize_week_label(int(m.group(1)))

    m = re.search(r"\bweek(\d{1,2})\b", q, flags=re.IGNORECASE)
    if m:
        return _normalize_week_label(int(m.group(1)))

    return None


def zones_from_summary(summary_df: pd.DataFrame) -> List[str]:
    zones = []
    for v in summary_df["Metric"].astype(str).tolist():
        m = re.match(r"^Picked Vol\. Zone (.*) %$", v.strip())
        if m:
            zones.append(m.group(1).strip())
    return sorted(set(zones))


def extract_zone(question: str, zones: List[str]) -> Optional[str]:
    q = (question or "").upper()
    zones_sorted = sorted(zones, key=lambda z: len(z), reverse=True)
    for z in zones_sorted:
        if z.upper() in q:
            return z
    return None


# =============================================================================
# ✅ METRIC SELECTION
# =============================================================================

def select_relevant_rows(question: str, summary_df: pd.DataFrame) -> List[str]:
    q = (question or "").lower()
    metrics = summary_df["Metric"].astype(str).tolist()

    hits = []

    if "picked" in q and "volume" in q and "Picked Volume" in metrics:
        hits.append("Picked Volume")

    if ("on time" in q or "ontime" in q) and "arrival" in q:
        for m in ["AIR On Time Arrival", "AIR EXPRESS On Time Arrival", "EXPRESS On Time Arrival", "SURFACE On Time Arrival"]:
            if m in metrics:
                hits.append(m)

    if ("on time" in q or "ontime" in q) and "delivery" in q:
        for m in ["AIR On Time Delivery", "AIR EXPRESS On Time Delivery", "EXPRESS On Time Delivery", "SURFACE On Time Delivery"]:
            if m in metrics:
                hits.append(m)

    if "cn" in q and ("status" in q or "current" in q) and "CN Status Breakdown" in metrics:
        hits.append("CN Status Breakdown")

    if "ndr" in q and "NDR not available" in metrics:
        hits.append("NDR not available")

    if ("business" in q or "retail" in q or "scm" in q) and "BUSINESS TYPE BREAKDOWN" in metrics:
        hits.append("BUSINESS TYPE BREAKDOWN")

    if "tptr" in q or "mode" in q:
        for m in metrics:
            if str(m).startswith("TPTR Mode"):
                hits.append(m)

    if not hits:
        tokens = [t for t in re.findall(r"[a-zA-Z0-9%]+", q) if len(t) > 2]
        stop = {
            "the", "and", "with", "from", "this", "that",
            "mein", "me", "ka", "ki", "ke",
            "for", "show", "dikhao", "bata", "batao", "please",
            "week", "zone"
        }
        tokens = [t for t in tokens if t not in stop]
        for m in metrics:
            mm = str(m).lower()
            if any(t in mm for t in tokens):
                hits.append(m)

    seen, out = set(), []
    for h in hits:
        if h not in seen and h in metrics:
            seen.add(h)
            out.append(h)
    return out


# =============================================================================
# ✅ ZONE BLOCK LOGIC
# =============================================================================

def _zone_header_text(zone: str) -> str:
    return f"Picked Vol. Zone {zone} %"


def _find_zone_block(summary_df: pd.DataFrame, zone: Optional[str]) -> Optional[Tuple[int, int]]:
    if not zone:
        return None

    target = _zone_header_text(zone).strip().upper()
    metrics_upper = summary_df["Metric"].astype(str).str.strip().str.upper()

    starts = summary_df.index[metrics_upper == target].tolist()
    if not starts:
        return None

    start_idx = starts[0]

    zone_header_idx = summary_df.index[
        summary_df["Metric"].astype(str).str.strip().str.match(r"^Picked Vol\. Zone .* %$")
    ].tolist()
    zone_header_idx = sorted(zone_header_idx)

    end_idx = len(summary_df)
    for idx in zone_header_idx:
        if idx > start_idx:
            end_idx = idx
            break

    return start_idx, end_idx


def _pick_metric_row_index(summary_df: pd.DataFrame, metric_name: str, zone_block: Optional[Tuple[int, int]]) -> Optional[int]:
    metric_name = (metric_name or "").strip()
    if not metric_name:
        return None

    if zone_block:
        s, e = zone_block
        sub = summary_df.iloc[s + 1: e]
        hits = sub.index[sub["Metric"].astype(str).str.strip() == metric_name].tolist()
        if hits:
            return hits[0]

    hits_all = summary_df.index[summary_df["Metric"].astype(str).str.strip() == metric_name].tolist()
    if hits_all:
        return hits_all[0]
    return None


def _expand_header_block(summary_df: pd.DataFrame, header_idx: int, zone_block: Optional[Tuple[int, int]]) -> List[int]:
    if header_idx is None:
        return []

    start_limit = header_idx + 1
    end_limit = len(summary_df)
    if zone_block:
        _, end_limit = zone_block

    collected = []
    for i in range(start_limit, end_limit):
        mname = str(summary_df.loc[i, "Metric"]).strip()

        if mname.startswith("Picked Vol. Zone"):
            break
        if mname in ("BUSINESS TYPE BREAKDOWN", "CN Status Breakdown"):
            break

        collected.append(i)
        if len(collected) >= 60:
            break

    return collected


# =============================================================================
# ✅ VALUE GETTERS + DERIVED CALCS
# =============================================================================

def _get_value_by_metric_zone_week(summary_df: pd.DataFrame, week: str, zone: str, metric: str) -> Any:
    zone_block = _find_zone_block(summary_df, zone)
    idx = _pick_metric_row_index(summary_df, metric, zone_block)
    if idx is None:
        return None
    if week not in summary_df.columns:
        return None
    return summary_df.loc[idx, week]


def _get_zone_header_percent(summary_df: pd.DataFrame, week: str, zone: str) -> Any:
    metric = _zone_header_text(zone)
    metrics_upper = summary_df["Metric"].astype(str).str.strip().str.upper()
    hits = summary_df.index[metrics_upper == metric.strip().upper()].tolist()
    if not hits or week not in summary_df.columns:
        return None
    return summary_df.loc[hits[0], week]


def compute_derived_lines(summary_df: pd.DataFrame, question: str, week: Optional[str], zone: Optional[str], items: List[Tuple[str, str]]) -> List[str]:
    if not summary_df.empty and week and zone and week in summary_df.columns:
        ql = (question or "").lower()
        wants_pct = _percent_intent(question)
        derived: List[str] = []

        if wants_pct and ("picked" in ql and "volume" in ql) and (zone.upper() != "ALL INDIA"):
            zh = _get_zone_header_percent(summary_df, week, zone)
            if zh is not None and str(zh).strip() != "" and pd.notna(zh):
                derived.append(f"Picked Volume % (from summary zone header): {str(zh)}")

        if wants_pct and (("cn status" in ql) or ("cn" in ql and "status" in ql) or ("business" in ql) or ("retail" in ql) or ("scm" in ql)):
            header_names = {"CN Status Breakdown", "BUSINESS TYPE BREAKDOWN"}
            present_headers = [m for (m, _) in items if m in header_names]
            for hdr in present_headers:
                collecting = False
                rows: List[Tuple[str, Any]] = []
                for m, v in items:
                    if m == hdr:
                        collecting = True
                        continue
                    if collecting and m in header_names:
                        break
                    if collecting:
                        rows.append((m, v))

                if rows:
                    if any(_has_percent_sign(v) for _, v in rows):
                        continue

                    nums = [(_to_float_maybe(v), name) for name, v in rows]
                    if all(n is not None for n, _ in nums):
                        total = sum(n for n, _ in nums if n is not None)
                        if total > 0:
                            derived.append(f"{hdr} % share (computed within breakdown):")
                            for n, name in nums:
                                pct = _safe_pct(n, total)
                                if pct is not None:
                                    derived.append(f"• {name}: {_pct_str(pct)}  [= {n:g} / {total:g} × 100]")

        return derived

    return []


# =============================================================================
# ✅ ANSWER BUILDER
# =============================================================================

def answer_from_summary(summary_df: pd.DataFrame, question: str) -> Tuple[Optional[str], Optional[str], List[Tuple[str, str]]]:
    if summary_df.empty:
        return None, None, []

    zones = zones_from_summary(summary_df)
    week = extract_week(question)

    week_cols = [c for c in summary_df.columns if isinstance(c, str) and re.match(r"^W-\d{1,2}$", c.strip())]
    if not week and week_cols:
        week = sorted(week_cols, key=lambda x: int(x.split("-")[1]))[-1]

    zone = extract_zone(question, zones)
    if not zone:
        zone = "ALL INDIA" if "ALL INDIA" in zones else (zones[0] if zones else None)

    if not week or week not in summary_df.columns:
        return week, zone, []

    zone_block = _find_zone_block(summary_df, zone)
    rows = select_relevant_rows(question, summary_df)

    ql = (question or "").lower()
    if _percent_intent(question) and ("picked" in ql and "volume" in ql) and zone:
        zh = _zone_header_text(zone)
        if zh in summary_df["Metric"].astype(str).tolist():
            rows = [zh] + rows
        if "Picked Volume" in summary_df["Metric"].astype(str).tolist():
            if "Picked Volume" not in rows:
                rows.append("Picked Volume")

    result: List[Tuple[str, str]] = []

    for r in rows:
        if zone and r == _zone_header_text(zone):
            metrics_upper = summary_df["Metric"].astype(str).str.strip().str.upper()
            hits = summary_df.index[metrics_upper == r.strip().upper()].tolist()
            if hits:
                val = summary_df.loc[hits[0], week]
                result.append((str(r), "" if pd.isna(val) else str(val)))
            continue

        idx = _pick_metric_row_index(summary_df, r, zone_block)
        if idx is None:
            continue

        val = summary_df.loc[idx, week]
        result.append((str(r), "" if pd.isna(val) else str(val)))

        if r in ("BUSINESS TYPE BREAKDOWN", "CN Status Breakdown"):
            extra_indices = _expand_header_block(summary_df, idx, zone_block)
            for j in extra_indices:
                mname = str(summary_df.loc[j, "Metric"]).strip()
                if mname.startswith("Picked Vol. Zone"):
                    break
                v = summary_df.loc[j, week]
                result.append((mname, "" if pd.isna(v) else str(v)))

    return week, zone, result


# =============================================================================
# ✅ LINK → XLSX BYTES FETCHING (ROBUST)
# =============================================================================

def _is_xlsx_zip(content: bytes) -> bool:
    return bool(content) and len(content) >= 4 and content.startswith(b"PK")


def _extract_id_resourcekey(url: str) -> Tuple[Optional[str], Optional[str]]:
    u = (url or "").strip()
    if not u:
        return None, None

    rk = None
    try:
        qs = parse_qs(urlparse(u).query)
        rk = (qs.get("resourcekey", [None]) or [None])[0]
    except Exception:
        rk = None

    m = re.search(r"docs\.google\.com\/spreadsheets\/d\/([a-zA-Z0-9_-]+)", u)
    if m:
        return m.group(1), rk

    m = re.search(r"drive\.google\.com\/file\/d\/([a-zA-Z0-9_-]+)", u)
    if m:
        return m.group(1), rk

    m = re.search(r"drive\.usercontent\.google\.com\/download\?id=([a-zA-Z0-9_-]+)", u)
    if m:
        return m.group(1), rk

    if "id=" in u:
        try:
            qs = parse_qs(urlparse(u).query)
            fid = (qs.get("id", [None]) or [None])[0]
            return fid, rk
        except Exception:
            pass

    return None, rk


def _build_google_candidates(raw_url: str) -> List[Tuple[str, str, dict]]:
    raw = (raw_url or "").strip()
    fid, rk = _extract_id_resourcekey(raw)
    candidates: List[Tuple[str, str, dict]] = []

    if "docs.google.com/spreadsheets" in raw and fid:
        candidates.append(("generic", f"https://docs.google.com/spreadsheets/d/{fid}/export", {"format": "xlsx"}))
        candidates.append(("generic", f"https://docs.google.com/spreadsheets/d/{fid}/export", {"format": "xlsx", "exportFormat": "xlsx"}))

        params_uc = {"id": fid, "export": "download"}
        if rk:
            params_uc["resourcekey"] = rk
        candidates.append(("usercontent", "https://drive.usercontent.google.com/download", params_uc))
        candidates.append(("drive_uc", "https://drive.google.com/uc", {"id": fid, "export": "download"}))

    if fid:
        params_uc = {"id": fid, "export": "download"}
        if rk:
            params_uc["resourcekey"] = rk
        candidates.append(("usercontent", "https://drive.usercontent.google.com/download", params_uc))
        candidates.append(("drive_uc", "https://drive.google.com/uc", {"id": fid, "export": "download"}))

    if "dropbox.com" in raw:
        if "dl=0" in raw:
            raw = raw.replace("dl=0", "dl=1")
        elif "dl=1" not in raw:
            sep = "&" if "?" in raw else "?"
            raw = raw + f"{sep}dl=1"

    candidates.append(("generic_raw", raw, {}))
    return candidates


def _download_with_google_confirm(session: requests.Session, url: str, params: dict, timeout_sec: int) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

    r = session.get(url, params=params, headers=headers, timeout=timeout_sec, allow_redirects=True)
    if r.status_code != 200:
        raise Exception(f"Download failed (HTTP {r.status_code}).")

    if _is_xlsx_zip(r.content):
        return r.content

    ctype = (r.headers.get("Content-Type") or "").lower()
    is_html = "text/html" in ctype or (r.content[:20].lstrip().lower().startswith(b"<!doctype html") or b"<html" in r.content[:200].lower())
    if not is_html:
        return r.content or b""

    html = r.text or ""

    confirm = None
    for k, v in r.cookies.items():
        if k.startswith("download_warning"):
            confirm = v
            break

    if not confirm:
        m = re.search(r"confirm=([0-9A-Za-z_]+)", html)
        if m:
            confirm = m.group(1)

    uuid = None
    m2 = re.search(r"uuid=([0-9A-Za-z_-]+)", html)
    if m2:
        uuid = m2.group(1)

    if not confirm and ("can't scan this file for viruses" in html.lower() or "can&#39;t scan this file for viruses" in html.lower()):
        confirm = "t"

    if not confirm:
        return r.content or b""

    params2 = dict(params)
    params2["confirm"] = confirm
    if uuid and "uuid" not in params2:
        params2["uuid"] = uuid

    r2 = session.get(url, params=params2, headers=headers, timeout=timeout_sec, allow_redirects=True)
    if r2.status_code != 200:
        raise Exception(f"Download failed after confirmation (HTTP {r2.status_code}).")

    chunks = []
    for chunk in r2.iter_content(chunk_size=1024 * 1024):
        if chunk:
            chunks.append(chunk)
    return b"".join(chunks)


@st.cache_data(show_spinner=False)
def fetch_xlsx_bytes_from_link(url: str, timeout_sec: int = 180) -> bytes:
    raw = (url or "").strip()
    if not raw:
        raise Exception("Empty link.")

    session = requests.Session()
    candidates = _build_google_candidates(raw)

    last_preview = ""
    last_status = None

    for kind, base_url, params in candidates:
        try:
            if kind in ("usercontent", "drive_uc"):
                data = _download_with_google_confirm(session, base_url, params=params, timeout_sec=timeout_sec)
            elif kind == "generic":
                headers = {
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream,*/*",
                }
                r = session.get(base_url, params=params, headers=headers, timeout=timeout_sec, allow_redirects=True)
                last_status = r.status_code
                data = r.content or b""
            else:
                headers = {
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/octet-stream,*/*",
                }
                r = session.get(base_url, headers=headers, timeout=timeout_sec, allow_redirects=True)
                last_status = r.status_code
                data = r.content or b""

            if _is_xlsx_zip(data):
                return data

            last_preview = (data[:500].decode("utf-8", errors="ignore").strip().lower() if data else "")

        except Exception as e:
            last_preview = str(e).lower()

    if "accounts.google" in last_preview or "<html" in last_preview or "login" in last_preview:
        raise Exception(
            "Google returned an HTML page instead of the Excel file. "
            "Use a Drive 'file/d/<id>' link, or download manually and Upload."
        )

    if last_status:
        raise Exception(f"Download failed (HTTP {last_status}). The link may be private or not a direct download.")
    raise Exception("Could not download the file. Please try a Drive file link or Upload.")


# =============================================================================
# ✅ NOTEBOOK TRIGGER: week_optimized.ipynb → processed xlsx (FIXED)
# =============================================================================

def _pick_output_xlsx(work_dir: str) -> Optional[str]:
    priority = ["EDD_Summary_Report.xlsx", "EDD Report.xlsx"]

    for name in priority:
        p = os.path.join(work_dir, name)
        if os.path.exists(p) and os.path.getsize(p) > 0:
            return p

    xlsx_files = []
    for fn in os.listdir(work_dir):
        if fn.lower().endswith(".xlsx"):
            fp = os.path.join(work_dir, fn)
            if os.path.isfile(fp) and os.path.getsize(fp) > 0:
                xlsx_files.append(fp)

    if not xlsx_files:
        return None

    xlsx_files.sort(key=lambda p: os.path.getmtime(p))
    return xlsx_files[-1]


def run_week_optimized_notebook(
    raw_xlsx_bytes: bytes,
    notebook_path: str = "week_optimized.ipynb",
    timeout_sec: int = 1800,
    log_cb=None,
) -> bytes:
    """
    ✅ FIXED FOR ALL nbconvert VERSIONS:
    - Live logging via subclass
    - Handles both preprocess_cell signatures (with/without store_history)
    """
    if nbformat is None or ExecutePreprocessor is None:
        raise Exception("Notebook execution libraries missing. Please install: nbformat, nbconvert, ipykernel, jupyter_client")

    if not os.path.exists(notebook_path):
        raise Exception(f"Notebook not found: {notebook_path}")

    def _emit_cell_outputs(cell: dict):
        if not log_cb:
            return
        outs = cell.get("outputs", []) or []
        for o in outs:
            ot = o.get("output_type")
            if ot == "stream":
                txt = o.get("text", "")
                if txt:
                    log_cb(txt)
            elif ot == "error":
                ename = o.get("ename", "Error")
                evalue = o.get("evalue", "")
                tb = o.get("traceback", []) or []
                log_cb(f"[ERROR] {ename}: {evalue}\n" + "\n".join(tb))

    class LiveExecutePreprocessor(ExecutePreprocessor):
        def __init__(self, *args, log_cb=None, **kwargs):
            super().__init__(*args, **kwargs)
            self._log_cb = log_cb

        # IMPORTANT: execute.py may call preprocess_cell(..., store_history=...)
        def preprocess_cell(self, cell, resources, cell_index, store_history=True):
            # ✅ Compatibility: some nbconvert versions don't accept store_history in super()
            try:
                cell, resources = super().preprocess_cell(
                    cell, resources, cell_index, store_history=store_history
                )
            except TypeError:
                cell, resources = super().preprocess_cell(cell, resources, cell_index)

            _emit_cell_outputs(cell)
            return cell, resources

    with tempfile.TemporaryDirectory(prefix="edd_proc_") as td:
        in_path = os.path.join(td, "EDD Report.xlsx")
        with open(in_path, "wb") as f:
            f.write(raw_xlsx_bytes)

        with open(notebook_path, "r", encoding="utf-8") as f:
            nb = nbformat.read(f, as_version=4)

        ep = LiveExecutePreprocessor(timeout=timeout_sec, kernel_name="python3", log_cb=log_cb)

        try:
            ep.preprocess(nb, {"metadata": {"path": td}})
        except Exception as e:
            tb = traceback.format_exc()
            raise Exception(f"Notebook execution failed: {e}\n\n{tb}")

        out_path = _pick_output_xlsx(td)
        if not out_path:
            raise Exception("Processing completed but no .xlsx output was found in temp folder.")

        with open(out_path, "rb") as f:
            return f.read()


# =============================================================================
# ✅ STREAMLIT UI
# =============================================================================

st.set_page_config(page_title="EDD MIS Chatbot", layout="wide")

st.title("📦 Performance Analytics Chatbot")
st.caption("Process Raw Report → Download Processed File → View Summary → Ask Questions")

model = "gpt-4o-mini"

if "xlsx_bytes" not in st.session_state:
    st.session_state["xlsx_bytes"] = None
if "processed_filename" not in st.session_state:
    st.session_state["processed_filename"] = "EDD_processed.xlsx"
if "report_source" not in st.session_state:
    st.session_state["report_source"] = "Upload"

if "proc_log_lines" not in st.session_state:
    st.session_state["proc_log_lines"] = []

tabs = st.tabs(["🛠️ Process File", "📊 Summary View", "💬 Ask a Question"])


# ---------------------------
# TAB 1: Process File
# ---------------------------
with tabs[0]:
    st.subheader("Process Raw Report")
    st.caption("Upload RAW EDD report (.xlsx) → Click Process → Download processed file (with 'summary' sheet).")

    if nbformat is None or ExecutePreprocessor is None:
        st.warning(
            "Notebook runner libs missing. Add these to requirements.txt:\n"
            "- nbformat\n- nbconvert\n- ipykernel\n- jupyter_client\n"
        )

    raw_up = st.file_uploader("Upload RAW EDD Excel (.xlsx)", type=["xlsx"], key="raw_upload")

    col1, col2 = st.columns([1, 2])
    with col1:
        proc_btn = st.button("Process", type="primary", disabled=(raw_up is None))
    with col2:
        clear_proc = st.button("Clear Processed")

    log_box = st.empty()

    def push_log(txt: str):
        if txt is None:
            return
        for line in str(txt).splitlines():
            line = line.rstrip("\n").rstrip("\r").rstrip()
            if line:
                st.session_state["proc_log_lines"].append(line)

        log_box.markdown(
            "#### ⏱️ Processing log\n"
            "```\n" + "\n".join(st.session_state["proc_log_lines"][-80:]) + "\n```"
        )

    if clear_proc:
        st.session_state["xlsx_bytes"] = None
        st.session_state["processed_filename"] = "EDD_processed.xlsx"
        st.session_state["proc_log_lines"] = []
        log_box.empty()
        st.success("Cleared processed file from session.")

    if proc_btn and raw_up is not None:
        try:
            st.session_state["proc_log_lines"] = []
            push_log("Starting notebook execution...")

            raw_bytes = raw_up.getvalue()

            t0 = time.perf_counter()
            with st.spinner("(processing raw report)"):
                processed_bytes = run_week_optimized_notebook(
                    raw_xlsx_bytes=raw_bytes,
                    notebook_path="week_optimized.ipynb",
                    timeout_sec=1800,
                    log_cb=push_log,
                )
            t1 = time.perf_counter()

            push_log(f"DONE. Total notebook time: {t1 - t0:.2f}s")

            st.session_state["xlsx_bytes"] = processed_bytes
            base = os.path.splitext(raw_up.name)[0]
            st.session_state["processed_filename"] = f"processed_{base}.xlsx"

            st.success("Processed successfully ✅")
            st.info(f"⏱️ Total processing time: {t1 - t0:.2f} seconds")

            st.download_button(
                label="⬇️ Download Processed Excel",
                data=processed_bytes,
                file_name=st.session_state["processed_filename"],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            st.info("Ab aap 'Summary View' aur 'Ask a Question' tabs me isi processed file ko use kar sakte ho.")

        except Exception as e:
            st.error(f"Processing failed: {e}")


# ---------------------------
# TAB 2: Summary View
# ---------------------------
with tabs[1]:
    st.subheader("Summary Sheet")
    st.caption("If you already have the processed excel file with the summary sheet, you can upload it here.")

    source = st.radio(
        "Select Processed Report Source:",
        ["Use Processed From Tab", "Upload", "Link"],
        horizontal=True,
        index=0,
        key="summary_source_radio",
    )

    xbytes = None

    if source == "Use Processed From Tab":
        xbytes = st.session_state.get("xlsx_bytes")
        if not xbytes:
            st.info("Pehle 'Process File' tab me raw report process karo, ya Upload/Link choose karo.")
    elif source == "Upload":
        uploaded = st.file_uploader("Upload PROCESSED Excel (.xlsx)", type=["xlsx"], key="processed_upload")
        if uploaded is not None:
            st.session_state["xlsx_bytes"] = uploaded.getvalue()
            st.session_state["processed_filename"] = uploaded.name
            xbytes = st.session_state["xlsx_bytes"]
    else:
        st.write("Paste a **direct downloadable** .xlsx link (public/shareable).")
        st.caption("Tip: Prefer Google Drive 'file/d/<id>' link for best reliability.")
        link = st.text_input(
            "Processed Report Link (.xlsx)",
            placeholder="e.g. Google Drive file link / Google Sheets / Dropbox / direct URL",
            key="processed_report_link_input",
        )
        colA, colB = st.columns([1, 2])
        with colA:
            fetch_btn = st.button("Fetch Processed Report", key="fetch_processed_btn")
        with colB:
            clear_btn = st.button("Clear", key="clear_processed_btn")

        if clear_btn:
            st.session_state["xlsx_bytes"] = None
            st.session_state["processed_report_link_input"] = ""
            xbytes = None

        if fetch_btn and (link or "").strip():
            try:
                with st.spinner("Downloading processed report from link..."):
                    st.session_state["xlsx_bytes"] = fetch_xlsx_bytes_from_link(link.strip())
                st.session_state["processed_filename"] = "processed_from_link.xlsx"
                st.success("Processed report fetched successfully ✅")
                xbytes = st.session_state["xlsx_bytes"]
            except Exception as e:
                st.session_state["xlsx_bytes"] = None
                st.error(f"Could not fetch report: {e}")
                xbytes = None
        else:
            xbytes = st.session_state.get("xlsx_bytes")

    if not xbytes:
        st.info("No processed file loaded yet.")
    else:
        try:
            sdf = load_summary_df(xbytes, sheet="summary")
            st.dataframe(sdf, use_container_width=True, height=600)
        except Exception as e:
            st.error(f"Could not read the summary sheet: {e}")


# ---------------------------
# TAB 3: Ask a Question
# ---------------------------
with tabs[2]:
    st.subheader("Ask Questions (Summary + Auto Calculations)")
    st.caption("Ye tab current loaded processed Excel ke 'summary' sheet se answer dega, aur jahan possible ho wahan %/ratios compute bhi karega.")

    xbytes = st.session_state.get("xlsx_bytes")
    if not xbytes:
        st.info("Please process/upload/fetch a processed Excel file first (must contain 'summary' sheet).")
    else:
        try:
            summary_df = load_summary_df(xbytes, sheet="summary")
        except Exception as e:
            st.error(f"Could not read the summary sheet: {e}")
            summary_df = pd.DataFrame()

        q = st.text_input(
            "Type your question:",
            placeholder="e.g. week-48 picked volume % EAST-I / week 46 air on time arrival EAST-I / week 46 CN status % ALL INDIA",
            key="ask_q_input",
        )
        ask = st.button("Ask", key="ask_btn")

        if ask and q.strip():
            week, zone, items = answer_from_summary(summary_df, q)

            if not items:
                st.error("Couldn't match the week/metric. Try: 'week 46 AIR On Time Arrival EAST-I'")
            else:
                derived_lines = compute_derived_lines(summary_df, q, week, zone, items)

                with st.expander("🔎 Matched rows (from summary)"):
                    st.write("\n".join([f"• {m}: {v}" for m, v in items]) or "-")

                if derived_lines:
                    with st.expander("🧮 Computed (derived) metrics"):
                        st.write("\n".join([f"• {x}" for x in derived_lines]))

                client = make_client()

                context_lines = "\n".join([f"- {m}: {v}" for m, v in items])
                derived_block = "\n".join([f"- {x}" for x in derived_lines]) if derived_lines else ""

                prompt = f"""Question: {q}

Excel Summary for {week} (zone={zone}):
{context_lines}

Derived calculations (computed from above values; only if possible):
{derived_block if derived_block else "- (none)"}

Now answer the question in a short, direct way.
Rules:
- You MAY compute percentages/ratios/trends from the provided numbers.
- If you compute something, show the formula briefly (e.g., A/B*100).
- Do NOT invent missing denominators. If a required number is missing, clearly say what's missing.
- Do NOT show JSON or code.
- If multiple rows, format as bullet points.
"""

                if client:
                    with st.spinner("Generating answer..."):
                        ans = llm_answer(
                            client=client,
                            model=model,
                            system=(
                                "You are a logistics MIS analyst.\n"
                                "You must stay grounded in the provided Excel context.\n"
                                "You are allowed to do arithmetic (percentages/ratios) using the provided values.\n"
                                "If any required value is missing, say exactly what is missing.\n"
                            ),
                            user=prompt,
                        )
                    st.success(ans)
                else:
                    out_lines = [f"• {m}: {v}" for m, v in items]
                    if derived_lines:
                        out_lines.append("\nComputed:")
                        out_lines += [f"• {x}" for x in derived_lines]
                    st.success("\n".join(out_lines))
