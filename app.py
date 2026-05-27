"""
Sales Call Summary Download
Streamlit app — lets users apply filters and download matching rows as CSV.

Two download formats that exactly match the Tableau dashboard lenses
(field names and left-to-right column order):
  • Sales Call Summary        (53 columns)
  • GC Sales Call Summary     (52 columns)

Computed columns reproduce two Tableau calculated fields inline:
  • "Sales Result Status"  — Calculation_169940583539675136
      IF SalesResultStatus='Not Sold' AND Sale_is_In_Progress__c THEN 'Not Sold In-Progress'
      ELSEIF SalesResultStatus='Not Sold' THEN 'Not Sold Final'
      ELSE SalesResultStatus
  • "Order Not Shipped"    — Calculation_739153323692511235 (GC lens only)
      IIF(IsNull(InsideSalesInteractionID), 'No', 'Yes')

Data source: INTEGRATE_IO_DATABASE.RPTDB."SalesCallSummary" (Snowflake)
Credentials: stored in .streamlit/secrets.toml (never committed to git)
Deploy:       Streamlit Community Cloud → main file = streamlit/app.py
"""

import io
from datetime import date, timedelta

import pandas as pd
import snowflake.connector
import streamlit as st
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# ─── Page config ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Sales Call Summary Download",
    page_icon="📥",
    layout="wide",
)

# ─── Constants ───────────────────────────────────────────────────────────────

TABLE = 'INTEGRATE_IO_DATABASE.RPTDB."SalesCallSummary"'

# All columns fetched from Snowflake — the union of every DB column needed by
# either lens, plus ActivityDates (date-range filter / ORDER BY) and
# Sale_is_In_Progress__c (required to compute the "Sales Result Status" column).
ALL_DB_COLS = [
    "ActivityDates",
    "ActivityNumber",
    "Affiliation",
    "Affiliation_Parent__c",
    "AnnualizedSoldVolume",
    "Big_Hit_Target_Call",
    "BigHitAuditID",
    "BigHitVerificationType",
    "BigHitVerified",
    "C_STORE_DISTRIBUTOR__c",
    "Client",
    "Client_Account18_ID",
    "CommOrder18_ID__c",
    "CommOrderID",
    "CommittedOrderDate",
    "CommittedOrderEndDate",
    "Competitor",
    "CoolSchoolID",
    "CorporateAffiliation__c",
    "CustomerType",
    "DateCompleted",
    "DateofLastCall",
    "DaysSinceLastCall__c",
    "Employee",
    "EstimatedWeeklyCasePotential",
    "FeedbackElaboration",
    "Followup_Call__c",
    "GPO",
    "GeoMarket_ID",
    "GeographicMarket",
    "InitiativeName_InsideSales",
    "InsideSalesInteractionID",
    "InsideSalesNotes",
    "InsideSalesStatus",
    "Is_operator_under_contract__c",
    "Job_Title",
    "KI18ID",
    "KIID",
    "KeyImpactRegionChain",
    "LLO",
    "LLO_Call",
    "LTOWeeks",
    "LastUpdateDate",
    "LastUpdated",
    "Marketing_program_exist__c",
    "Meeting_type__c",
    "Name",
    "Not_Sold_Price__c",
    "Not_Sold_Result_is_Final__c",
    "NotSoldReason",
    "Number_of_Operator_Units__c",
    "OnBehalfofDistributor",
    "OperatorFeedback__c",
    "OperatorID",
    "OperatorName",
    "OperatorUnwillingtoConsiderFeedback",
    "Operator_Contract_Expiry_Date__c",
    "PipelineRelatedCall",
    "PipelineRelatedCall__c",
    "PipelineRelated18_ID",
    "PipelineRelatedCallStatus",
    "PlacementCall",
    "PremierID",
    "PrimaryEmployeeAssigned",
    "Product18_ID__c",
    "ProductBrand",
    "ProductCategory",
    "ProductPkSz",
    "ProductPresented",
    "ProductSKU",
    "PurchaseFrequency",
    "Quality_Issue__c",
    "Quantity_CasesperWeek",
    "QuantityEntered",
    "Reasons_unwilling_to_consider__c",
    "Sale_is_In_Progress__c",
    "Sales_in_Progress__c",
    "SalesCallSummaryNotes",
    "SalesForceActivityID",
    "SalesResultStatus",
    "SalesResultStatusLong",
    "Sampled__c",
    "SegmentName",
    "Source",
    "Specific_Issue__c",
    "StartDate",
    "Status",
    "StrategicLLO",
    "SubSegmentName",
    "SupervisorName",
    "TargetedAssignmentCall",
    "Tech1500_TY_Rank__c",
    "UsingPlusMinusAmount",
    "Using_Purchase_Frequency",
    "Was_a_cutting_conducted__c",
    "What_is_the_allowance_per_case_needed__c",
    "WhataretheNextSteps",
    "When_do_you_expect_testing_to_conclude__c",
    "Where_do_we_need_to_get_pricing__c",
    "Who_Was_Distributor_Name__c",
    "Who_needs_to_review_product_at_Operator__c",
    "Who_was_the_Distributor__c",
    "Will_Client_pay_slotting_fee__c",
    "Zone",
    "pa_ProductActivity18_ID__c",
    "t_NotSoldReason__c",
]

# ── "Sales Call Summary" lens ─────────────────────────────────────────────────
# Column order matches the Tableau worksheet rows left-to-right.
# "Sales Result Status" (with spaces) is a computed column; see add_computed_cols().
SCS_OUTPUT_COLS = [
    "Job_Title",
    "Employee",
    "Status",
    "PipelineRelatedCall__c",
    "StartDate",
    "DateCompleted",
    "GeographicMarket",
    "Zone",
    "CustomerType",
    "KeyImpactRegionChain",
    "Number_of_Operator_Units__c",
    "LLO",
    "OperatorName",
    "OnBehalfofDistributor",
    "SegmentName",
    "SubSegmentName",
    "Affiliation",
    "GPO",
    "Client",
    "ProductPresented",
    "ProductSKU",
    "ProductBrand",
    "ProductCategory",
    "ProductPkSz",
    "EstimatedWeeklyCasePotential",
    "SalesResultStatus",
    "Sales Result Status",        # computed — Tableau Calculation_169940583539675136
    "UsingPlusMinusAmount",
    "t_NotSoldReason__c",
    "OperatorUnwillingtoConsiderFeedback",
    "OperatorFeedback__c",
    "WhataretheNextSteps",
    "Competitor",
    "PurchaseFrequency",
    "QuantityEntered",
    "Quantity_CasesperWeek",
    "LTOWeeks",
    "AnnualizedSoldVolume",
    "BigHitVerified",
    "BigHitAuditID",
    "BigHitVerificationType",
    "SalesCallSummaryNotes",
    "InitiativeName_InsideSales",
    "InsideSalesStatus",
    "InsideSalesNotes",
    "InsideSalesInteractionID",
    "PremierID",
    "DateofLastCall",
    "CorporateAffiliation__c",
    "SalesForceActivityID",
    "Meeting_type__c",
    "ActivityNumber",
    "OperatorID",
]

# ── "GC Sales Call Summary" lens ──────────────────────────────────────────────
# "Sales Result Status" and "Order Not Shipped" are computed columns.
GC_OUTPUT_COLS = [
    "Employee",
    "Status",
    "StartDate",
    "DateCompleted",
    "GeographicMarket",
    "Zone",
    "CustomerType",
    "LLO",
    "KeyImpactRegionChain",
    "OperatorName",
    "SegmentName",
    "SubSegmentName",
    "Affiliation",
    "GPO",
    "Client",
    "ProductPresented",
    "ProductSKU",
    "ProductBrand",
    "ProductCategory",
    "ProductPkSz",
    "Number_of_Operator_Units__c",
    "EstimatedWeeklyCasePotential",
    "SalesResultStatus",
    "Sales Result Status",        # computed — Tableau Calculation_169940583539675136
    "PipelineRelatedCall__c",
    "BigHitVerified",
    "BigHitAuditID",
    "BigHitVerificationType",
    "Order Not Shipped",          # computed — Tableau Calculation_739153323692511235
    "InsideSalesNotes",
    "t_NotSoldReason__c",
    "OperatorUnwillingtoConsiderFeedback",
    "OperatorFeedback__c",
    "WhataretheNextSteps",
    "Competitor",
    "PurchaseFrequency",
    "Name",
    "QuantityEntered",
    "Quantity_CasesperWeek",
    "CommittedOrderDate",
    "AnnualizedSoldVolume",
    "OnBehalfofDistributor",
    "CorporateAffiliation__c",
    "Affiliation_Parent__c",
    "SalesCallSummaryNotes",
    "InitiativeName_InsideSales",
    "SalesForceActivityID",
    "KIID",
    "Meeting_type__c",
    "ActivityNumber",
    "CommOrder18_ID__c",
    "OperatorID",
]

# Categorical filters: (sidebar label, Snowflake column name)
CATEGORICAL_FILTERS = [
    ("Status",                "Status"),
    ("Source",                "Source"),
    ("Employee",              "Employee"),
    ("Geographic Market",     "GeographicMarket"),
    ("Corporate Affiliation", "CorporateAffiliation__c"),
    ("Customer Type",         "CustomerType"),
    ("Segment",               "SegmentName"),
    ("Sub-Segment",           "SubSegmentName"),
    ("Client (Manufacturer)", "Client"),
    ("Affiliation",           "Affiliation"),
    ("Sales Result Status",   "_SalesResultStatus"),  # derived — handled specially
]


# ─── Snowflake connection ─────────────────────────────────────────────────────

@st.cache_resource(show_spinner="Connecting to Snowflake…")
def get_conn():
    s = st.secrets["snowflake"]
    auth_type = s.get("auth_type", "password")

    if auth_type == "private_key":
        pem = s["private_key"]
        if isinstance(pem, str):
            pem = pem.encode()
        pk = serialization.load_pem_private_key(pem, password=None, backend=default_backend())
        pkb = pk.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return snowflake.connector.connect(
            account=s["account"],
            user=s["user"],
            private_key=pkb,
            role=s.get("role", "INTEGRATE_IO_ROLE"),
            warehouse=s.get("warehouse", "INTEGRATE_IO_WAREHOUSE"),
            database=s.get("database", "INTEGRATE_IO_DATABASE"),
            schema=s.get("schema", "RPTDB"),
        )
    else:
        return snowflake.connector.connect(
            account=s["account"],
            user=s["user"],
            password=s["password"],
            role=s.get("role", "INTEGRATE_IO_ROLE"),
            warehouse=s.get("warehouse", "INTEGRATE_IO_WAREHOUSE"),
            database=s.get("database", "INTEGRATE_IO_DATABASE"),
            schema=s.get("schema", "RPTDB"),
        )


def run_query(sql: str, params=None) -> pd.DataFrame:
    """Small queries only (filter options, COUNT).  Do NOT use for full data fetch."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    cur.close()
    return pd.DataFrame(rows, columns=cols)


# ─── Cached filter options (refresh once per day) ────────────────────────────

@st.cache_data(ttl=86400, show_spinner="Loading filter options…")
def load_filter_options() -> dict:
    """Return sorted distinct values for every categorical filter column."""
    options = {}
    cols_to_load = [col for _, col in CATEGORICAL_FILTERS if not col.startswith("_")]

    for col in cols_to_load:
        df = run_query(
            f'SELECT DISTINCT "{col}" FROM {TABLE} '
            f'WHERE "{col}" IS NOT NULL AND "{col}" != \'\' '
            f'ORDER BY "{col}"'
        )
        options[col] = df.iloc[:, 0].tolist()

    # Sales Result Status — derived from SalesResultStatus + Sale_is_In_Progress__c
    df_srs = run_query(f"""
        SELECT DISTINCT
            CASE
                WHEN "SalesResultStatus" = 'Not Sold' AND "Sale_is_In_Progress__c" = TRUE
                    THEN 'Not Sold In-Progress'
                WHEN "SalesResultStatus" = 'Not Sold' AND "Sale_is_In_Progress__c" = FALSE
                    THEN 'Not Sold Final'
                ELSE "SalesResultStatus"
            END AS srs
        FROM {TABLE}
        WHERE "SalesResultStatus" IS NOT NULL
        ORDER BY srs
    """)
    options["_SalesResultStatus"] = df_srs["SRS"].tolist()

    # Date range bounds
    df_dates = run_query(
        f'SELECT MIN("ActivityDates")::DATE, MAX("ActivityDates")::DATE FROM {TABLE}'
    )
    options["_date_min"] = df_dates.iloc[0, 0]
    options["_date_max"] = df_dates.iloc[0, 1]

    return options


# ─── Query builder ────────────────────────────────────────────────────────────

def build_where(date_start: date, date_end: date, selections: dict) -> tuple[str, list]:
    """Return (WHERE clause string, params list) for the given filters."""
    clauses = []
    params  = []

    # Date range
    clauses.append('"ActivityDates"::DATE BETWEEN %s AND %s')
    params += [str(date_start), str(date_end)]

    # Categorical filters
    for label, col in CATEGORICAL_FILTERS:
        vals = selections.get(col, [])
        if not vals:
            continue

        if col == "_SalesResultStatus":
            # Translate chosen display values back to raw SQL conditions
            raw_vals, include_in_progress, include_final = [], False, False
            for v in vals:
                if v == "Not Sold In-Progress":
                    include_in_progress = True
                elif v == "Not Sold Final":
                    include_final = True
                else:
                    raw_vals.append(v)

            sub_clauses = []
            if raw_vals:
                placeholders = ", ".join(["%s"] * len(raw_vals))
                sub_clauses.append(
                    f'("SalesResultStatus" NOT IN (\'Not Sold\') '
                    f'AND "SalesResultStatus" IN ({placeholders}))'
                )
                params += raw_vals
            if include_in_progress:
                sub_clauses.append(
                    '"SalesResultStatus" = \'Not Sold\' AND "Sale_is_In_Progress__c" = TRUE'
                )
            if include_final:
                sub_clauses.append(
                    '"SalesResultStatus" = \'Not Sold\' AND "Sale_is_In_Progress__c" = FALSE'
                )
            if sub_clauses:
                clauses.append("(" + " OR ".join(sub_clauses) + ")")
        else:
            placeholders = ", ".join(["%s"] * len(vals))
            clauses.append(f'"{col}" IN ({placeholders})')
            params += vals

    where = "WHERE " + "\n  AND ".join(clauses) if clauses else ""
    return where, params


def count_records(where: str, params: list) -> int:
    df = run_query(f"SELECT COUNT(*) FROM {TABLE} {where}", params)
    return int(df.iloc[0, 0])


# ─── Computed columns ─────────────────────────────────────────────────────────

def _as_bool(val) -> bool:
    """Coerce a Snowflake boolean value (True/False/1/0/'TRUE'/'FALSE') to Python bool."""
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        return val.strip().upper() in ("TRUE", "1", "YES")
    return False


def add_computed_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Add Tableau calculated-field columns using vectorized operations.

    "Sales Result Status"  — Calculation_169940583539675136
    "Order Not Shipped"    — Calculation_739153323692511235 (GC lens)
    """
    # Vectorised boolean coercion for Sale_is_In_Progress__c
    ip_col = df["Sale_is_In_Progress__c"]
    if pd.api.types.is_bool_dtype(ip_col):
        in_progress = ip_col
    elif pd.api.types.is_numeric_dtype(ip_col):
        in_progress = ip_col.astype(bool)
    else:
        in_progress = ip_col.map(_as_bool)

    not_sold = df["SalesResultStatus"] == "Not Sold"
    srs = df["SalesResultStatus"].copy()
    srs = srs.mask(not_sold &  in_progress, "Not Sold In-Progress")
    srs = srs.mask(not_sold & ~in_progress, "Not Sold Final")
    df["Sales Result Status"] = srs

    # IIF(IsNull([InsideSalesInteractionID]), 'No', 'Yes')
    iid = df["InsideSalesInteractionID"]
    is_null = iid.isna() | (iid.astype(str).str.strip().isin(["", "None", "nan"]))
    df["Order Not Shipped"] = is_null.map({True: "No", False: "Yes"})

    return df


# ─── Streaming fetch ──────────────────────────────────────────────────────────

def stream_fetch(where: str, params: list, progress_text) -> dict:
    """Fetch data from Snowflake in chunks and write directly to CSV buffers.

    Processes both download lenses in a single pass so we never hold the full
    dataset as a DataFrame.  Returns CSV bytes + 100-row preview DataFrames.
    """
    conn = get_conn()
    cur  = conn.cursor()

    quoted_cols = ", ".join(f'"{c}" AS "{c}"' for c in ALL_DB_COLS)
    sql = f'SELECT {quoted_cols} FROM {TABLE} {where} ORDER BY "ActivityDates" DESC'
    cur.execute(sql, params or [])

    col_names   = [d[0] for d in cur.description]
    scs_buf     = io.StringIO()
    gc_buf      = io.StringIO()
    scs_preview: list = []
    gc_preview:  list = []
    first_chunk = True
    total_rows  = 0
    CHUNK       = 10_000

    while True:
        rows = cur.fetchmany(CHUNK)
        if not rows:
            break

        chunk_df = pd.DataFrame(rows, columns=col_names)
        chunk_df = add_computed_cols(chunk_df)
        total_rows += len(chunk_df)
        progress_text.caption(f"⏳ Fetched {total_rows:,} rows…")

        for buf, output_cols, preview_list in [
            (scs_buf, SCS_OUTPUT_COLS, scs_preview),
            (gc_buf,  GC_OUTPUT_COLS,  gc_preview),
        ]:
            available  = [c for c in output_cols if c in chunk_df.columns]
            out_chunk  = chunk_df[available]
            out_chunk.to_csv(buf, index=False, header=first_chunk)
            if len(preview_list) < 100:
                remaining = 100 - len(preview_list)
                preview_list.extend(out_chunk.head(remaining).to_dict("records"))

        first_chunk = False

    cur.close()

    return {
        "scs_csv":     scs_buf.getvalue().encode("utf-8"),
        "gc_csv":      gc_buf.getvalue().encode("utf-8"),
        "scs_preview": pd.DataFrame(scs_preview),
        "gc_preview":  pd.DataFrame(gc_preview),
        "total_rows":  total_rows,
    }


# ─── UI ──────────────────────────────────────────────────────────────────────

def main():
    st.title("📥 Sales Call Summary Download")
    st.caption("Filter the sales call data and download matching rows as CSV.")

    # Load options (cached)
    with st.spinner("Loading filter options…"):
        opts = load_filter_options()

    date_min = opts["_date_min"] or date.today() - timedelta(days=365)
    date_max = opts["_date_max"] or date.today()

    # ── Sidebar filters ──────────────────────────────────────────────────────
    with st.sidebar:
        st.header("Filters")

        date_start, date_end = st.date_input(
            "Activity Date Range",
            value=(date_max - timedelta(days=90), date_max),
            min_value=date_min,
            max_value=date_max,
            help="Filters on the ActivityDates column (= DateCompleted if set, else StartDate).",
        )

        selections = {}
        for label, col in CATEGORICAL_FILTERS:
            choices = opts.get(col, [])
            selections[col] = st.multiselect(label, choices, default=[])

        st.divider()
        apply = st.button("Apply Filters", type="primary", use_container_width=True)

    # ── Main area ────────────────────────────────────────────────────────────
    if not apply and "last_where" not in st.session_state:
        st.info("Set your filters in the sidebar and click **Apply Filters**.")
        return

    if apply:
        where, params = build_where(date_start, date_end, selections)
        st.session_state["last_where"]  = where
        st.session_state["last_params"] = params
        st.session_state["df"]          = None   # clear stale data
    else:
        where  = st.session_state["last_where"]
        params = st.session_state["last_params"]

    # Record count
    with st.spinner("Counting matching records…"):
        n = count_records(where, params)

    count_col, _ = st.columns([3, 1])
    with count_col:
        st.metric("Matching records", f"{n:,}")

    if n == 0:
        st.warning("No records match the current filters.")
        return

    st.divider()

    # Fetch button
    fetch_col, _ = st.columns([2, 3])
    with fetch_col:
        do_fetch = st.button("⬇️ Fetch data", use_container_width=True)

    if do_fetch:
        progress_text = st.empty()
        with st.spinner(f"Streaming {n:,} rows from Snowflake…"):
            result = stream_fetch(where, params, progress_text)
        progress_text.empty()
        st.session_state["fetch_result"] = result
        st.success(f"Ready — {result['total_rows']:,} rows fetched.")

    result = st.session_state.get("fetch_result")
    if result is None:
        return

    total_rows = result["total_rows"]

    # ── Download tabs — one per dashboard lens ───────────────────────────────
    st.subheader("Download")
    tab_scs, tab_gc = st.tabs(["Sales Call Summary", "GC Sales Call Summary"])

    for tab, csv_key, preview_key, lens_name, output_cols in [
        (tab_scs, "scs_csv", "scs_preview", "Sales Call Summary",    SCS_OUTPUT_COLS),
        (tab_gc,  "gc_csv",  "gc_preview",  "GC Sales Call Summary", GC_OUTPUT_COLS),
    ]:
        with tab:
            csv_bytes  = result[csv_key]
            preview_df = result[preview_key]
            safe_name  = lens_name.lower().replace(" ", "_")
            n_cols     = len([c for c in output_cols if c in (list(preview_df.columns) + ["Sales Result Status", "Order Not Shipped"])])

            dl_col, info_col = st.columns([2, 3])
            with dl_col:
                st.download_button(
                    label=f"⬇️ Download CSV  ({total_rows:,} rows × {n_cols} cols)",
                    data=csv_bytes,
                    file_name=f"{safe_name}_{date_start}_{date_end}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    type="primary",
                    key=f"dl_{safe_name}",
                )
            with info_col:
                st.caption(
                    f"{len(preview_df.columns)} columns · column order matches "
                    f"the Tableau **{lens_name}** worksheet"
                )

            with st.expander("Preview (first 100 rows)", expanded=False):
                st.dataframe(preview_df, use_container_width=True)


if __name__ == "__main__":
    main()
