import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from fpdf import FPDF
import tempfile
import os
from datetime import date

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None

# --- Page Configuration ---
st.set_page_config(page_title="YT Asset Strategic Analysis", layout="wide")

st.title("Strategic Asset Lifecycle & Decay Analysis")

# 1. Static Glossary
st.markdown("""
### 📖 Glossary
- **Asset**: The core creative content, identified in this data by a unique **Custom ID**. 
- **Iteration**: Each individual upload of an asset. **Video 1** is the original baseline, while **Video 2, 3, etc.** are re-uploads.
- **28-Day Injection Impact**: Measures the **percentage lift**, the **total view increase**, and the **daily view velocity** added to the asset's total volume in the 28 days after an iteration launch.
""")
st.divider()

st.markdown("""
## 🚪 Data input front end
Choose a source below to load your YouTube asset view data.
- `Upload CSV`: use a local file export.
- `BigQuery`: query live data by `Custom ID` and date range.
""")

st.markdown("""
### How to use
1. Pick your data source.
2. Enter one or more `Custom ID`s when using BigQuery.
3. Select a start and end date.
4. Run the analysis and download the PDF report.
""")

def format_views(n):
    """Formats large numbers into readable K or M strings."""
    if n is None: return "0"
    abs_n = abs(n)
    if abs_n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    elif abs_n >= 1_000:
        return f"{n/1_000:.1f}K"
    else:
        return f"{int(n)}"

def calculate_decay_day(video_df, decay_threshold_pct):
    video_df = video_df.sort_values('Days Since Published')
    if video_df.empty: return None
    peak_val = video_df['Metrics Organic Views'].max()
    if peak_val <= 0: return None
    peak_day = video_df.loc[video_df['Metrics Organic Views'].idxmax(), 'Days Since Published']
    threshold = (decay_threshold_pct / 100) * peak_val
    post_peak = video_df[video_df['Days Since Published'] >= peak_day].copy()
    post_peak['below_threshold'] = post_peak['Metrics Organic Views'] < threshold
    post_peak['streak'] = post_peak['below_threshold'].rolling(window=3).sum()
    decay_hit = post_peak[post_peak['streak'] == 3]
    if not decay_hit.empty:
        return decay_hit.iloc[0]['Days Since Published'] - 2
    return None

@st.cache_data(ttl=300)
def query_bq_data(custom_ids, start_date, end_date):
    if bigquery is None:
        raise ImportError("google-cloud-bigquery must be installed to query BigQuery.")

    client = bigquery.Client()
    query = """
    SELECT
        data_warehouse_yt_content_owner_video_metadata.video_id AS data_warehouse_yt_content_owner_video_metadata_video_id,
        data_warehouse_yt_content_owner_video_metadata.video_title AS data_warehouse_yt_content_owner_video_metadata_video_title,
        data_warehouse_yt_channel_metadata.channel_title AS data_warehouse_yt_channel_metadata_channel_title,
        DATE(data_warehouse_yt_content_owner_video_metadata.time_published) AS data_warehouse_yt_content_owner_video_metadata_time_published_date,
        data_warehouse_yt_content_owner_video_metadata.custom_id AS data_warehouse_yt_content_owner_video_metadata_custom_id,
        data_warehouse_yt_daily_video_views.day AS data_warehouse_yt_daily_video_views_day_date,
        COALESCE(SUM(data_warehouse_yt_daily_video_views.organic_views), 0) AS data_warehouse_yt_daily_video_views_organic_views
    FROM `resonant-gizmo-745.data_warehouse.daily_video_views_yt` AS data_warehouse_yt_daily_video_views
    LEFT JOIN `resonant-gizmo-745.data_warehouse.content_owner_video_metadata` AS data_warehouse_yt_content_owner_video_metadata
        ON data_warehouse_yt_daily_video_views.video_id = data_warehouse_yt_content_owner_video_metadata.video_id
    LEFT JOIN `resonant-gizmo-745.data_warehouse.metadata_channel` AS data_warehouse_yt_channel_metadata
        ON data_warehouse_yt_daily_video_views.channel_id = data_warehouse_yt_channel_metadata.channel_id
    WHERE data_warehouse_yt_daily_video_views.day >= @start_date
      AND data_warehouse_yt_daily_video_views.day <= @end_date
      AND data_warehouse_yt_content_owner_video_metadata.custom_id IN UNNEST(@custom_ids)
      AND (data_warehouse_yt_channel_metadata.channel_title <> 'StreamVault' OR data_warehouse_yt_channel_metadata.channel_title IS NULL)
    GROUP BY
        1, 2, 3, 4, 5, 6
    ORDER BY
        4 DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            bigquery.ArrayQueryParameter("custom_ids", "STRING", custom_ids),
        ]
    )

    return client.query(query, job_config=job_config).to_dataframe()

@st.cache_data(ttl=300)
def query_bq_custom_ids(start_date, end_date):
    if bigquery is None:
        raise ImportError("google-cloud-bigquery must be installed to query BigQuery.")

    client = bigquery.Client()
    query = """
    SELECT DISTINCT
        data_warehouse_yt_content_owner_video_metadata.custom_id AS custom_id
    FROM `resonant-gizmo-745.data_warehouse.daily_video_views_yt` AS data_warehouse_yt_daily_video_views
    LEFT JOIN `resonant-gizmo-745.data_warehouse.content_owner_video_metadata` AS data_warehouse_yt_content_owner_video_metadata
        ON data_warehouse_yt_daily_video_views.video_id = data_warehouse_yt_content_owner_video_metadata.video_id
    LEFT JOIN `resonant-gizmo-745.data_warehouse.metadata_channel` AS data_warehouse_yt_channel_metadata
        ON data_warehouse_yt_daily_video_views.channel_id = data_warehouse_yt_channel_metadata.channel_id
    WHERE data_warehouse_yt_daily_video_views.day >= @start_date
      AND data_warehouse_yt_daily_video_views.day <= @end_date
      AND data_warehouse_yt_content_owner_video_metadata.custom_id IS NOT NULL
      AND (data_warehouse_yt_channel_metadata.channel_title <> 'StreamVault' OR data_warehouse_yt_channel_metadata.channel_title IS NULL)
    ORDER BY custom_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )

    result = client.query(query, job_config=job_config).to_dataframe()
    return result['custom_id'].astype(str).tolist()

# --- Data source selection ---
if 'bq_df' not in st.session_state:
    st.session_state.bq_df = None

data_source = st.radio("Choose data source", ["Upload CSV", "BigQuery"], horizontal=True)

bq_df = st.session_state.bq_df
uploaded_file = None

if 'available_custom_ids' not in st.session_state:
    st.session_state.available_custom_ids = []
if 'selected_custom_ids' not in st.session_state:
    st.session_state.selected_custom_ids = []

if data_source == "BigQuery":
    with st.expander("BigQuery settings", expanded=True):
        st.write("Use BigQuery to query your YouTube data by `custom_id` and date range.")
        start_date = st.date_input("Start date", date(2019, 1, 1))
        end_date = st.date_input("End date", date(2019, 12, 28))

        if end_date < start_date:
            st.error("End date must be the same or after the start date.")

        if st.button("Load available Custom IDs"):
            with st.spinner("Fetching available Custom IDs..."):
                try:
                    st.session_state.available_custom_ids = query_bq_custom_ids(start_date, end_date)
                    st.success(f"Loaded {len(st.session_state.available_custom_ids)} Custom IDs.")
                except Exception as exc:
                    st.error(f"BigQuery error: {exc}")

        if st.session_state.available_custom_ids:
            available_ids = st.session_state.available_custom_ids
            page_size = st.number_input("IDs per page", min_value=5, max_value=100, value=25, step=5)
            max_page = max(1, (len(available_ids) - 1) // page_size + 1)
            page = st.slider("Custom ID page", 1, max_page, 1)
            start_ix = (page - 1) * page_size
            end_ix = min(start_ix + page_size, len(available_ids))
            page_ids = available_ids[start_ix:end_ix]

            selected_on_page = st.multiselect(
                f"Select Custom IDs on page {page} ({start_ix+1}-{end_ix})",
                options=page_ids,
                default=[cid for cid in page_ids if cid in st.session_state.selected_custom_ids]
            )

            selected_set = set(st.session_state.selected_custom_ids)
            selected_set.update(selected_on_page)
            deselected_on_page = set(page_ids) - set(selected_on_page)
            selected_set.difference_update(deselected_on_page)
            st.session_state.selected_custom_ids = sorted(selected_set)

            st.write(f"Selected {len(st.session_state.selected_custom_ids)} Custom IDs total.")
            if st.button("Clear selected Custom IDs"):
                st.session_state.selected_custom_ids = []

        custom_ids_text = st.text_area(
            "Manual Custom IDs (comma-separated)",
            ", ".join(st.session_state.selected_custom_ids) if st.session_state.selected_custom_ids else ""
        )
        final_custom_ids = [cid.strip() for cid in custom_ids_text.split(",") if cid.strip()]
        if final_custom_ids:
            st.write(f"Using {len(final_custom_ids)} Custom IDs for the query.")
        else:
            st.warning("No Custom IDs selected yet.")

        if st.button("Load BigQuery data"):
            if not final_custom_ids:
                st.warning("Enter or select at least one Custom ID.")
            else:
                with st.spinner("Querying BigQuery..."):
                    try:
                        st.session_state.bq_df = query_bq_data(final_custom_ids, start_date, end_date)
                        bq_df = st.session_state.bq_df
                        if bq_df.empty:
                            st.warning("No data found for the selected Custom ID(s) and date range.")
                    except Exception as exc:
                        st.error(f"BigQuery error: {exc}")

elif data_source == "Upload CSV":
    with st.expander("CSV upload", expanded=True):
        uploaded_file = st.file_uploader("Upload your YouTube CSV Data", type=["csv"])

if uploaded_file is not None and data_source == "Upload CSV":
    df = pd.read_csv(uploaded_file)
elif data_source == "BigQuery" and bq_df is not None:
    df = bq_df
else:
    df = None

if df is not None:
    # 2. Data Processing
    if data_source == "BigQuery":
        df.columns = [col.replace('\n', '').strip() for col in df.columns]
        df.rename(columns={
            'data_warehouse_yt_content_owner_video_metadata_video_id': 'Video data Video ID',
            'data_warehouse_yt_content_owner_video_metadata_video_title': 'Video data Video Title',
            'data_warehouse_yt_channel_metadata_channel_title': 'Video data Channel Title',
            'data_warehouse_yt_content_owner_video_metadata_time_published_date': 'Video data Published Date',
            'data_warehouse_yt_content_owner_video_metadata_custom_id': 'Video data Custom ID',
            'data_warehouse_yt_daily_video_views_day_date': 'Metrics Date Date',
            'data_warehouse_yt_daily_video_views_organic_views': 'Metrics Organic Views'
        }, inplace=True)
    df.columns = [col.replace('\n', '').strip() for col in df.columns]
    df['Metrics Organic Views'] = pd.to_numeric(df['Metrics Organic Views'].astype(str).str.replace(',', ''), errors='coerce')
    df = df.dropna(subset=['Metrics Organic Views'])
    df['Video data Published Date'] = pd.to_datetime(df['Video data Published Date'])
    df['Metrics Date Date'] = pd.to_datetime(df['Metrics Date Date'])

    with st.expander("Data summary", expanded=True):
        st.write(f"**Data source:** {data_source}")
        st.write(f"**Rows loaded:** {len(df)}")
        if 'Video data Custom ID' in df.columns:
            st.write(f"**Custom IDs:** {df['Video data Custom ID'].nunique()}")
        if 'Metrics Date Date' in df.columns:
            st.write(f"**Metrics date range:** {df['Metrics Date Date'].min().date()} to {df['Metrics Date Date'].max().date()}")
        st.dataframe(df.head(20))

    # Ranking & Timing Logic
    video_info = df[['Video data Custom ID', 'Video data Video ID', 'Video data Published Date']].drop_duplicates()
    video_info = video_info.sort_values(by=['Video data Custom ID', 'Video data Published Date'])
    video_info['Video Rank'] = video_info.groupby('Video data Custom ID').cumcount() + 1
    
    vols = video_info.groupby('Video data Custom ID')['Video data Video ID'].count().reset_index(name='Total_Videos')
    starts = video_info.groupby('Video data Custom ID')['Video data Published Date'].min().reset_index(name='Asset_Day_0')
    
    video_info = video_info.merge(starts, on='Video data Custom ID').merge(vols, on='Video data Custom ID')
    video_info['Days_From_Start'] = (video_info['Video data Published Date'] - video_info['Asset_Day_0']).dt.days
    
    df = df.merge(video_info[['Video data Video ID', 'Video Rank', 'Total_Videos', 'Asset_Day_0', 'Days_From_Start']], on='Video data Video ID')
    df['Days Since Asset Start'] = (df['Metrics Date Date'] - df['Asset_Day_0']).dt.days
    df['Days Since Published'] = (df['Metrics Date Date'] - df['Video data Published Date']).dt.days

    # 3. Sidebar
    st.sidebar.header("Global Settings")
    max_timeline = st.sidebar.slider("Timeline Window (Days)", 30, 1500, 700)
    decay_pct = st.sidebar.slider("Burn-off Threshold (% of Peak)", 10, 95, 90)

    # 4. PDF Setup
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(200, 10, "YouTube Strategic Iteration Report", ln=True, align='C')
    pdf.ln(10)

    unique_vols = sorted([v for v in df['Total_Videos'].unique() if v > 1])

    for current_vol in unique_vols:
        sub_df = df[df['Total_Videos'] == current_vol]
        group_sample_size = sub_df['Video data Custom ID'].nunique()
        
        st.header(f"📈 Assets with {current_vol} Iterations (Sample: {group_sample_size})")
        
        # Chart Logic
        ranks = range(1, current_vol + 1)
        timeline = range(0, max_timeline + 1)
        template = pd.MultiIndex.from_product([ranks, timeline], names=['Video Rank', 'Days Since Asset Start']).to_frame(index=False)
        agg_actual = sub_df.groupby(['Video Rank', 'Days Since Asset Start'])['Metrics Organic Views'].mean().reset_index()
        agg_plot = template.merge(agg_actual, on=['Video Rank', 'Days Since Asset Start'], how='left').fillna(0)
        
        avg_launches = video_info[video_info['Total_Videos'] == current_vol].groupby('Video Rank')['Days_From_Start'].mean()
        for r, start_day in avg_launches.items():
            agg_plot.loc[(agg_plot['Video Rank'] == r) & (agg_plot['Days Since Asset Start'] < start_day), 'Metrics Organic Views'] = 0
        agg_plot['Video Rank Name'] = "Video " + agg_plot['Video Rank'].astype(str)

        fig = px.area(agg_plot, x="Days Since Asset Start", y="Metrics Organic Views", color="Video Rank Name",
                     title=f"Timeline Stacking for {current_vol} Iterations", template="plotly_white")
        fig.update_traces(line=dict(width=0))
        st.plotly_chart(fig, use_container_width=True)

        # 5. Insights Section
        group_summary_txt = f"--- {current_vol} Iterations Group ---\nSample Size: {group_sample_size} assets\n"
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Upload Timing**")
            for r in range(2, current_vol + 1):
                t_str = f"- V{r}: Day {avg_launches[r]:.1f} ({avg_launches[r]-avg_launches[r-1]:.1f} days after V{r-1})"
                st.write(t_str); group_summary_txt += t_str + "\n"

        with col2:
            st.markdown(f"**Decay (Day below {decay_pct}% peak)**")
            decay_results = []
            for vid_id in sub_df['Video data Video ID'].unique():
                d_day = calculate_decay_day(sub_df[sub_df['Video data Video ID'] == vid_id], decay_pct)
                if d_day is not None:
                    decay_results.append({'Rank': sub_df[sub_df['Video data Video ID'] == vid_id]['Video Rank'].iloc[0], 'Day': d_day})
            if decay_results:
                decay_sum = pd.DataFrame(decay_results).groupby('Rank')['Day'].mean()
                for rank, day in decay_sum.items():
                    d_str = f"- Video {int(rank)}: Day {day:.1f}"
                    st.write(d_str); group_summary_txt += d_str + "\n"

        with col3:
            st.markdown("**28-Day Injection Impact**")
            asset_daily_total = sub_df.groupby(['Video data Custom ID', 'Metrics Date Date'])['Metrics Organic Views'].sum().reset_index()
            
            for r in range(2, current_vol + 1):
                total_lifts = []
                total_diffs = []
                for asset_id in sub_df['Video data Custom ID'].unique():
                    launch_date = sub_df[(sub_df['Video data Custom ID'] == asset_id) & (sub_df['Video Rank'] == r)]['Video data Published Date'].min()
                    asset_data = asset_daily_total[asset_daily_total['Video data Custom ID'] == asset_id]
                    
                    before = asset_data[(asset_data['Metrics Date Date'] < launch_date) & 
                                        (asset_data['Metrics Date Date'] >= launch_date - pd.Timedelta(days=28))]['Metrics Organic Views'].sum()
                    after = asset_data[(asset_data['Metrics Date Date'] >= launch_date) & 
                                       (asset_data['Metrics Date Date'] < launch_date + pd.Timedelta(days=28))]['Metrics Organic Views'].sum()
                    
                    diff = after - before
                    total_diffs.append(diff)
                    if before > 0:
                        total_lifts.append((diff / before) * 100)
                
                if total_diffs:
                    avg_lift = np.mean(total_lifts) if total_lifts else 0
                    avg_diff = np.mean(total_diffs)
                    avg_daily = avg_diff / 28
                    
                    # Update format to: +1894.1% (+34.5K views, 1.2K daily views)
                    l_str = f"- Injection V{r}: **{avg_lift:+.1f}%** (+{format_views(avg_diff)} views, {format_views(avg_daily)} daily views)"
                    st.write(l_str); group_summary_txt += l_str.replace('**', '') + "\n"

        # --- PDF Export ---
        pdf.set_font("Arial", 'B', 12); pdf.cell(0, 10, f"Group: {current_vol} Iterations", ln=True)
        pdf.set_font("Arial", size=10)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            fig.write_image(tmp.name)
            pdf.image(tmp.name, x=10, w=180); os.remove(tmp.name)
        pdf.multi_cell(0, 7, group_summary_txt); pdf.ln(10)
        st.divider()

    st.download_button("📥 Download Strategic Report (PDF)", data=pdf.output(dest='S').encode('latin-1'), file_name="YT_Strategic_Report.pdf")
else:
    st.info("👋 Upload a CSV to begin.")
