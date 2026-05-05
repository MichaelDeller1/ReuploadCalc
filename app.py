import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from fpdf import FPDF
import tempfile
import os

# --- Page Configuration ---
st.set_page_config(page_title="YT Asset Strategic Analysis", layout="wide")

st.title("Strategic Asset Lifecycle & Lift Analysis")

# 1. Static Glossary
st.markdown("""
### 📖 Glossary
- **Asset**: The core creative content (identified by Custom ID).
- **Decay Day**: The day a video's views drop below your threshold for a sustained period.
- **Net Lift**: The total performance change of the asset (Old + New video) compared to the old video's baseline.
""")
st.divider()

def calculate_decay_day(video_df, views_col, decay_threshold_pct, streak_days):
    video_df = video_df.sort_values('Days Since Published')
    if video_df.empty: return None
    
    peak_val = video_df[views_col].max()
    if peak_val <= 0: return None
    
    peak_day = video_df.loc[video_df[views_col].idxmax(), 'Days Since Published']
    
    # Threshold logic: views < (100 - decay_pct)% of peak
    threshold = ((100 - decay_threshold_pct) / 100) * peak_val
    
    post_peak = video_df[video_df['Days Since Published'] >= peak_day].copy()
    post_peak['below_threshold'] = post_peak[views_col] < threshold
    
    post_peak['streak'] = post_peak['below_threshold'].rolling(window=streak_days).sum()
    decay_hit = post_peak[post_peak['streak'] == streak_days]
    
    if not decay_hit.empty:
        return decay_hit.iloc[0]['Days Since Published'] - (streak_days - 1)
    return None

uploaded_file = st.file_uploader("Upload your YouTube CSV Data", type=["csv"])

if uploaded_file is not None:
    # --- Sidebar Controls ---
    st.sidebar.header("Analysis Parameters")
    decay_pct_input = st.sidebar.slider("Decay Threshold (% drop from peak)", 10, 99, 90)
    streak_input = st.sidebar.slider("Consecutive Days to Confirm Decay", 1, 14, 5)
    window_input = st.sidebar.slider("Comparison Window (Days)", 7, 90, 28)
    max_timeline = st.sidebar.slider("Timeline Window (Days)", 30, 1500, 700)

    # 2. Data Processing
    df = pd.read_csv(uploaded_file)
    df.columns = [col.strip() for col in df.columns]
    
    # MAPPING TO YOUR ACTUAL CSV HEADERS
    views_col = 'Organic Views'
    custom_id_col = 'Custom ID'
    video_id_col = 'Video ID'
    pub_date_col = 'Published Date'
    metrics_date_col = 'Date Date'

    # Safety check for missing columns
    required = [views_col, custom_id_col, video_id_col, pub_date_col, metrics_date_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        st.error(f"Missing columns in CSV: {missing}. Please check your headers.")
        st.stop()

    df[views_col] = pd.to_numeric(df[views_col].astype(str).str.replace(',', ''), errors='coerce')
    df = df.dropna(subset=[views_col])
    df[pub_date_col] = pd.to_datetime(df[pub_date_col])
    df[metrics_date_col] = pd.to_datetime(df[metrics_date_col])

    # --- TOP LEVEL OVERVIEW ---
    total_assets = df[custom_id_col].nunique()
    total_videos = df[video_id_col].nunique()
    
    st.subheader("📊 Global Asset Overview")
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Unique Assets", f"{total_assets:,}")
    m2.metric("Total Video Uploads", f"{total_videos:,}")
    m3.metric("Avg Iterations per Asset", f"{total_videos/total_assets:.2f}" if total_assets > 0 else "0")
    st.divider()

    # Ranking & Timing Logic
    video_info = df[[custom_id_col, video_id_col, pub_date_col]].drop_duplicates()
    video_info = video_info.sort_values(by=[custom_id_col, pub_date_col])
    video_info['Video Rank'] = video_info.groupby(custom_id_col).cumcount() + 1
    
    vols = video_info.groupby(custom_id_col)[video_id_col].count().reset_index(name='Total_Videos')
    starts = video_info.groupby(custom_id_col)[pub_date_col].min().reset_index(name='Asset_Day_0')
    
    video_info = video_info.merge(starts, on=custom_id_col).merge(vols, on=custom_id_col)
    video_info['Days_From_Start'] = (video_info[pub_date_col] - video_info['Asset_Day_0']).dt.days
    
    df = df.merge(video_info[[video_id_col, 'Video Rank', 'Total_Videos', 'Asset_Day_0', 'Days_From_Start']], on=video_id_col)
    df['Days Since Asset Start'] = (df[metrics_date_col] - df['Asset_Day_0']).dt.days
    df['Days Since Published'] = (df[metrics_date_col] - df[pub_date_col]).dt.days

    # 3. PDF Initialization
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(200, 10, "YouTube Strategic Iteration & Lift Report", ln=True, align='C')
    pdf.set_font("Arial", size=10)
    pdf.cell(200, 10, f"Total Assets: {total_assets} | Total Videos: {total_videos}", ln=True, align='C')

    unique_vols = sorted([v for v in df['Total_Videos'].unique() if v > 1])

    for current_vol in unique_vols:
        sub_df = df[df['Total_Videos'] == current_vol]
        group_sample_size = sub_df[custom_id_col].nunique()
        
        st.header(f"📈 {group_sample_size} Assets With {current_vol} Iterations")
        
        # --- LIFT CALCULATION ---
        lift_data = []
        for asset_id in sub_df[custom_id_col].unique():
            asset_data = sub_df[sub_df[custom_id_col] == asset_id]
            for r in range(2, int(current_vol) + 1):
                v_prev = asset_data[asset_data['Video Rank'] == r-1]
                v_curr = asset_data[asset_data['Video Rank'] == r]
                
                if not v_curr.empty and not v_prev.empty:
                    launch_date = v_curr[pub_date_col].iloc[0]
                    pre_start = launch_date - pd.Timedelta(days=window_input)
                    pre_end = launch_date - pd.Timedelta(days=1)
                    post_start = launch_date
                    post_end = launch_date + pd.Timedelta(days=window_input-1)
                    
                    views_pre_old = v_prev[(v_prev[metrics_date_col] >= pre_start) & (v_prev[metrics_date_col] <= pre_end)][views_col].sum()
                    views_post_old = v_prev[(v_prev[metrics_date_col] >= post_start) & (v_prev[metrics_date_col] <= post_end)][views_col].sum()
                    views_post_new = v_curr[(v_curr[metrics_date_col] >= post_start) & (v_curr[metrics_date_col] <= post_end)][views_col].sum()
                    
                    if views_pre_old > 0:
                        old_v_change = ((views_post_old - views_pre_old) / views_pre_old) * 100
                        total_lift = (((views_post_old + views_post_new) - views_pre_old) / views_pre_old) * 100
                        lift_data.append({'Rank': r, 'Old_Change': old_v_change, 'Net_Lift': total_lift})

        # --- Plotting ---
        agg_plot = sub_df.groupby(['Video Rank', 'Days Since Asset Start'])[views_col].mean().reset_index()
        agg_plot = agg_plot[agg_plot['Days Since Asset Start'] <= max_timeline]
        agg_plot['Video Rank Name'] = "Video " + agg_plot['Video Rank'].astype(str)
        agg_plot = agg_plot.sort_values(['Days Since Asset Start', 'Video Rank'])

        fig = px.area(
            agg_plot, x="Days Since Asset Start", y=views_col, color="Video Rank Name",
            template="plotly_white",
            category_orders={"Video Rank Name": [f"Video {i}" for i in range(1, 15)]},
            labels={views_col: "Views", "Days Since Asset Start": "Day"}
        )
        fig.update_layout(hovermode="x unified", hoverlabel=dict(bgcolor="white", font_size=12))
        fig.update_traces(line=dict(width=0.5))
        st.plotly_chart(fig, width='stretch')

        # --- UI Insights ---
        col1, col2 = st.columns(2)
        group_summary_txt = f"{group_sample_size} Assets With {current_vol} Iterations\n"

        with col1:
            st.subheader("⏱️ Timing & Decay")
            for r in range(1, int(current_vol) + 1):
                vids = sub_df[sub_df['Video Rank'] == r][video_id_col].unique()
                d_days = [calculate_decay_day(sub_df[sub_df[video_id_col] == v], views_col, decay_pct_input, streak_input) for v in vids]
                valid_d = [d for d in d_days if d is not None]
                avg_d = np.mean(valid_d) if valid_d else 0
                d_str = f"Video {r}: Avg Decay Day {avg_d:.1f}"
                st.write(d_str)
                group_summary_txt += d_str + "\n"

        with col2:
            st.subheader(f"🚀 {window_input}-Day Impact")
            if lift_data:
                lift_df = pd.DataFrame(lift_data).groupby('Rank').mean()
                for rank, row in lift_df.iterrows():
                    l_str = f"V{int(rank)} Launch: V{int(rank-1)} changed {row['Old_Change']:.1f}%, Net Asset Lift: {row['Net_Lift']:.1f}%"
                    st.write(l_str)
                    group_summary_txt += l_str + "\n"

        # PDF Logic
        pdf.set_font("Arial", 'B', 12)
        pdf.cell(0, 10, f"Group: {group_sample_size} Assets With {current_vol} Iterations", ln=True)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            fig.write_image(tmp.name)
            pdf.image(tmp.name, x=10, w=180)
            os.remove(tmp.name)
        pdf.set_font("Arial", size=10)
        pdf.multi_cell(0, 7, group_summary_txt)
        pdf.ln(5)

    st.download_button("📥 Download Strategic Report", data=pdf.output(dest='S').encode('latin-1', 'replace'), file_name="YT_Strategic_Analysis.pdf")
else:
    st.info("👋 Upload your YouTube CSV to begin.")
