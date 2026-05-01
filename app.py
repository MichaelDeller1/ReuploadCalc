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
- **Asset**: The core creative content (Custom ID).
- **Decay Day**: The day views stayed **90% below peak** for 5 consecutive days.
- **Net Difference**: Total views 28 days *after* a re-upload vs. 28 days *before*.
""")
st.divider()

def calculate_decay_day(video_df, decay_threshold_pct):
    video_df = video_df.sort_values('Days Since Published')
    if video_df.empty: return None
    
    peak_val = video_df['Organic Views'].max()
    if peak_val <= 0: return None
    
    peak_day = video_df.loc[video_df['Organic Views'].idxmax(), 'Days Since Published']
    # Threshold is 90% BELOW peak (meaning only 10% of peak remains)
    threshold = ((100 - decay_threshold_pct) / 100) * peak_val
    
    post_peak = video_df[video_df['Days Since Published'] >= peak_day].copy()
    post_peak['below_threshold'] = post_peak['Organic Views'] < threshold
    
    # Updated to 5 consecutive days as requested
    post_peak['streak'] = post_peak['below_threshold'].rolling(window=5).sum()
    decay_hit = post_peak[post_peak['streak'] == 5]
    
    if not decay_hit.empty:
        return decay_hit.iloc[0]['Days Since Published'] - 4
    return None

uploaded_file = st.file_uploader("Upload your YouTube CSV Data", type=["csv"])

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    df.columns = [col.strip() for col in df.columns]
    
    views_col, custom_id_col, video_id_col, pub_date_col, metrics_date_col = \
        'Organic Views', 'Custom ID', 'Video ID', 'Published Date', 'Date Date'

    # Data Cleaning
    df[views_col] = pd.to_numeric(df[views_col].astype(str).str.replace(',', ''), errors='coerce')
    df = df.dropna(subset=[views_col])
    df[pub_date_col] = pd.to_datetime(df[pub_date_col])
    df[metrics_date_col] = pd.to_datetime(df[metrics_date_col])

    # Ranking & Timing
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

    # Global Settings
    st.sidebar.header("Global Settings")
    max_timeline = st.sidebar.slider("Timeline Window (Days)", 30, 1500, 700)
    decay_pct = 90 # Fixed at 90% as per request

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(200, 10, "YouTube Strategic Iteration & Lift Report", ln=True, align='C')

    unique_vols = sorted([v for v in df['Total_Videos'].unique() if v > 1])

    for current_vol in unique_vols:
        sub_df = df[df['Total_Videos'] == current_vol]
        group_sample_size = sub_df[custom_id_col].nunique()
        
        st.header(f"📈 Assets with {current_vol} Iterations (n={group_sample_size})")
        
        # --- LIFT CALCULATION (28 Day Windows) ---
        lift_data = []
        for asset_id in sub_df[custom_id_col].unique():
            asset_data = sub_df[sub_df[custom_id_col] == asset_id]
            for r in range(2, int(current_vol) + 1):
                v_prev = asset_data[asset_data['Video Rank'] == r-1]
                v_curr = asset_data[asset_data['Video Rank'] == r]
                
                launch_date = v_curr[pub_date_col].iloc[0]
                
                # Windows
                pre_start, pre_end = launch_date - pd.Timedelta(days=28), launch_date - pd.Timedelta(days=1)
                post_start, post_end = launch_date, launch_date + pd.Timedelta(days=27)
                
                views_pre_old = v_prev[(v_prev[metrics_date_col] >= pre_start) & (v_prev[metrics_date_col] <= pre_end)][views_col].sum()
                views_post_old = v_prev[(v_prev[metrics_date_col] >= post_start) & (v_prev[metrics_date_col] <= post_end)][views_col].sum()
                views_post_new = v_curr[(v_curr[metrics_date_col] >= post_start) & (v_curr[metrics_date_col] <= post_end)][views_col].sum()
                
                if views_pre_old > 0:
                    old_v_change = ((views_post_old - views_pre_old) / views_pre_old) * 100
                    total_lift = (( (views_post_old + views_post_new) - views_pre_old) / views_pre_old) * 100
                    lift_data.append({'Rank': r, 'Old_Change': old_v_change, 'Net_Lift': total_lift})

        # Plotting & Aggregation
        ranks = range(1, int(current_vol) + 1)
        timeline = range(0, max_timeline + 1)
        template = pd.MultiIndex.from_product([ranks, timeline], names=['Video Rank', 'Days Since Asset Start']).to_frame(index=False)
        agg_actual = sub_df.groupby(['Video Rank', 'Days Since Asset Start'])[views_col].mean().reset_index()
        agg_plot = template.merge(agg_actual, on=['Video Rank', 'Days Since Asset Start'], how='left').fillna(0)
        
        avg_launches = video_info[video_info['Total_Videos'] == current_vol].groupby('Video Rank')['Days_From_Start'].mean()
        for r, start_day in avg_launches.items():
            agg_plot.loc[(agg_plot['Video Rank'] == r) & (agg_plot['Days Since Asset Start'] < start_day), views_col] = 0

        agg_plot['Video Rank Name'] = "Video " + agg_plot['Video Rank'].astype(str)
        fig = px.area(agg_plot, x="Days Since Asset Start", y=views_col, color="Video Rank Name", template="plotly_white")
        fig.update_traces(line=dict(width=0))
        st.plotly_chart(fig, use_container_width=True)

        # UI Tables
        col1, col2 = st.columns(2)
        group_summary_txt = f"Analysis for {current_vol} Iterations\n"

        with col1:
            st.subheader("⏱️ Timing & Decay")
            for r in range(1, int(current_vol) + 1):
                # Calculate avg decay for this rank
                d_days = [calculate_decay_day(sub_df[sub_df[video_id_col] == vid], 90) for vid in sub_df[sub_df['Video Rank']==r][video_id_col].unique()]
                avg_d = np.nanmean([d for d in d_days if d is not None])
                d_str = f"Video {r}: Avg Decay Day {avg_d:.1f}"
                st.write(d_str)
                group_summary_txt += d_str + "\n"

        with col2:
            st.subheader("🚀 28-Day Impact")
            if lift_data:
                lift_df = pd.DataFrame(lift_data).groupby('Rank').mean()
                for rank, row in lift_df.iterrows():
                    l_str = f"V{int(rank)} Launch: V{int(rank-1)} changed {row['Old_Change']:.1f}%, Net Asset Lift: {row['Net_Lift']:.1f}%"
                    st.write(l_str)
                    group_summary_txt += l_str + "\n"

        # PDF Export
        pdf.set_font("Arial", 'B', 12)
        pdf.cell(0, 10, f"Group: {current_vol} Iterations", ln=True)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            fig.write_image(tmp.name)
            pdf.image(tmp.name, x=10, w=180)
            os.remove(tmp.name)
        pdf.set_font("Arial", size=10)
        pdf.multi_cell(0, 7, group_summary_txt)
        pdf.ln(5)

    st.download_button("📥 Download Strategic Report", data=pdf.output(dest='S').encode('latin-1', 'replace'), file_name="Strategic_Report.pdf")
