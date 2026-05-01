import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from fpdf import FPDF
import tempfile
import os

# --- Page Configuration ---
st.set_page_config(page_title="YT Asset Strategic Analysis", layout="wide")

st.title("Strategic Asset Lifecycle & Decay Analysis")

# 1. Static Glossary
st.markdown("""
### 📖 Glossary
- **Asset**: The core creative content, identified by the **Custom ID**. 
- **Iteration**: Each individual upload of an asset. **Video 1** is the baseline, while **Video 2, 3, etc.** are subsequent versions.
""")
st.divider()

def calculate_decay_day(video_df, decay_threshold_pct):
    video_df = video_df.sort_values('Days Since Published')
    if video_df.empty: return None
    
    peak_val = video_df['Organic Views'].max()
    if peak_val <= 0: return None
    
    peak_day = video_df.loc[video_df['Organic Views'].idxmax(), 'Days Since Published']
    threshold = (decay_threshold_pct / 100) * peak_val
    
    post_peak = video_df[video_df['Days Since Published'] >= peak_day].copy()
    post_peak['below_threshold'] = post_peak['Organic Views'] < threshold
    
    # Require 3 consecutive days below threshold to confirm "decay"
    post_peak['streak'] = post_peak['below_threshold'].rolling(window=3).sum()
    decay_hit = post_peak[post_peak['streak'] == 3]
    
    if not decay_hit.empty:
        return decay_hit.iloc[0]['Days Since Published'] - 2
    return None

uploaded_file = st.file_uploader("Upload your YouTube CSV Data", type=["csv"])

if uploaded_file is not None:
    # 2. Data Processing
    df = pd.read_csv(uploaded_file)
    
    # Clean whitespace from headers
    df.columns = [col.strip() for col in df.columns]
    
    # Map the specific columns from your screenshot
    views_col = 'Organic Views'
    custom_id_col = 'Custom ID'
    video_id_col = 'Video ID'
    pub_date_col = 'Published Date'
    metrics_date_col = 'Date Date'

    # Validate Columns exist
    required_cols = [views_col, custom_id_col, video_id_col, pub_date_col, metrics_date_col]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.error(f"Missing columns: {missing}. Please check your CSV headers.")
        st.stop()

    # Clean Metrics
    df[views_col] = pd.to_numeric(df[views_col].astype(str).str.replace(',', ''), errors='coerce')
    df = df.dropna(subset=[views_col])
    df[pub_date_col] = pd.to_datetime(df[pub_date_col])
    df[metrics_date_col] = pd.to_datetime(df[metrics_date_col])

    # Ranking & Timing Logic
    video_info = df[[custom_id_col, video_id_col, pub_date_col]].drop_duplicates()
    video_info = video_info.sort_values(by=[custom_id_col, pub_date_col])
    
    # Calculate iteration rank (Video 1, Video 2...)
    video_info['Video Rank'] = video_info.groupby(custom_id_col).cumcount() + 1
    
    # Metadata for grouping
    vols = video_info.groupby(custom_id_col)[video_id_col].count().reset_index(name='Total_Videos')
    starts = video_info.groupby(custom_id_col)[pub_date_col].min().reset_index(name='Asset_Day_0')
    
    video_info = video_info.merge(starts, on=custom_id_col).merge(vols, on=custom_id_col)
    video_info['Days_From_Start'] = (video_info[pub_date_col] - video_info['Asset_Day_0']).dt.days
    
    # Merge timing back to master DF
    df = df.merge(video_info[[video_id_col, 'Video Rank', 'Total_Videos', 'Asset_Day_0', 'Days_From_Start']], on=video_id_col)
    df['Days Since Asset Start'] = (df[metrics_date_col] - df['Asset_Day_0']).dt.days
    df['Days Since Published'] = (df[metrics_date_col] - df[pub_date_col]).dt.days

    # 3. Sidebar
    st.sidebar.header("Global Settings")
    max_timeline = st.sidebar.slider("Timeline Window (Days)", 30, 1500, 700)
    decay_pct = st.sidebar.slider("Burn-off Threshold (% of Peak)", 10, 95, 20) # Usually low threshold for decay

    # 4. PDF Setup
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(200, 10, "YouTube Strategic Iteration Report", ln=True, align='C')
    pdf.ln(10)

    # Filter for assets that have at least one re-upload
    unique_vols = sorted([v for v in df['Total_Videos'].unique() if v > 1])

    for current_vol in unique_vols:
        sub_df = df[df['Total_Videos'] == current_vol]
        group_sample_size = sub_df[custom_id_col].nunique()
        
        st.header(f"📈 Assets with {current_vol} Iterations (Sample: {group_sample_size})")
        
        # Zero-Padding & Alignment Logic
        ranks = range(1, int(current_vol) + 1)
        timeline = range(0, max_timeline + 1)
        template = pd.MultiIndex.from_product([ranks, timeline], names=['Video Rank', 'Days Since Asset Start']).to_frame(index=False)
        
        agg_actual = sub_df.groupby(['Video Rank', 'Days Since Asset Start'])[views_col].mean().reset_index()
        agg_plot = template.merge(agg_actual, on=['Video Rank', 'Days Since Asset Start'], how='left').fillna(0)
        
        # Hide views before the average launch date for that rank
        avg_launches = video_info[video_info['Total_Videos'] == current_vol].groupby('Video Rank')['Days_From_Start'].mean()
        for r, start_day in avg_launches.items():
            agg_plot.loc[(agg_plot['Video Rank'] == r) & (agg_plot['Days Since Asset Start'] < start_day), views_col] = 0

        agg_plot['Video Rank Name'] = "Video " + agg_plot['Video Rank'].astype(str)

        # Plotly Figure
        fig = px.area(agg_plot, x="Days Since Asset Start", y=views_col, color="Video Rank Name",
                      title=f"Timeline Stacking for {current_vol} Iterations", 
                      template="plotly_white",
                      color_discrete_sequence=px.colors.qualitative.Safe)
        
        # UI Clean up: Remove line borders from the area chart
        fig.update_traces(line=dict(width=0))
        st.plotly_chart(fig, use_container_width=True)

        # Build Insights Section
        col_in1, col_in2 = st.columns(2)
        group_summary_txt = f"--- {current_vol} Iterations Group ---\nSample Size: {group_sample_size} assets\n"
        
        with col_in1:
            st.markdown("**Upload Timing**")
            for r in range(2, int(current_vol) + 1):
                t_str = f"- Video {r}: Avg Day {avg_launches[r]:.1f} ({avg_launches[r]-avg_launches[r-1]:.1f} days after V{r-1})"
                st.write(t_str)
                group_summary_txt += t_str + "\n"

        with col_in2:
            st.markdown(f"**Decay (Day below {decay_pct}% peak)**")
            decay_results = []
            for vid_id in sub_df[video_id_col].unique():
                d_day = calculate_decay_day(sub_df[sub_df[video_id_col] == vid_id], decay_pct)
                if d_day is not None:
                    v_rank = sub_df[sub_df[video_id_col] == vid_id]['Video Rank'].iloc[0]
                    decay_results.append({'Rank': v_rank, 'Day': d_day})
            
            if decay_results:
                decay_sum = pd.DataFrame(decay_results).groupby('Rank')['Day'].mean()
                for rank, day in decay_sum.items():
                    d_str = f"- Video {int(rank)}: Day {day:.1f}"
                    st.write(d_str)
                    group_summary_txt += d_str + "\n"

        # --- ADD CHART AND TEXT TO PDF ---
        pdf.set_font("Arial", 'B', 12)
        pdf.cell(0, 10, f"Group: {current_vol} Iterations", ln=True)
        pdf.set_font("Arial", size=10)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmpfile:
            fig.write_image(tmpfile.name)
            pdf.image(tmpfile.name, x=10, w=180)
            os.remove(tmpfile.name)
            
        pdf.multi_cell(0, 7, group_summary_txt)
        pdf.ln(10)
        st.divider()

    # Final Download
    # Note: .encode('latin-1', 'replace') handles odd characters that might be in titles
    try:
        report_data = pdf.output(dest='S').encode('latin-1', 'replace')
        st.download_button("📥 Download Full Strategic Report (PDF)", 
                           data=report_data, 
                           file_name="YT_Iteration_Report.pdf", 
                           mime="application/pdf")
    except Exception as e:
        st.error(f"Error generating PDF: {e}")

else:
    st.info("👋 Upload a CSV with 'Custom ID', 'Video ID', 'Published Date', 'Date Date', and 'Organic Views' to start.")
