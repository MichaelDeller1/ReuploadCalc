import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from fpdf import FPDF
import base64

# --- Page Configuration ---
st.set_page_config(page_title="YT Asset Strategic Analysis", layout="wide")

st.title("Strategic Asset Lifecycle & Decay Analysis")

def calculate_decay_day(video_df, decay_threshold_pct):
    """Finds the first day of a 3-day streak below X% of peak views."""
    video_df = video_df.sort_values('Days Since Published')
    if video_df.empty: return None
    
    peak_val = video_df['Metrics Organic Views'].max()
    if peak_val <= 0: return None
    
    peak_row = video_df.loc[video_df['Metrics Organic Views'].idxmax()]
    peak_day = peak_row['Days Since Published']
    threshold = (decay_threshold_pct / 100) * peak_val
    
    post_peak = video_df[video_df['Days Since Published'] >= peak_day].copy()
    post_peak['below_threshold'] = post_peak['Metrics Organic Views'] < threshold
    
    post_peak['streak'] = post_peak['below_threshold'].rolling(window=3).sum()
    decay_hit = post_peak[post_peak['streak'] == 3]
    
    if not decay_hit.empty:
        return decay_hit.iloc[0]['Days Since Published'] - 2
    return None

def create_pdf(summary_text, sample_size, vol):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(200, 10, f"YouTube Asset Analysis: {vol} Iterations", ln=True, align='C')
    pdf.set_font("Arial", size=12)
    pdf.ln(10)
    pdf.cell(200, 10, f"Sample Size: {sample_size} unique assets", ln=True)
    pdf.ln(5)
    pdf.multi_cell(0, 10, summary_text)
    return pdf.output(dest='S').encode('latin-1')

uploaded_file = st.file_uploader("Upload your YouTube CSV Data", type=["csv"])

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    df.columns = [col.replace('\n', '').strip() for col in df.columns]
    df['Metrics Organic Views'] = pd.to_numeric(df['Metrics Organic Views'].astype(str).str.replace(',', ''), errors='coerce')
    df = df.dropna(subset=['Metrics Organic Views'])
    df['Video data Published Date'] = pd.to_datetime(df['Video data Published Date'])
    df['Metrics Date Date'] = pd.to_datetime(df['Metrics Date Date'])

    # 1. Logic: Ranking & Gaps
    video_info = df[['Video data Custom ID', 'Video data Video ID', 'Video data Published Date']].drop_duplicates()
    video_info = video_info.sort_values(by=['Video data Custom ID', 'Video data Published Date'])
    video_info['Video Rank'] = video_info.groupby('Video data Custom ID').cumcount() + 1
    
    vol = video_info.groupby('Video data Custom ID')['Video data Video ID'].count().reset_index(name='Total_Videos')
    start_dates = video_info.groupby('Video data Custom ID')['Video data Published Date'].min().reset_index(name='Asset_Day_0')
    
    video_info = video_info.merge(start_dates, on='Video data Custom ID').merge(vol, on='Video data Custom ID')
    video_info['Days_From_Start'] = (video_info['Video data Published Date'] - video_info['Asset_Day_0']).dt.days
    
    df = df.merge(video_info[['Video data Video ID', 'Video Rank', 'Total_Videos', 'Asset_Day_0', 'Days_From_Start']], on='Video data Video ID')
    df['Days Since Asset Start'] = (df['Metrics Date Date'] - df['Asset_Day_0']).dt.days
    df['Days Since Published'] = (df['Metrics Date Date'] - df['Video data Published Date']).dt.days

    # 2. Sidebar Controls
    st.sidebar.header("Analysis Parameters")
    selected_vol = st.sidebar.selectbox("Filter by Iteration Count", sorted(df['Total_Videos'].unique()), index=2)
    max_timeline = st.sidebar.slider("Timeline Window (Days)", 30, 1500, 1200)
    decay_pct = st.sidebar.slider("Burn-off Threshold (% of Peak)", 10, 95, 90)

    # 3. Data Subsetting
    sub_df = df[df['Total_Videos'] == selected_vol]
    sample_size = sub_df['Video data Custom ID'].nunique()

    # FIX: Aligning the timeline for stacking
    # We ensure each video only has data from its 'Days_From_Start' onwards
    sub_df_aligned = sub_df[sub_df['Days Since Asset Start'] >= sub_df['Days_From_Start']]
    
    agg_mean = sub_df_aligned.groupby(['Video Rank', 'Days Since Asset Start'])['Metrics Organic Views'].mean().reset_index()
    agg_mean['Video Rank'] = "Video " + agg_mean['Video Rank'].astype(str)

    # 4. Main Chart
    st.subheader(f"True Timeline Stacking: Assets with {selected_vol} Iterations")
    fig = px.area(agg_mean[agg_mean['Days Since Asset Start'] <= max_timeline], 
                 x="Days Since Asset Start", y="Metrics Organic Views", color="Video Rank",
                 labels={"Metrics Organic Views": "Avg Daily Views", "Days Since Asset Start": "Days Since Original Upload"},
                 template="plotly_white")
    st.plotly_chart(fig, use_container_width=True)

    # 5. Strategic Insights
    st.markdown("### 📊 Strategic Data Insights")
    report_text = f"Analysis for assets with {selected_vol} iterations.\n"
    
    st.markdown(f"* **Sample Size:** This analysis represents **{sample_size} unique assets**.")
    
    # Timing
    timing_info = video_info[video_info['Total_Videos'] == selected_vol].pivot(index='Video data Custom ID', columns='Video Rank', values='Days_From_Start')
    
    for r in range(2, selected_vol + 1):
        gap = timing_info[r].mean()
        prev_gap = (timing_info[r] - timing_info[r-1]).mean()
        txt = f"* **Upload Timing:** Video {r} was posted on average **{gap:.1f} days** after Video 1 ({prev_gap:.1f} days after Video {r-1})."
        st.markdown(txt)
        report_text += txt.replace('* ', '') + "\n"

    # Decay
    st.markdown(f"**Burn-off Rates (Time until 3 days below {decay_pct}% of peak):**")
    decay_list = []
    for vid_id in sub_df['Video data Video ID'].unique():
        rank = sub_df[sub_df['Video data Video ID'] == vid_id]['Video Rank'].iloc[0]
        day = calculate_decay_day(sub_df[sub_df['Video data Video ID'] == vid_id], decay_pct)
        if day is not None: decay_list.append({'Rank': rank, 'Day': day})
    
    if decay_list:
        decay_summary = pd.DataFrame(decay_list).groupby('Rank')['Day'].mean()
        for rank, day in decay_summary.items():
            txt = f"- **Video {int(rank)}**: Momentum fades on **Day {day:.1f}** post-upload."
            st.write(txt)
            report_text += txt + "\n"

    # PDF Download
    st.divider()
    pdf_data = create_pdf(report_text, sample_size, selected_vol)
    st.download_button(label="📥 Download Report as PDF", 
                       data=pdf_data, 
                       file_name=f"Asset_Analysis_{selected_vol}_Iter.pdf", 
                       mime="application/pdf")
else:
    st.info("Please upload your YouTube performance CSV to begin.")
