import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np
from fpdf import FPDF

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

def create_pdf(full_report_text, counts):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(200, 10, "YouTube Strategic Asset Report", ln=True, align='C')
    pdf.ln(10)
    
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(200, 10, "Global Dataset Summary:", ln=True)
    pdf.set_font("Arial", size=11)
    pdf.cell(200, 10, f"- Total Channels: {counts['channels']}", ln=True)
    pdf.cell(200, 10, f"- Total Unique Videos: {counts['videos']}", ln=True)
    pdf.cell(200, 10, f"- Total Assets (Custom IDs): {counts['assets']}", ln=True)
    
    pdf.ln(10)
    pdf.set_font("Arial", size=10)
    pdf.multi_cell(0, 7, full_report_text)
    
    return pdf.output(dest='S').encode('latin-1')

uploaded_file = st.file_uploader("Upload your YouTube CSV Data", type=["csv"])

if uploaded_file is not None:
    # 1. Processing Data
    df = pd.read_csv(uploaded_file)
    df.columns = [col.replace('\n', '').strip() for col in df.columns]
    
    df['Metrics Organic Views'] = pd.to_numeric(df['Metrics Organic Views'].astype(str).str.replace(',', ''), errors='coerce')
    df = df.dropna(subset=['Metrics Organic Views'])
    df['Video data Published Date'] = pd.to_datetime(df['Video data Published Date'])
    df['Metrics Date Date'] = pd.to_datetime(df['Metrics Date Date'])

    # --- TOP LEVEL SUMMARY ---
    counts_dict = {
        'channels': df['Channel data Channel Name'].nunique(),
        'videos': df['Video data Video ID'].nunique(),
        'assets': df['Video data Custom ID'].nunique()
    }

    st.markdown("### 🌍 Global Dataset Overview")
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Channels", f"{counts_dict['channels']:,}")
    m2.metric("Total Unique Videos", f"{counts_dict['videos']:,}")
    m3.metric("Total Assets (Custom IDs)", f"{counts_dict['assets']:,}")
    st.divider()

    # 2. Iteration Logic
    video_info = df[['Video data Custom ID', 'Video data Video ID', 'Video data Published Date']].drop_duplicates()
    video_info = video_info.sort_values(by=['Video data Custom ID', 'Video data Published Date'])
    video_info['Video Rank'] = video_info.groupby('Video data Custom ID').cumcount() + 1
    
    vol_counts = video_info.groupby('Video data Custom ID')['Video data Video ID'].count().reset_index(name='Total_Videos')
    start_dates = video_info.groupby('Video data Custom ID')['Video data Published Date'].min().reset_index(name='Asset_Day_0')
    
    video_info = video_info.merge(start_dates, on='Video data Custom ID').merge(vol_counts, on='Video data Custom ID')
    video_info['Days_From_Start'] = (video_info['Video data Published Date'] - video_info['Asset_Day_0']).dt.days
    
    df = df.merge(video_info[['Video data Video ID', 'Video Rank', 'Total_Videos', 'Asset_Day_0', 'Days_From_Start']], on='Video data Video ID')
    df['Days Since Asset Start'] = (df['Metrics Date Date'] - df['Asset_Day_0']).dt.days
    df['Days Since Published'] = (df['Metrics Date Date'] - df['Video data Published Date']).dt.days

    # 3. Sidebar Global Controls
    st.sidebar.header("Global Settings")
    max_timeline = st.sidebar.slider("Timeline Window (Days)", 30, 1500, 700)
    decay_pct = st.sidebar.slider("Burn-off Threshold (% of Peak)", 10, 95, 90)

    # 4. Generate All Charts
    full_report_text = ""
    
    # Filter for groups with more than 1 video to show meaningful iteration analysis
    unique_vols = sorted([v for v in df['Total_Videos'].unique() if v > 1])
    
    for current_vol in unique_vols:
        sub_df = df[df['Total_Videos'] == current_vol]
        group_sample_size = sub_df['Video data Custom ID'].nunique()
        
        st.subheader(f"📈 Assets with {current_vol} Iterations (Sample: {group_sample_size} assets)")
        
        # --- ZERO PADDING LOGIC ---
        # We need a entry for every rank at every day from 0 to max_timeline
        ranks = range(1, current_vol + 1)
        timeline = range(0, max_timeline + 1)
        
        # Create a template of all possible Rank/Day combinations
        template = pd.MultiIndex.from_product([ranks, timeline], names=['Video Rank', 'Days Since Asset Start']).to_frame(index=False)
        
        # Aggregate actual data
        agg_actual = sub_df.groupby(['Video Rank', 'Days Since Asset Start'])['Metrics Organic Views'].mean().reset_index()
        
        # Merge actual data into template and fill missing with 0
        agg_plot = template.merge(agg_actual, on=['Video Rank', 'Days Since Asset Start'], how='left').fillna(0)
        
        # Ensure we don't show "future" views (views before the average upload day for that rank)
        # Calculate average launch day per rank for this group
        avg_launches = video_info[video_info['Total_Videos'] == current_vol].groupby('Video Rank')['Days_From_Start'].mean()
        for r, start_day in avg_launches.items():
            agg_plot.loc[(agg_plot['Video Rank'] == r) & (agg_plot['Days Since Asset Start'] < start_day), 'Metrics Organic Views'] = 0

        agg_plot['Video Rank Name'] = "Video " + agg_plot['Video Rank'].astype(str)

        # Plot
        fig = px.area(agg_plot, x="Days Since Asset Start", y="Metrics Organic Views", color="Video Rank Name",
                     labels={"Metrics Organic Views": "Avg Daily Views", "Days Since Asset Start": "Days Since Original Upload"},
                     template="plotly_white")
        st.plotly_chart(fig, use_container_width=True)

        # Insights for this group
        group_text = f"\n--- {current_vol} ITERATIONS GROUP ---\n"
        group_text += f"Sample Size: {group_sample_size} assets\n"
        
        col_in1, col_in2 = st.columns(2)
        with col_in1:
            st.write("**Upload Timing**")
            for r in range(2, current_vol + 1):
                gap_v1 = avg_launches[r]
                gap_prev = avg_launches[r] - avg_launches[r-1]
                t_str = f"- Video {r}: Day {gap_v1:.1f} ({gap_prev:.1f} days after Video {r-1})"
                st.write(t_str)
                group_text += t_str + "\n"

        with col_in2:
            st.write(f"**Decay (Day below {decay_pct}% peak)**")
            decay_list = []
            for vid_id in sub_df['Video data Video ID'].unique():
                d_day = calculate_decay_day(sub_df[sub_df['Video data Video ID'] == vid_id], decay_pct)
                if d_day is not None:
                    rank = sub_df[sub_df['Video data Video ID'] == vid_id]['Video Rank'].iloc[0]
                    decay_list.append({'Rank': rank, 'Day': d_day})
            
            if decay_list:
                decay_summary = pd.DataFrame(decay_list).groupby('Rank')['Day'].mean()
                for rank, day in decay_summary.items():
                    d_str = f"- Video {int(rank)}: Day {day:.1f}"
                    st.write(d_str)
                    group_text += d_str + "\n"
        
        full_report_text += group_text
        st.divider()

    # 5. PDF Export
    pdf_data = create_pdf(full_report_text, counts_dict)
    st.download_button("📥 Download All Insights as PDF", data=pdf_data, file_name="YT_Asset_Full_Report.pdf", mime="application/pdf")
else:
    st.info("👋 Please upload your CSV to generate all iteration charts.")
