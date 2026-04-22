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
- **Asset**: The core creative content, identified in this data by a unique **Custom ID**. 
- **Iteration**: Each individual upload of an asset. **Video 1** is the original baseline, while **Video 2, 3, etc.** are re-uploads.
- **28-Day Injection Impact**: Measures the % increase in total asset viewership in the 28 days *after* an iteration is posted compared to the 28 days *before* it was posted.
""")
st.divider()

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

uploaded_file = st.file_uploader("Upload your YouTube CSV Data", type=["csv"])

if uploaded_file is not None:
    # 2. Data Processing
    df = pd.read_csv(uploaded_file)
    df.columns = [col.replace('\n', '').strip() for col in df.columns]
    df['Metrics Organic Views'] = pd.to_numeric(df['Metrics Organic Views'].astype(str).str.replace(',', ''), errors='coerce')
    df = df.dropna(subset=['Metrics Organic Views'])
    df['Video data Published Date'] = pd.to_datetime(df['Video data Published Date'])
    df['Metrics Date Date'] = pd.to_datetime(df['Metrics Date Date'])

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
        
        # Plot Logic
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

        # 5. Injection Impact & Insights
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
            # Calculate total views across all videos for each Custom ID per day
            asset_daily_total = sub_df.groupby(['Video data Custom ID', 'Metrics Date Date'])['Metrics Organic Views'].sum().reset_index()
            
            for r in range(2, current_vol + 1):
                # Calculate avg impact across all assets in group
                total_lift_pct = []
                for asset_id in sub_df['Video data Custom ID'].unique():
                    # Get launch date of Video R
                    launch_date = sub_df[(sub_df['Video data Custom ID'] == asset_id) & (sub_df['Video Rank'] == r)]['Video data Published Date'].min()
                    
                    asset_data = asset_daily_total[asset_daily_total['Video data Custom ID'] == asset_id]
                    
                    before = asset_data[(asset_data['Metrics Date Date'] < launch_date) & 
                                        (asset_data['Metrics Date Date'] >= launch_date - pd.Timedelta(days=28))]['Metrics Organic Views'].sum()
                    after = asset_data[(asset_data['Metrics Date Date'] >= launch_date) & 
                                       (asset_data['Metrics Date Date'] < launch_date + pd.Timedelta(days=28))]['Metrics Organic Views'].sum()
                    
                    if before > 0:
                        total_lift_pct.append(((after - before) / before) * 100)
                
                if total_lift_pct:
                    avg_lift = np.mean(total_lift_pct)
                    l_str = f"- Injection V{r}: **{avg_lift:+.1f}%** total volume"
                    st.write(l_str); group_summary_txt += l_str.replace('**', '') + "\n"

        # --- ADD TO PDF ---
        pdf.set_font("Arial", 'B', 12); pdf.cell(0, 10, f"Group: {current_vol} Iterations", ln=True)
        pdf.set_font("Arial", size=10)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            fig.write_image(tmp.name)
            pdf.image(tmp.name, x=10, w=180); os.remove(tmp.name)
        pdf.multi_cell(0, 7, group_summary_txt); pdf.ln(10)
        st.divider()

    st.download_button("📥 Download Strategic Report (PDF)", data=pdf.output(dest='S').encode('latin-1'), file_name="YT_Injection_Impact_Report.pdf")
else:
    st.info("👋 Upload a CSV to begin.")
