import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np

st.set_page_config(page_title="Advanced YT Iteration Analytics", layout="wide")

st.title("Strategic Asset Lifecycle & Decay Analysis")

def calculate_decay_day(video_df):
    """Finds the first day of a 3-day streak below 90% of peak views."""
    video_df = video_df.sort_values('Days Since Published')
    if video_df.empty: return None
    
    peak_val = video_df['Metrics Organic Views'].max()
    peak_day = video_df.loc[video_df['Metrics Organic Views'].idxmax(), 'Days Since Published']
    threshold = 0.9 * peak_val
    
    # Check only days after the peak
    post_peak = video_df[video_df['Days Since Published'] >= peak_day].copy()
    post_peak['below_threshold'] = post_peak['Metrics Organic Views'] < threshold
    
    # Find 3 consecutive days below threshold
    post_peak['streak'] = post_peak['below_threshold'].rolling(window=3).sum()
    decay_hit = post_peak[post_peak['streak'] == 3]
    
    if not decay_hit.empty:
        return decay_hit.iloc[0]['Days Since Published']
    return None

uploaded_file = st.file_uploader("Upload your CSV", type=["csv"])

if uploaded_file is not None:
    df = pd.read_csv(uploaded_file)
    df.columns = [col.replace('\n', '').strip() for col in df.columns]
    df['Metrics Organic Views'] = pd.to_numeric(df['Metrics Organic Views'].astype(str).str.replace(',', ''), errors='coerce')
    df['Video data Published Date'] = pd.to_datetime(df['Video data Published Date'])
    df['Metrics Date Date'] = pd.to_datetime(df['Metrics Date Date'])
    df = df.dropna(subset=['Metrics Organic Views'])

    # 1. Logic: Ranking & Gaps
    video_info = df[['Video data Custom ID', 'Video data Video ID', 'Video data Published Date']].drop_duplicates()
    video_info = video_info.sort_values(by=['Video data Custom ID', 'Video data Published Date'])
    video_info['Video Rank'] = video_info.groupby('Video data Custom ID').cumcount() + 1
    
    # Asset Volume
    vol = video_info.groupby('Video data Custom ID')['Video data Video ID'].count().reset_index(name='Total_Videos')
    
    # Start Dates & Relative Days
    start_dates = video_info.groupby('Video data Custom ID')['Video data Published Date'].min().reset_index(name='Asset_Day_0')
    
    video_info = video_info.merge(start_dates, on='Video data Custom ID').merge(vol, on='Video data Custom ID')
    video_info['Days_From_Start'] = (video_info['Video data Published Date'] - video_info['Asset_Day_0']).dt.days
    
    df = df.merge(video_info[['Video data Video ID', 'Video Rank', 'Total_Videos', 'Asset_Day_0']], on='Video data Video ID')
    df['Days Since Asset Start'] = (df['Metrics Date Date'] - df['Asset_Day_0']).dt.days
    df['Days Since Published'] = (df['Metrics Date Date'] - df['Video data Published Date']).dt.days

    # 2. Sidebar Selection
    selected_vol = st.sidebar.selectbox("Select Asset Group (by Iteration Count)", sorted(df['Total_Videos'].unique()), index=2)
    
    # 3. Aggregation for Plots
    sub_df = df[df['Total_Videos'] == selected_vol]
    sample_size = sub_df['Video data Custom ID'].nunique()
    
    agg_mean = sub_df.groupby(['Video Rank', 'Days Since Asset Start'])['Metrics Organic Views'].mean().reset_index()
    agg_mean['Video Rank'] = "Video " + agg_mean['Video Rank'].astype(str)

    # 4. Calculation for Bullet Points
    # Timing
    timing_info = video_info[video_info['Total_Videos'] == selected_vol].pivot(index='Video data Custom ID', columns='Video Rank', values='Days_From_Start')
    
    # Decay
    decay_list = []
    for vid_id in sub_df['Video data Video ID'].unique():
        v_df = sub_df[sub_df['Video data Video ID'] == vid_id]
        rank = v_df['Video Rank'].iloc[0]
        day = calculate_decay_day(v_df)
        if day is not None: decay_list.append({'Rank': rank, 'Day': day})
    decay_df = pd.DataFrame(decay_list)

    # 5. UI Layout
    st.subheader(f"Total Combined Performance: {selected_vol} Iterations")
    fig = px.area(agg_mean, x="Days Since Asset Start", y="Metrics Organic Views", color="Video Rank", 
                 title=f"Sample Size: {sample_size} Assets")
    st.plotly_chart(fig, use_container_width=True)

    # The Logic Insights
    st.markdown("### 📊 Group Performance Insights")
    st.markdown(f"**Sample Size:** Analysis based on {sample_size} unique assets.")
    
    # Timing Gaps
    if selected_vol >= 2:
        gap_2_1 = timing_info[2].mean()
        st.write(f"- On average, **Video 2** was posted **{gap_2_1:.1f} days** after Video 1.")
    if selected_vol >= 3:
        gap_3_1 = timing_info[3].mean()
        gap_3_2 = (timing_info[3] - timing_info[2]).mean()
        st.write(f"- On average, **Video 3** was posted **{gap_3_1:.1f} days** after Video 1 (and **{gap_3_2:.1f} days** after Video 2).")

    # Decay Insights
    st.markdown("**Burn-off Rates (Time to drop below 90% of peak):**")
    if not decay_df.empty:
        avg_decay = decay_df.groupby('Rank')['Day'].mean()
        for rank, day in avg_decay.items():
            st.write(f"- On average, **Video {int(rank)}** drops below 90% of its peak on **Day {day:.1f}**.")
    else:
        st.write("- Not enough data to calculate decay patterns for this group.")
