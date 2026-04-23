# YT Asset Strategic Analysis

A Streamlit app for analyzing YouTube asset iteration performance, decay timing, and 28-day injection impact across repeated video uploads.

## 🚀 What this app does

- Reads YouTube CSV data and identifies assets by `Custom ID`.
- Detects upload sequence rank for each asset iteration (Video 1, Video 2, etc.).
- Visualizes timeline stacking of organic views across asset iterations.
- Calculates decay timing when views fall below a configurable percent of peak performance.
- Estimates 28-day injection impact for re-uploads, including percentage lift, total view increase, and average daily view gain.
- Exports a PDF report summarizing each iteration group.

## 📁 Required CSV columns

The app expects a CSV with these columns:

- `Video data Custom ID`
- `Video data Video ID`
- `Video data Published Date`
- `Metrics Date Date`
- `Metrics Organic Views`

> The tool normalizes numeric view values and parses dates automatically.

## 🧠 Key features

- Sidebar controls for timeline window and decay threshold.
- Interactive Plotly charts for average view progression by iteration.
- Insight panels showing upload timing, decay behavior, and 28-day performance lift.
- PDF download of the full strategic report.

## 🛠️ Installation

```bash
python -m pip install -r requirements.txt
```

## ▶️ Run the app

```bash
streamlit run app.py
```

## 📌 Notes

- Upload the YouTube CSV file using the file uploader in the app.
- The app supports assets with more than one upload iteration and calculates comparative group metrics.
- PDF export includes charts and summary findings for each iteration group.
