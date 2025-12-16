import pandas as pd
import requests
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from textblob import TextBlob
from datetime import datetime
import certifi
import json
import io

# 🔐 Configuration
GNEWS_API_KEY = "13fff0b4ba346ab82a827b22540b8515"
GSHEET_NAME = "Digital_Scam_Alerts"
JSON_KEY_FILE = "C:\\Users\\Dell\\Downloads\\digital-scam-1685dc844428.json"

# 1️⃣ Fetch NCRB Cybercrime Data
def fetch_ncrb_data():
    print("📂 Fetching NCRB cybercrime data (secure mode)...")
    url = "https://ncrb.gov.in/uploads/crime-in-india/Table%2021.1.xlsx"
    try:
        response = requests.get(url, verify=certifi.where(), timeout=30)
        response.raise_for_status()
        df = pd.read_excel(io.BytesIO(response.content))
        df.columns = df.columns.astype(str).str.strip()
        df["Source"] = "NCRB"
        df["Year"] = pd.Timestamp.now().year
        print(f"✅ Loaded {len(df)} NCRB records securely.")
        return df
    except Exception as e:
        print("❌ Error fetching NCRB data:", e)
        return pd.DataFrame()

# 2️⃣ Fetch Real-Time Phishing URLs
def fetch_phishing_data():
    print("🌐 Fetching real-time phishing data...")
    url = "http://data.phishtank.com/data/online-valid.json"
    try:
        res = requests.get(url)
        data = res.json()
        df = pd.DataFrame(data)[["url", "submission_time", "verified"]]
        df["submission_time"] = pd.to_datetime(df["submission_time"])
        df["Date"] = df["submission_time"].dt.date
        df["Source"] = "PhishTank"
        print(f"✅ Loaded {len(df)} phishing URLs.")
        return df
    except Exception as e:
        print("❌ Error fetching phishing data:", e)
        return pd.DataFrame()

# 3️⃣ Fetch News Headlines
def fetch_news_data():
    print("📰 Fetching latest fraud-related news...")
    url = f"https://gnews.io/api/v4/search?q=cyber+fraud+india&lang=en&country=in&max=50&token={GNEWS_API_KEY}"
    try:
        res = requests.get(url)
        if res.status_code != 200:
            print("❌ GNews API error:", res.text)
            return pd.DataFrame()
        articles = res.json().get("articles", [])
        news_data = [{
            "Title": a["title"],
            "Description": a["description"],
            "Source": a["source"]["name"],
            "Published_At": a["publishedAt"],
            "URL": a["url"]
        } for a in articles]
        df = pd.DataFrame(news_data)
        df["Source"] = "GNews"
        print(f"✅ Loaded {len(df)} news articles.")
        return df
    except Exception as e:
        print("❌ Error fetching news data:", e)
        return pd.DataFrame()

# 4️⃣ Sentiment & State Detection
def enrich_news(df):
    print("🧠 Performing sentiment and state detection...")
    if df.empty:
        return df

    states = [
        "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar", "Chhattisgarh", "Goa", "Gujarat", "Haryana",
        "Himachal Pradesh", "Jharkhand", "Karnataka", "Kerala", "Madhya Pradesh", "Maharashtra", "Manipur",
        "Meghalaya", "Mizoram", "Nagaland", "Odisha", "Punjab", "Rajasthan", "Sikkim", "Tamil Nadu", "Telangana",
        "Tripura", "Uttar Pradesh", "Uttarakhand", "West Bengal", "Delhi", "Puducherry"
    ]

    detected_states, sentiments = [], []
    for _, row in df.iterrows():
        text = f"{row['Title']} {row['Description']}".lower()
        found_state = next((s for s in states if s.lower() in text), "Unknown")
        detected_states.append(found_state)
        sentiments.append(TextBlob(text).sentiment.polarity)

    df["Detected_State"] = detected_states
    df["Sentiment_Score"] = sentiments
    df["Sentiment_Label"] = df["Sentiment_Score"].apply(lambda x: "Positive" if x > 0.1 else ("Negative" if x < -0.1 else "Neutral"))
    df["Month"] = pd.to_datetime(df["Published_At"]).dt.to_period("M").astype(str)
    print("✅ News enriched with sentiment and state classification.")
    return df

# 5️⃣ Connect to Google Sheet
def connect_gsheet(sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, scope)
    client = gspread.authorize(creds)
    return client.open(sheet_name)

# 6️⃣ Write Data to Sheet (with Timestamp fix)
def write_to_gsheet(sheet, tab_name, df):
    if df.empty:
        print(f"⚠️ No data to write for {tab_name}.")
        return
    try:
        # Convert datetime-like columns to string
        df = df.copy()
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]) or pd.api.types.is_timedelta64_dtype(df[col]):
                df[col] = df[col].astype(str)

        ws = None
        try:
            ws = sheet.worksheet(tab_name)
            sheet.del_worksheet(ws)
        except:
            pass

        ws = sheet.add_worksheet(title=tab_name, rows=str(len(df)+1), cols=str(len(df.columns)))
        ws.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"✅ {tab_name} updated ({len(df)} rows).")
    except Exception as e:
        print(f"❌ Error writing to {tab_name}:", e)

# 7️⃣ Main Execution
def main():
    print("🚀 Starting Advanced Digital Fraud Data Import...")

    ncrb_df = fetch_ncrb_data()
    phishing_df = fetch_phishing_data()
    news_df = fetch_news_data()
    news_enriched = enrich_news(news_df)

    trend_summary = (
        news_enriched.groupby(["Detected_State", "Month", "Sentiment_Label"])
        .size()
        .reset_index(name="Count")
    )

    sheet = connect_gsheet(GSHEET_NAME)
    write_to_gsheet(sheet, "NCRB_Stats", ncrb_df)
    write_to_gsheet(sheet, "Phishing_Live", phishing_df)
    write_to_gsheet(sheet, "Fraud_News_Enriched", news_enriched)
    write_to_gsheet(sheet, "Trends_Summary", trend_summary)

    print("✅ All data enriched and uploaded successfully!")
    print("📊 You can now build Looker Studio dashboards by state, sentiment, and trend.")

if __name__ == "__main__":
    main()
