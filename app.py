import streamlit as st
import pandas as pd
import numpy as np
import requests
import os
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(layout="wide")
st.title("S&P500 Quant Scanner (FMP)")

API_KEY = "YOUR_API_KEY"

BASE_DIR = os.path.dirname(__file__)
file_path = os.path.join(BASE_DIR, "sp500_constituents.xlsx")

sp500 = pd.read_excel(file_path)
tickers = sp500["Symbol"].dropna().tolist()

st.write("Universe size:", len(tickers))

MAX_STOCKS = st.slider("Max stocks to scan", 5, 100, 20)
tickers = tickers[:MAX_STOCKS]


# ---------------------------------------------------
# SAFE REQUEST
# ---------------------------------------------------

def safe_request(url):

    try:

        r = requests.get(url)

        if r.status_code != 200:
            return None

        data = r.json()

        if isinstance(data, dict) and "Error Message" in data:
            return None

        return data

    except:

        return None


# ---------------------------------------------------
# MOMENTUM
# ---------------------------------------------------

def get_momentum(ticker):

    url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?apikey={API_KEY}"

    data = safe_request(url)

    if not data or "historical" not in data:
        return None, None

    df = pd.DataFrame(data["historical"])

    if len(df) < 50:
        return None, None

    df = df.sort_values("date")

    try:
        m6 = (df["close"].iloc[-1] / df["close"].iloc[-min(126, len(df))]) - 1
        m12 = (df["close"].iloc[-1] / df["close"].iloc[0]) - 1
    except:
        return None, None

    return m6, m12


# ---------------------------------------------------
# RATIOS
# ---------------------------------------------------

def get_ratios(ticker):

    url = f"https://financialmodelingprep.com/api/v3/ratios/{ticker}?limit=1&apikey={API_KEY}"

    data = safe_request(url)

    if not data or len(data) == 0:
        return None, None

    ev_ebit = data[0].get("enterpriseValueMultiple", np.nan)
    roa = data[0].get("returnOnAssets", np.nan)

    return ev_ebit, roa


# ---------------------------------------------------
# ROIC
# ---------------------------------------------------

def get_roic(ticker):

    url = f"https://financialmodelingprep.com/api/v3/key-metrics/{ticker}?limit=1&apikey={API_KEY}"

    data = safe_request(url)

    if not data or len(data) == 0:
        return np.nan

    return data[0].get("roic", np.nan)


# ---------------------------------------------------
# PROCESS STOCK
# ---------------------------------------------------

def process_stock(ticker):

    m6, m12 = get_momentum(ticker)

    ev_ebit, roa = get_ratios(ticker)

    roic = get_roic(ticker)

    if m6 is None and m12 is None:
        return None

    piotroski = int(roa > 0) if roa is not None else 0

    return {
        "Ticker": ticker,
        "Momentum6M": m6,
        "Momentum12M": m12,
        "EV_EBIT": ev_ebit,
        "ROIC": roic,
        "Piotroski": piotroski
    }


# ---------------------------------------------------
# MULTITHREAD
# ---------------------------------------------------

def run_parallel(func, items):

    results = []

    with ThreadPoolExecutor(max_workers=6) as executor:

        futures = [executor.submit(func, item) for item in items]

        for f in futures:

            try:

                r = f.result()

                if r is not None:
                    results.append(r)

            except:
                pass

    return results


# ---------------------------------------------------
# RUN
# ---------------------------------------------------

if st.button("Run Scan"):

    with st.spinner("Scanning..."):

        results = run_parallel(process_stock, tickers)

    st.write("Stocks successfully processed:", len(results))

    if len(results) == 0:
        st.error("No data returned from API")
        st.stop()

    df = pd.DataFrame(results)

    df["Composite"] = (
        df["Momentum6M"].rank(ascending=False)
        + df["Momentum12M"].rank(ascending=False)
        + df["ROIC"].rank(ascending=False)
        + df["EV_EBIT"].rank(ascending=True)
    )

    df = df.sort_values("Composite")

    st.dataframe(df, height=700)

    st.download_button(
        "Download CSV",
        df.to_csv(index=False),
        "quant_results.csv"
    )
