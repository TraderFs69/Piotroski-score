import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import os
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(layout="wide")

st.title("Russell Quant Factor Scanner")

# -----------------------------
# Charger fichier Russell
# -----------------------------

BASE_DIR = os.path.dirname(__file__)
file_path = os.path.join(BASE_DIR, "russell3000_constituents.xlsx")

df = pd.read_excel(file_path)

tickers = df["Symbol"].dropna().tolist()

st.write("Total stocks in universe:", len(tickers))

MAX_STOCKS = st.slider("Max stocks to scan", 50, 1000, 300)

tickers = tickers[:MAX_STOCKS]


# -----------------------------
# FACTORS
# -----------------------------

def momentum_6m(price):

    try:
        return (price["Close"].iloc[-1] / price["Close"].iloc[-126]) - 1
    except:
        return np.nan


def momentum_12m(price):

    try:
        return (price["Close"].iloc[-1] / price["Close"].iloc[0]) - 1
    except:
        return np.nan


def ev_ebit(info):

    try:
        ev = info.get("enterpriseValue")
        ebit = info.get("ebitda")

        if ev and ebit:
            return ev / ebit

        return np.nan

    except:
        return np.nan


def roic(info):

    try:
        ebit = info.get("ebitda")
        assets = info.get("totalAssets")

        if ebit and assets:
            return ebit / assets

        return np.nan

    except:
        return np.nan


def piotroski(stock):

    try:

        income = stock.financials
        balance = stock.balance_sheet
        cash = stock.cashflow

        if income.shape[1] < 2:
            return np.nan

        ni = income.loc["Net Income"]
        assets = balance.loc["Total Assets"]
        cfo = cash.loc["Total Cash From Operating Activities"]

        lt_debt = balance.loc["Long Term Debt"]

        revenue = income.loc["Total Revenue"]
        gross = income.loc["Gross Profit"]

        current_assets = balance.loc["Total Current Assets"]
        current_liab = balance.loc["Total Current Liabilities"]

        score = 0

        roa = ni[0] / assets[0]
        roa_prev = ni[1] / assets[1]

        if roa > 0:
            score += 1

        if cfo[0] > 0:
            score += 1

        if cfo[0] > ni[0]:
            score += 1

        if roa > roa_prev:
            score += 1

        if lt_debt[0] < lt_debt[1]:
            score += 1

        if (current_assets[0] / current_liab[0]) > (current_assets[1] / current_liab[1]):
            score += 1

        if (gross[0] / revenue[0]) > (gross[1] / revenue[1]):
            score += 1

        if (revenue[0] / assets[0]) > (revenue[1] / assets[1]):
            score += 1

        return score

    except:
        return np.nan


# -----------------------------
# PROCESS TICKER
# -----------------------------

@st.cache_data(show_spinner=False)
def process_ticker(ticker):

    try:

        stock = yf.Ticker(ticker)

        price = stock.history(period="1y")

        if price.empty:
            return None

        info = stock.info

        p = piotroski(stock)

        m6 = momentum_6m(price)

        m12 = momentum_12m(price)

        ev = ev_ebit(info)

        r = roic(info)

        return {
            "Ticker": ticker,
            "Piotroski": p,
            "Momentum6M": m6,
            "Momentum12M": m12,
            "EV_EBIT": ev,
            "ROIC": r
        }

    except:

        return None


# -----------------------------
# MULTITHREAD SCAN
# -----------------------------

def run_parallel(func, items, workers=20):

    results = []

    with ThreadPoolExecutor(max_workers=workers) as executor:

        futures = [executor.submit(func, item) for item in items]

        for f in futures:

            try:

                r = f.result()

                if r:
                    results.append(r)

            except:
                pass

    return results


# -----------------------------
# RUN SCAN
# -----------------------------

if st.button("Run Quant Scan"):

    with st.spinner("Scanning universe..."):

        results = run_parallel(process_ticker, tickers)

    df = pd.DataFrame(results)

    if df.empty:
        st.warning("No data retrieved.")
        st.stop()

    df["Composite"] = (
        df["Piotroski"].rank(ascending=False)
        + df["Momentum6M"].rank(ascending=False)
        + df["Momentum12M"].rank(ascending=False)
        + df["ROIC"].rank(ascending=False)
        + df["EV_EBIT"].rank(ascending=True)
    )

    df = df.sort_values("Composite")

    st.success("Scan Complete")

    st.dataframe(df, height=700)

    st.download_button(
        "Download Results CSV",
        df.to_csv(index=False),
        "quant_results.csv"
    )
