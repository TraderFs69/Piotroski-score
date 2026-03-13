import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import os
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(layout="wide")

st.title("Russell Quant Factor Scanner")

# ---------------------------------------------------
# LOAD UNIVERSE
# ---------------------------------------------------

BASE_DIR = os.path.dirname(__file__)
file_path = os.path.join(BASE_DIR, "russell3000_constituents.xlsx")

df_universe = pd.read_excel(file_path)

tickers = df_universe["Symbol"].dropna().tolist()

st.write("Universe size:", len(tickers))

MAX_STOCKS = st.slider("Max stocks to scan", 50, 1000, 300)

tickers = tickers[:MAX_STOCKS]

# ---------------------------------------------------
# MOMENTUM
# ---------------------------------------------------

def momentum(price):

    try:
        m6 = (price.iloc[-1] / price.iloc[-126]) - 1
        m12 = (price.iloc[-1] / price.iloc[0]) - 1
        return m6, m12
    except:
        return np.nan, np.nan


# ---------------------------------------------------
# EV / EBIT
# ---------------------------------------------------

def ev_ebit(stock):

    try:

        balance = stock.balance_sheet
        income = stock.financials

        debt = balance.loc["Long Term Debt"].iloc[0]

        cash = balance.loc["Cash"].iloc[0]

        shares = stock.fast_info["shares"]

        price = stock.fast_info["last_price"]

        market_cap = shares * price

        ebit = income.loc["Operating Income"].iloc[0]

        enterprise_value = market_cap + debt - cash

        return enterprise_value / ebit

    except:

        return np.nan


# ---------------------------------------------------
# ROIC
# ---------------------------------------------------

def roic(stock):

    try:

        income = stock.financials
        balance = stock.balance_sheet

        ebit = income.loc["Operating Income"].iloc[0]

        debt = balance.loc["Long Term Debt"].iloc[0]

        equity = balance.loc["Total Stockholder Equity"].iloc[0]

        invested_capital = debt + equity

        return ebit / invested_capital

    except:

        return np.nan


# ---------------------------------------------------
# PIOTROSKI
# ---------------------------------------------------

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

        revenue = income.loc["Total Revenue"]
        gross = income.loc["Gross Profit"]

        score = 0

        roa = ni.iloc[0] / assets.iloc[0]
        roa_prev = ni.iloc[1] / assets.iloc[1]

        if roa > 0:
            score += 1

        if cfo.iloc[0] > 0:
            score += 1

        if cfo.iloc[0] > ni.iloc[0]:
            score += 1

        if roa > roa_prev:
            score += 1

        if (gross.iloc[0] / revenue.iloc[0]) > (gross.iloc[1] / revenue.iloc[1]):
            score += 1

        if (revenue.iloc[0] / assets.iloc[0]) > (revenue.iloc[1] / assets.iloc[1]):
            score += 1

        return score

    except:

        return np.nan


# ---------------------------------------------------
# PROCESS TICKER
# ---------------------------------------------------

def process_ticker(ticker):

    try:

        stock = yf.Ticker(ticker)

        price = stock.history(period="1y")

        if price.empty:
            return None

        m6, m12 = momentum(price["Close"])

        ev = ev_ebit(stock)

        r = roic(stock)

        p = piotroski(stock)

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


# ---------------------------------------------------
# MULTITHREAD
# ---------------------------------------------------

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


# ---------------------------------------------------
# RUN SCAN
# ---------------------------------------------------

if st.button("Run Quant Scan"):

    with st.spinner("Scanning universe..."):

        results = run_parallel(process_ticker, tickers)

    df = pd.DataFrame(results)

    if df.empty:

        st.warning("No data retrieved")

        st.stop()

    # ---------------------------------------------------
    # COMPOSITE SCORE
    # ---------------------------------------------------

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
