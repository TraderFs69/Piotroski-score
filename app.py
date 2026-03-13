import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import os

st.set_page_config(layout="wide")

st.title("Quant Research Terminal")

# ---------------------------------------------------
# LOAD RUSSELL UNIVERSE
# ---------------------------------------------------

BASE_DIR = os.path.dirname(__file__)
file_path = os.path.join(BASE_DIR, "russell3000_constituents.xlsx")

df_universe = pd.read_excel(file_path)

tickers = df_universe["Symbol"].dropna().tolist()

st.write("Universe size:", len(tickers))

MAX_STOCKS = st.slider("Max stocks to scan", 100, 2000, 500)

tickers = tickers[:MAX_STOCKS]

# ---------------------------------------------------
# DOWNLOAD PRICE DATA
# ---------------------------------------------------

@st.cache_data
def download_prices(tickers):

    data = yf.download(
        tickers,
        period="1y",
        group_by="ticker",
        threads=True,
        auto_adjust=True
    )

    return data

prices = download_prices(tickers)

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
# FUNDAMENTALS
# ---------------------------------------------------

@st.cache_data
def fundamentals(ticker):

    try:

        stock = yf.Ticker(ticker)
        info = stock.info

        ev = info.get("enterpriseValue")
        ebit = info.get("ebitda")
        assets = info.get("totalAssets")

        if ev and ebit:
            ev_ebit = ev / ebit
        else:
            ev_ebit = np.nan

        if ebit and assets:
            roic = ebit / assets
        else:
            roic = np.nan

        return ev_ebit, roic

    except:

        return np.nan, np.nan


# ---------------------------------------------------
# PIOTROSKI
# ---------------------------------------------------

def piotroski(ticker):

    try:

        stock = yf.Ticker(ticker)

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

        if roa > 0: score += 1
        if cfo[0] > 0: score += 1
        if cfo[0] > ni[0]: score += 1
        if roa > roa_prev: score += 1
        if lt_debt[0] < lt_debt[1]: score += 1
        if (current_assets[0]/current_liab[0]) > (current_assets[1]/current_liab[1]): score += 1
        if (gross[0]/revenue[0]) > (gross[1]/revenue[1]): score += 1
        if (revenue[0]/assets[0]) > (revenue[1]/assets[1]): score += 1

        return score

    except:

        return np.nan


# ---------------------------------------------------
# SCAN
# ---------------------------------------------------

if st.button("Run Quant Scan"):

    results = []

    progress = st.progress(0)

    for i, ticker in enumerate(tickers):

        try:

            price = prices[ticker]["Close"]

            m6, m12 = momentum(price)

            ev_ebit, roic = fundamentals(ticker)

            p = piotroski(ticker)

            results.append({

                "Ticker": ticker,
                "Piotroski": p,
                "Momentum6M": m6,
                "Momentum12M": m12,
                "EV_EBIT": ev_ebit,
                "ROIC": roic

            })

        except:

            pass

        progress.progress((i+1)/len(tickers))

    df = pd.DataFrame(results)

# ---------------------------------------------------
# RANKING
# ---------------------------------------------------

    df["Composite"] = (
        df["Piotroski"].rank(ascending=False)
        + df["Momentum6M"].rank(ascending=False)
        + df["Momentum12M"].rank(ascending=False)
        + df["ROIC"].rank(ascending=False)
        + df["EV_EBIT"].rank(ascending=True)
    )

    df = df.sort_values("Composite")

# ---------------------------------------------------
# OUTPUT
# ---------------------------------------------------

    st.success("Scan complete")

    st.subheader("Top Quant Stocks")

    st.dataframe(df.head(50), height=600)

# ---------------------------------------------------
# HEATMAP
# ---------------------------------------------------

    st.subheader("Factor Heatmap")

    heat = df.set_index("Ticker")[
        ["Piotroski","Momentum6M","Momentum12M","EV_EBIT","ROIC"]
    ]

    st.dataframe(heat.head(50))

# ---------------------------------------------------
# PORTFOLIO
# ---------------------------------------------------

    st.subheader("Quant Portfolio")

    portfolio = df.head(20)

    portfolio["Weight"] = 1 / len(portfolio)

    st.dataframe(portfolio)

# ---------------------------------------------------
# EXPORT
# ---------------------------------------------------

    st.download_button(
        "Download Results",
        df.to_csv(index=False),
        "quant_results.csv"
    )
