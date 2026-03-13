import streamlit as st
import pandas as pd
import numpy as np
import requests
import os
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(layout="wide")
st.title("S&P500 Quant Scanner (Financial Modeling Prep)")

API_KEY = "0a99uPvKjVkKvQYrjKxvoK7UyO1BekKa"

BASE_DIR = os.path.dirname(__file__)
file_path = os.path.join(BASE_DIR,"sp500_constituents.xlsx")

df_universe = pd.read_excel(file_path)
tickers = df_universe["Symbol"].dropna().tolist()

st.write("Universe size:",len(tickers))

MAX_STOCKS = st.slider("Max stocks",50,500,200)
tickers = tickers[:MAX_STOCKS]

# ---------------------------------------------------
# PRICE MOMENTUM
# ---------------------------------------------------

def get_price(ticker):

    url=f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}?serietype=line&apikey={API_KEY}"

    r=requests.get(url).json()

    if "historical" not in r:
        return None

    df=pd.DataFrame(r["historical"])

    if len(df)<252:
        return None

    df=df.sort_values("date")

    m6=(df["close"].iloc[-1]/df["close"].iloc[-126])-1
    m12=(df["close"].iloc[-1]/df["close"].iloc[-252])-1

    return m6,m12

# ---------------------------------------------------
# ROIC
# ---------------------------------------------------

def get_roic(ticker):

    url=f"https://financialmodelingprep.com/api/v3/key-metrics/{ticker}?limit=1&apikey={API_KEY}"

    r=requests.get(url).json()

    if len(r)==0:
        return np.nan

    return r[0].get("roic",np.nan)

# ---------------------------------------------------
# EV / EBIT
# ---------------------------------------------------

def get_ev_ebit(ticker):

    url=f"https://financialmodelingprep.com/api/v3/ratios/{ticker}?limit=1&apikey={API_KEY}"

    r=requests.get(url).json()

    if len(r)==0:
        return np.nan

    return r[0].get("enterpriseValueMultiple",np.nan)

# ---------------------------------------------------
# PIOTROSKI
# ---------------------------------------------------

def get_piotroski(ticker):

    try:

        income_url=f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=2&apikey={API_KEY}"
        balance_url=f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{ticker}?limit=2&apikey={API_KEY}"
        cash_url=f"https://financialmodelingprep.com/api/v3/cash-flow-statement/{ticker}?limit=2&apikey={API_KEY}"

        income=requests.get(income_url).json()
        balance=requests.get(balance_url).json()
        cash=requests.get(cash_url).json()

        if len(income)<2:
            return np.nan

        ni=income[0]["netIncome"]
        ni_prev=income[1]["netIncome"]

        assets=balance[0]["totalAssets"]
        assets_prev=balance[1]["totalAssets"]

        cfo=cash[0]["operatingCashFlow"]

        revenue=income[0]["revenue"]
        revenue_prev=income[1]["revenue"]

        gross=income[0]["grossProfit"]
        gross_prev=income[1]["grossProfit"]

        debt=balance[0]["longTermDebt"]
        debt_prev=balance[1]["longTermDebt"]

        current_assets=balance[0]["totalCurrentAssets"]
        current_assets_prev=balance[1]["totalCurrentAssets"]

        current_liab=balance[0]["totalCurrentLiabilities"]
        current_liab_prev=balance[1]["totalCurrentLiabilities"]

        score=0

        roa=ni/assets
        roa_prev=ni_prev/assets_prev

        if roa>0:
            score+=1

        if cfo>0:
            score+=1

        if cfo>ni:
            score+=1

        if roa>roa_prev:
            score+=1

        if debt<debt_prev:
            score+=1

        cr=current_assets/current_liab
        cr_prev=current_assets_prev/current_liab_prev

        if cr>cr_prev:
            score+=1

        gm=gross/revenue
        gm_prev=gross_prev/revenue_prev

        if gm>gm_prev:
            score+=1

        at=revenue/assets
        at_prev=revenue_prev/assets_prev

        if at>at_prev:
            score+=1

        score+=1

        return score

    except:

        return np.nan

# ---------------------------------------------------
# PROCESS STOCK
# ---------------------------------------------------

def process_ticker(ticker):

    try:

        m=get_price(ticker)

        if m is None:
            return None

        m6,m12=m

        roic=get_roic(ticker)

        ev=get_ev_ebit(ticker)

        p=get_piotroski(ticker)

        return{
            "Ticker":ticker,
            "Piotroski":p,
            "Momentum6M":m6,
            "Momentum12M":m12,
            "ROIC":roic,
            "EV_EBIT":ev
        }

    except:

        return None

# ---------------------------------------------------
# MULTITHREAD
# ---------------------------------------------------

def run_parallel(func,items,workers=10):

    results=[]

    with ThreadPoolExecutor(max_workers=workers) as executor:

        futures=[executor.submit(func,item) for item in items]

        for f in futures:

            try:

                r=f.result()

                if r:
                    results.append(r)

            except:
                pass

    return results

# ---------------------------------------------------
# RUN SCAN
# ---------------------------------------------------

if st.button("Run Scan"):

    with st.spinner("Scanning..."):

        results=run_parallel(process_ticker,tickers)

    df=pd.DataFrame(results)

    if df.empty:
        st.warning("No data")
        st.stop()

    df["Composite"]=(
        df["Piotroski"].rank(ascending=False)
        +df["Momentum6M"].rank(ascending=False)
        +df["Momentum12M"].rank(ascending=False)
        +df["ROIC"].rank(ascending=False)
        +df["EV_EBIT"].rank(ascending=True)
    )

    df=df.sort_values("Composite")

    st.dataframe(df,height=700)

    st.download_button(
        "Download CSV",
        df.to_csv(index=False),
        "quant_results.csv"
    )
