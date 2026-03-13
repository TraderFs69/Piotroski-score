import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import os
from concurrent.futures import ThreadPoolExecutor

st.set_page_config(layout="wide")
st.title("S&P500 Quant Scanner")

BASE_DIR=os.path.dirname(__file__)
file_path=os.path.join(BASE_DIR,"sp500_constituents.xlsx")

sp500=pd.read_excel(file_path)

tickers=sp500["Symbol"].tolist()

st.write("Universe size:",len(tickers))

MAX_STOCKS=st.slider("Max stocks",10,200,50)

tickers=tickers[:MAX_STOCKS]


# ---------------------------------------------------
# UTILITY FUNCTION
# ---------------------------------------------------

def find_value(df,names):

    for n in names:
        if n in df.index:
            return df.loc[n].iloc[0]

    return None


# ---------------------------------------------------
# MOMENTUM
# ---------------------------------------------------

def momentum(stock):

    try:

        price=stock.history(period="1y")

        if len(price)<200:
            return None,None

        m6=(price["Close"].iloc[-1]/price["Close"].iloc[-126])-1
        m12=(price["Close"].iloc[-1]/price["Close"].iloc[0])-1

        return m6,m12

    except:

        return None,None


# ---------------------------------------------------
# ROIC
# ---------------------------------------------------

def roic(stock):

    try:

        income=stock.financials
        balance=stock.balance_sheet

        ebit=find_value(income,[
            "Operating Income",
            "OperatingIncome",
            "EBIT"
        ])

        debt=find_value(balance,[
            "Long Term Debt",
            "LongTermDebt"
        ])

        equity=find_value(balance,[
            "Total Stockholder Equity",
            "StockholdersEquity"
        ])

        if ebit is None or equity is None:
            return None

        debt=debt if debt else 0

        invested=debt+equity

        return ebit/invested

    except:

        return None


# ---------------------------------------------------
# EV / EBIT
# ---------------------------------------------------

def ev_ebit(stock):

    try:

        income=stock.financials
        balance=stock.balance_sheet

        ebit=find_value(income,[
            "Operating Income",
            "OperatingIncome",
            "EBIT"
        ])

        if ebit is None:
            return None

        price=stock.fast_info["last_price"]
        shares=stock.fast_info["shares"]

        market_cap=price*shares

        debt=find_value(balance,[
            "Long Term Debt",
            "LongTermDebt"
        ])

        cash=find_value(balance,[
            "Cash",
            "Cash And Cash Equivalents",
            "CashAndCashEquivalents"
        ])

        debt=debt if debt else 0
        cash=cash if cash else 0

        ev=market_cap+debt-cash

        return ev/ebit

    except:

        return None


# ---------------------------------------------------
# PIOTROSKI
# ---------------------------------------------------

def piotroski(stock):

    try:

        income=stock.financials
        balance=stock.balance_sheet
        cash=stock.cashflow

        ni=find_value(income,[
            "Net Income",
            "NetIncome"
        ])

        assets=find_value(balance,[
            "Total Assets",
            "TotalAssets"
        ])

        cfo=find_value(cash,[
            "Total Cash From Operating Activities",
            "OperatingCashFlow"
        ])

        if ni is None or assets is None or cfo is None:
            return None

        ni_prev=income.loc[income.index[0]].iloc[1]
        assets_prev=balance.loc[balance.index[0]].iloc[1]

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

        return score

    except:

        return None


# ---------------------------------------------------
# PROCESS STOCK
# ---------------------------------------------------

def process_stock(ticker):

    try:

        stock=yf.Ticker(ticker)

        m6,m12=momentum(stock)

        if m6 is None:
            return None

        r=roic(stock)
        ev=ev_ebit(stock)
        p=piotroski(stock)

        return{
            "Ticker":ticker,
            "Momentum6M":m6,
            "Momentum12M":m12,
            "ROIC":r,
            "EV_EBIT":ev,
            "Piotroski":p
        }

    except:

        return None


# ---------------------------------------------------
# MULTITHREAD
# ---------------------------------------------------

def run_parallel(func,items):

    results=[]

    with ThreadPoolExecutor(max_workers=8) as executor:

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
# RUN
# ---------------------------------------------------

if st.button("Run Scan"):

    with st.spinner("Scanning..."):

        results=run_parallel(process_stock,tickers)

    st.write("Stocks processed:",len(results))

    if len(results)==0:

        st.error("No data returned")

        st.stop()

    df=pd.DataFrame(results)

    df["Composite"]=(
        df["Momentum6M"].rank(ascending=False)
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
