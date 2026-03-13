import streamlit as st
import pandas as pd
import numpy as np
import requests
import os

st.set_page_config(layout="wide")
st.title("S&P500 Quant Scanner (FMP Bulk)")

API_KEY="0a99uPvKjVkKvQYrjKxvoK7UyO1BekKa"

BASE_DIR=os.path.dirname(__file__)
file_path=os.path.join(BASE_DIR,"sp500_constituents.xlsx")

sp500=pd.read_excel(file_path)

tickers=sp500["Symbol"].tolist()

st.write("Universe size:",len(tickers))


# ---------------------------------------------------
# SAFE API REQUEST
# ---------------------------------------------------

def fetch_dataframe(url):

    try:

        r=requests.get(url)

        if r.status_code!=200:
            return pd.DataFrame()

        data=r.json()

        if isinstance(data,list):
            return pd.DataFrame(data)

        if isinstance(data,dict):

            if "data" in data:
                return pd.DataFrame(data["data"])

        return pd.DataFrame()

    except:

        return pd.DataFrame()


# ---------------------------------------------------
# LOAD BULK DATA
# ---------------------------------------------------

@st.cache_data

def load_bulk():

    ratios=fetch_dataframe(
        f"https://financialmodelingprep.com/api/v3/ratios-bulk?apikey={API_KEY}"
    )

    metrics=fetch_dataframe(
        f"https://financialmodelingprep.com/api/v3/key-metrics-bulk?apikey={API_KEY}"
    )

    prices=fetch_dataframe(
        f"https://financialmodelingprep.com/api/v3/stock/list?apikey={API_KEY}"
    )

    return ratios,metrics,prices


ratios,metrics,prices=load_bulk()


if ratios.empty or metrics.empty:

    st.error("API did not return data (check API key or endpoint)")
    st.stop()


# ---------------------------------------------------
# FILTER S&P500
# ---------------------------------------------------

ratios=ratios[ratios["symbol"].isin(tickers)]
metrics=metrics[metrics["symbol"].isin(tickers)]
prices=prices[prices["symbol"].isin(tickers)]


# ---------------------------------------------------
# MERGE DATA
# ---------------------------------------------------

df=ratios.merge(metrics,on="symbol",how="left")

df=df.merge(prices[["symbol","price"]],on="symbol",how="left")

df=df.rename(columns={"symbol":"Ticker"})


# ---------------------------------------------------
# FACTORS
# ---------------------------------------------------

df["ROIC"]=df["roic"]

df["EV_EBIT"]=df["enterpriseValueMultiple"]

df["Momentum6M"]=df["price"]/df["price"].shift(126)

df["Momentum12M"]=df["price"]/df["price"].shift(252)


df["Piotroski"]=(
    (df["returnOnAssets"]>0).astype(int)
    +(df["returnOnEquity"]>0).astype(int)
    +(df["grossProfitMargin"]>0).astype(int)
)


# ---------------------------------------------------
# COMPOSITE
# ---------------------------------------------------

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
