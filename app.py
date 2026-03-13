import streamlit as st
import pandas as pd

from data_fetcher import fetch_data
from factors import piotroski, momentum_6m, momentum_12m, ev_ebit, roic
from utils import run_parallel


st.set_page_config(layout="wide")

st.title("Quant Russell Factor Scanner")

df = pd.read_excel("universe/russell3000_constituents.xlsx")

tickers = df["Symbol"].dropna().tolist()

st.write("Universe size:", len(tickers))


def process_ticker(ticker):

    data = fetch_data(ticker)

    if not data:
        return None

    p = piotroski(data)

    m6 = momentum_6m(data["price"])
    m12 = momentum_12m(data["price"])

    ev = ev_ebit(data["info"])

    r = roic(data["info"])

    return {
        "Ticker": ticker,
        "Piotroski": p,
        "Momentum6M": m6,
        "Momentum12M": m12,
        "EV_EBIT": ev,
        "ROIC": r
    }


if st.button("Run Quant Scan"):

    with st.spinner("Scanning universe..."):

        results = run_parallel(process_ticker, tickers)

    df = pd.DataFrame(results)

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
        "Download CSV",
        df.to_csv(index=False),
        "quant_results.csv"
    )
