import numpy as np


def momentum_6m(price):

    try:
        return (price["Close"][-1] / price["Close"][-126]) - 1
    except:
        return np.nan


def momentum_12m(price):

    try:
        return (price["Close"][-1] / price["Close"][0]) - 1
    except:
        return np.nan


def ev_ebit(info):

    try:

        ev = info.get("enterpriseValue", None)
        ebit = info.get("ebitda", None)

        if ev and ebit:
            return ev / ebit

        return np.nan

    except:
        return np.nan


def roic(info):

    try:

        ebit = info.get("ebitda", None)
        invested_capital = info.get("totalAssets", None)

        if ebit and invested_capital:
            return ebit / invested_capital

        return np.nan

    except:
        return np.nan


def piotroski(data):

    try:

        income = data["income"]
        balance = data["balance"]
        cash = data["cash"]

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
