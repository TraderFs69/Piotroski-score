import yfinance as yf

def fetch_data(ticker):

    try:

        stock = yf.Ticker(ticker)

        income = stock.financials
        balance = stock.balance_sheet
        cash = stock.cashflow
        info = stock.info

        price = stock.history(period="1y")

        if income.shape[1] < 2:
            return None

        return {
            "ticker": ticker,
            "income": income,
            "balance": balance,
            "cash": cash,
            "info": info,
            "price": price
        }

    except:
        return None
