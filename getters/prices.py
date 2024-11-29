import yfinance as yf
from argparse import ArgumentParser

if __name__ == '__main__':
  parser = ArgumentParser()
  parser.add_argument("ticker",nargs="+",help="Tickers to download")
  parser.add_argument("--output",type=str,help="Output filename", required=True)

  args = parser.parse_args()
  data = yf.download(args.ticker, period='1d',interval="1m",group_by="Ticker")
  data = data.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index()
  data.rename({
    "Open":"open",
    "High":"high",
    "Low":"low",
    "Close": "close",
    "Volume":"volume",
    "Ticker": "asset",
    "Date": "datetime"
  },inplace=True, axis="columns")
  print(data)
  data.to_parquet(args.output)