import pandas as pd
import camelot
import time

start = time.time()
print("Reading PDF...")
tables = camelot.read_pdf("data/source.pdf", pages="2-end", flavor="stream")

print("Transforming data...")
df = pd.concat([table.df[8:] for table in tables])
df = df.rename(
    columns={
        0: "stock_curr",
        1: "market_name",
        2: "etf_name",
        3: "long_ticker",
        4: "bloomberg_ticker",
        5: "google_ticker",
        6: "isin",
        7: "instrument_type",
    }
).reset_index()
df[["stock_market", "currency"]] = df["stock_curr"].str.extract(r"(.+) \(([A-Z]{3})\)")
df[["ticker", "market_code"]] = df["bloomberg_ticker"].str.split(":", 1, expand=True)
df = df[["stock_market", "currency", "etf_name", "ticker", "market_code", "isin"]]

print("Saving to file...")
df.to_json("data/etfs.json", orient="records")

end = time.time()
execution_time = "{:.2f}".format(end - start)
print(f"Done! ({execution_time}s)")
