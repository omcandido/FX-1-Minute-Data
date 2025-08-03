import os
import pandas as pd
from deltalake import write_deltalake, DeltaTable
from histdata.api import download_hist_data


def main():
    output = os.environ.get("FX_DATA_OUTPUT", 'output')
    dt = DeltaTable(output)
    df = dt.to_pandas(partitions=[('pair', '=', 'AUDUSD')])
    partitions = pd.DataFrame(dt.partitions())

    pairs = partitions.pair.unique()

    for pair in pairs:
        print(f"Processing pair: {pair}")

        df = dt.to_pandas(partitions=[('pair', '=', pair)])
        if not df.empty:
            last_year = df['year'].max()
            last_month = df[df['year'] == last_year]['date'].dt.month.max()
            print(f"Last data for {pair}: {last_year}-{last_month:02d}")

            # Try to fetch the last year
            try:
                print(f"Fetching full year data for {pair} {last_year}")
                # Here you would call your download function, e.g.:
                download_hist_data(year=last_year, pair=pair, output_directory=output, delta_lake=True)
            except Exception as e:
                print(f"Failed to fetch full year data for {pair} {last_year}: {e}")
                # Resume fetching per month
                for month in range(1, 13):
                    try:
                        print(f"Fetching month {month} for {pair} {last_year}")
                        download_hist_data(year=last_year, month=month, pair=pair, output_directory=output, delta_lake=True)
                    except Exception as e:
                        print(f"Failed to fetch month {month} for {pair} {last_year}: {e}")
        else:
            raise ValueError(f"No data found for pair {pair} in Delta Lake.")
    

if __name__ == "__main__":
    main()