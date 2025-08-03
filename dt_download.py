import csv
import os
import pandas as pd
from histdata.api import download_hist_data
from deltalake import DeltaTable
from fx_logging import get_project_logger

# Setup logging
logger = get_project_logger(__name__)

def mkdir_p(path):
    import errno
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def download_new(row, output_folder):
    currency_pair_name, pair, history_first_trading_month = row
    year = int(history_first_trading_month[0:4])
    logger.info(f"Starting download for {currency_pair_name}")
    mkdir_p(output_folder)
    try:
        while True:
            could_download_full_year = False
            try:
                logger.debug(f"Attempting to download full year {year} for {pair}")
                result = download_hist_data(year=year,
                                                pair=pair,
                                                output_directory=output_folder,
                                                verbose=False,
                                                delta_lake=True)
                logger.info(f"Successfully downloaded year {year} for {pair}: {result}")
                could_download_full_year = True
            except AssertionError:
                logger.debug(f"Full year download failed for {year}, trying month by month")
                pass  # lets download it month by month.
            month = 1
            while not could_download_full_year and month <= 12:
                logger.debug(f"Downloading month {month}/{year} for {pair}")
                result = download_hist_data(year=str(year),
                                                month=str(month),
                                                pair=pair,
                                                output_directory=output_folder,
                                                verbose=False,
                                                delta_lake=True)
                logger.info(f"Downloaded month {month}/{year} for {pair}: {result}")
                month += 1
            year += 1
    except Exception as e:
        logger.info(f"Download complete for currency {currency_pair_name}: {e}")


def update_existing(pair, output_folder):
    """
    Update existing pair data in Delta Lake.
    We want to resume fetching analogous to the download_new function,
    starting at the last year and month available in the Delta Lake (we want to ensure overlap
    with the month because the last fetched month may not be complete).

    So, first we try to fetch the last full year, and if that fails, we resume fetching per month,
    starting from the last month available in the Delta Lake.
    """

    dt = DeltaTable(output_folder)
    df = dt.to_pandas(partitions=[('pair', '=', pair)])
    if not df.empty:
        last_year = df['year'].max()
        last_month = df[df['year'] == last_year]['date'].dt.month.max()
        logger.info(f"Last data for {pair}: {last_year}-{last_month:02d}")

        # Try to fetch the last year in the Delta Lake, and increase the year if it succeeds
        # if it fails, we will resume fetching per month until it fails, as in the download_new function
        try:
            while True:
                logger.info(f"Fetching full year data for {pair} {last_year}")
                download_hist_data(year=last_year, pair=pair, output_directory=output_folder, delta_lake=True)
                last_year += 1
        except Exception as e:
            logger.warning(f"Failed to fetch full year data for {pair} {last_year}: {e}")
            # Resume fetching per month
            for month in range(last_month, 13):
                try:
                    logger.info(f"Fetching month {month} for {pair} {last_year}")
                    download_hist_data(year=last_year, month=month, pair=pair, output_directory=output_folder, delta_lake=True)
                except Exception as e:
                    logger.warning(f"Failed to fetch month {month} for {pair} {last_year}: {e}")
                    # That means there are no more months to fetch, we can continue with the next pair
                    break
    else:
        raise ValueError(f"No data found for pair {pair} in Delta Lake. Cannot update existing data.")

def download_all():
    output_folder = os.environ.get("FX_DATA_OUTPUT", 'output')
    DT_EXISTS = True if DeltaTable.is_deltatable(output_folder) else False
    if DT_EXISTS:
        logger.info(f"Delta table exists at {output_folder}")
        dt = DeltaTable(output_folder)
        partitions = pd.DataFrame(dt.partitions())
        pairs = partitions.pair.unique()
    else:
        logger.info(f"Delta table does not exist at {output_folder}")


    with open('pairs.csv', 'r') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader, None)  # skip the headers
        for row in reader:
            if DT_EXISTS:
                pair = row[1].upper()
                if pair in pairs:
                    logger.info(f"Pair {pair} already exists in Delta Lake, trying to update...")
                    update_existing(pair, output_folder)
                    continue
            # If we reach here, we need to download the pair
            download_new(row, output_folder)

if __name__ == '__main__':
    download_all()
