import csv
import os
from histdata.api import download_hist_data
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


def download_all():
    output = os.environ.get("FX_DATA_OUTPUT", 'output')

    with open('pairs.csv', 'r') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader, None)  # skip the headers
        for row in reader:
            currency_pair_name, pair, history_first_trading_month = row
            year = int(history_first_trading_month[0:4])
            logger.info(f"Starting download for {currency_pair_name}")
            output_folder = os.path.join(output, pair)
            mkdir_p(output_folder)
            try:
                while True:
                    could_download_full_year = False
                    try:
                        logger.debug(f"Attempting to download full year {year} for {pair}")
                        result = download_hist_data(year=year,
                                                      pair=pair,
                                                      output_directory=output_folder,
                                                      verbose=False)
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
                                                      verbose=False)
                        logger.info(f"Downloaded month {month}/{year} for {pair}: {result}")
                        month += 1
                    year += 1
            except Exception as e:
                logger.info(f"Download complete for currency {currency_pair_name}: {e}")


if __name__ == '__main__':
    download_all()
