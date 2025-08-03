# copy paste from http://www.histdata.com/download-free-forex-data/?/ascii/1-minute-bar-quotes > raw.txt

import calendar
import sys
import os

# Add parent directory to path to import fx_logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fx_logging import get_project_logger

# Setup logging
logger = get_project_logger(__name__)

if __name__ == '__main__':
    all_tokens = []
    with open('copy_paste_data.txt') as r:
        lines = r.readlines()
        for line in lines:
            all_tokens.extend(line.strip().split('\t'))
    logger.debug(f"Extracted tokens: {all_tokens}")

    cal_month = {v: k for k,v in enumerate(calendar.month_abbr)}
    output_currency = list()
    output_pair = list()
    history_first_trading_month = list()
    for i in range(1, len(all_tokens), 2):
        pair = all_tokens[i - 1]
        beg = all_tokens[i]
        logger.info(f"Processing pair {pair} with beginning date {beg}")
        currency = pair.replace('/', '').lower()
        output_currency.append(currency)
        output_pair.append(pair)
        date = beg[1:-1]
        year = str(date.split('/')[0])
        month = date.split('/')[1][0:3]
        history_first_trading_month.append(year + str(cal_month[month]).zfill(2))

    import pandas as pd

    d = pd.DataFrame()
    d['currency_pair_name'] = output_pair
    d['currency_pair_code'] = output_currency
    d['history_first_trading_month'] = history_first_trading_month
    d.to_csv('output.csv', index=False, sep='\t')
    logger.info("Successfully created output.csv with currency pair data")