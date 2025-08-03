import csv
import os
import sys
from datetime import datetime
from datetime import timedelta
from fx_logging import get_project_logger

# Setup logging
logger = get_project_logger(__name__)

if __name__ == '__main__':

    args = sys.argv
    if len(args) != 3:
        logger.error('Usage: {0} <filename> <time zone difference>\n' \
              'Example: {0} DAT_ASCII_EURJPY_M1_201705.csv +13'.format(args[0].split(os.sep)[-1]))
    else:
        time_zone_difference = int(args[2])
        input_filename = args[1]
        output_filename = 'OUT_' + input_filename
        logger.info(f"Converting timezone for {input_filename} with offset {time_zone_difference} hours")
        
        with open(output_filename, 'w') as w:
            with open(input_filename, 'r') as r:
                reader = csv.reader(r, delimiter=';')
                for row in reader:
                    logger.debug(f"Processing row: {row}")
                    new_row = list(row)
                    ts = datetime.strptime(new_row[0], '%Y%m%d %H%M%S')
                    ts += timedelta(hours=time_zone_difference)
                    new_row[0] = ts.strftime('%Y%m%d %H%M%S')
                    w.write(';'.join(new_row) + '\n')
        
        logger.info(f"Conversion complete. Output written to {output_filename}")
