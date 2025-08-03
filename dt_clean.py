import os
import pandas as pd
from deltalake import write_deltalake, DeltaTable
from histdata.api import download_hist_data

from fx_logging import get_project_logger

# Setup logging
logger = get_project_logger(__name__)


def main():
    output = os.environ.get("FX_DATA_OUTPUT", 'output')
    dt = DeltaTable(output)
    partitions = pd.DataFrame(dt.partitions())
    pairs = partitions.pair.unique()

    # REMOVE DUPLICATES
    for pair in pairs:
        logger.info(f"Removing duplicates for {pair}")

        df = dt.to_pandas(partitions=[('pair', '=', pair)])
        df2 = df.drop_duplicates(subset=['date', 'pair']).reset_index(drop=True)

        number_of_duplicates = df.shape[0] - df2.shape[0]
        logger.info(f"  Found {number_of_duplicates} duplicates for {pair}.\nShape before: {df.shape}, after: {df2.shape}")

        write_deltalake(
            table_or_uri=dt,
            data=df2,
            mode='overwrite',
            predicate=f"pair = '{pair}'",
            partition_by=['pair', 'year'],
        )

        logger.info(f"  Removed duplicates for {pair}")

    # Verify that there are no duplicates left
    for pair in pairs:
        df = dt.to_pandas(partitions=[('pair', '=', pair)])
        duplicates = df.duplicated(subset=['date', 'pair']).sum()
        if duplicates > 0:
            logger.error(f"Error: Found {duplicates} duplicates for {pair} after cleanup!")
        else:
            logger.info(f"No duplicates found for {pair} after cleanup.")

    # COMPACT DELTA TABLE
    # We could compact in a single call, but I'm not sure how much memory it will use
    for pair in pairs:
        dt.optimize.compact(partition_filters=[('pair', '=', pair)])
    dt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=True)
    dt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)

    

if __name__ == "__main__":
    main()