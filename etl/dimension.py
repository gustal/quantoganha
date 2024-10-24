"""Create dimension tables."""

from sqlalchemy import create_engine
from tqdm import tqdm
import pandas as pd
import re
import os

# Create engine
ENGINE = create_engine('sqlite:///raw/rais.db')

# List files
files = os.listdir('./raw/dim/input_table')

# Iterate over files
for file in tqdm(files):
    # Extract info from name
    version = re.search('.*_v(.*).csv', file)[1]
    table_name = re.search('(.*)_v.*.csv', file)[1]

    # Read csv
    temp_csv = pd.read_csv(
        './raw/dim/input_table/' + file,
        sep=','
    )

    # Add version
    temp_csv = temp_csv.assign(version=version)

    # Write dimension table
    _ = temp_csv.to_sql(
        con=ENGINE,
        index=False,
        if_exists='replace',
        name='dim_' + table_name
    )
