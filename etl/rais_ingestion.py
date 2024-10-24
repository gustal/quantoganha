"""Curate RAIS data."""

from sqlalchemy import create_engine
from utils import curate
from tqdm import tqdm
import pandas as pd
import unicodedata
import re
import os

# Set working directory
CW_PATH = f'raw'
os.chdir(CW_PATH)

# Create engine
ENGINE = create_engine('sqlite:///rais.db')

# Products
products = ['RAIS']

for product in products:
    # Years
    folders = os.listdir(product)

    for folder in folders:
        year = int(folder)
        files = os.listdir(product + '/' + folder)

        for file in files:
            if re.match('.*.txt', file):
                if re.search('.*_VINC_.*', file):
                    file_full_path = f'{product}/{folder}/{file}'

                    chunksize = 500000
                    chunks = pd.read_table(
                        filepath_or_buffer=file_full_path,
                        chunksize=chunksize,
                        encoding='latin-1',
                        sep=';',
                        decimal=',',
                        na_values=['{ñ', 'N/A', 'nan', '']
                    )

                    filesize = os.path.getsize(file_full_path)
                    pbar = tqdm(chunks, desc=f'Running: {file_full_path}', total=(((filesize / 1000) / 0.48) / chunksize))
                    for chunk in pbar:
                        # Format data
                        chunk = curate.format(
                            temp_data=chunk,
                            product=product.lower()
                        )

                        # Add year to chunk
                        chunk = chunk.assign(year=year)

                        # Send to SQLite database
                        chunk.to_sql(
                            con=ENGINE,
                            index=False,
                            if_exists='append',
                            chunksize=16000,
                            name=product.lower() + '_bronze'
                        )

                    # Write log
                    log = {
                        'file': file_full_path,
                        'sent_to_db': True,
                        'is_success': True
                    }

                    pd.DataFrame([log]).to_sql(
                        con=ENGINE,
                        index=False,
                        if_exists='append',
                        name='log'
                    )
