"""Curate RAIS data."""

from sqlalchemy.dialects.sqlite import INTEGER, TEXT, FLOAT, BOOLEAN
from sqlalchemy import Table, Column, MetaData
from sqlalchemy import create_engine, inspect
from utils import curate
from tqdm import tqdm
import pandas as pd
import re
import os

# Set working directory
CW_PATH = f'raw'
os.chdir(CW_PATH)

# Create engine
ENGINE = create_engine('sqlite:///rais.db')

# Models
metadata = MetaData()

# Pandas datatypes to SQLite
datatype_dict = {
    'int': INTEGER,
    'float': FLOAT,
    'str': TEXT,
    'bool': BOOLEAN
}

# Create a list of SQLAlchemy Column objects based on the column names
columns = [
    Column(name, datatype_dict.get(datatype))
    for name, datatype
    in curate.table_schema['rais'].items()
]

# Table name
table_name = 'rais_teste'

# Define the table with the columns, including an auto-increment ID if needed
rais = Table(
    table_name,
    metadata,
    Column('id', INTEGER, primary_key=True, autoincrement=True),
    Column('year', INTEGER),
    *columns
)

# Initialize the inspector to query the database schema
inspector = inspect(ENGINE)

# Create table if it doesn't exist
if not (table_name in inspector.get_table_names()):
    metadata.create_all(ENGINE)

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
                            name=table_name # product.lower() + '_bronze'
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
