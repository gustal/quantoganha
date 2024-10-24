"""Download RAIS files from MTP."""

from utils import curate
import ftplib
import os

# Parameters
selected_year = [2022]
RAW_PATH = 'raw/'

# Login on ftp server
ftp = ftplib.FTP("ftp.mtps.gov.br", user="anonymous", passwd="", acct="", timeout=1e6)

# Change directory to RAIS folder
ftp.cwd("pdet/microdados/RAIS")

# List folders
pastas = ftp.nlst()

# Create directory to save RAIS files
os.makedirs(RAW_PATH + "RAIS", exist_ok=True)

# Convert years to string
selected_year = [str(x) for x in selected_year]

# Loop para cada pasta do FTP
for pasta in pastas:
    if pasta in selected_year:
        # Create year folder
        os.makedirs(RAW_PATH + f"RAIS/{pasta}", exist_ok=True)

        # List files inside year folder
        # If connection was lost the request will return an error, this is handled by the try/except blocks.
        try:
            files = ftp.nlst(pasta)

        except ConnectionError:
            # Connection might be closed after a while, so reconnect to server
            ftp = ftplib.FTP("ftp.mtps.gov.br", user="anonymous", passwd="", acct="", timeout=1e6)
            ftp.cwd("pdet/microdados/RAIS")
            files = ftp.nlst(pasta)

        for file in files:
            # Skip if extract file exists in output folder
            if not os.path.exists(RAW_PATH + f'RAIS/{pasta}/{file.replace("7z", "txt")}'):
                # Download file
                try:
                    with open(RAW_PATH + f'RAIS/{pasta}/{file}', 'wb') as arquivo_destino:
                        # recupere o binário "caminho do binário no ftp e escreva em arquivo_destino
                        ftp.retrbinary(f'RETR {pasta}/{file}', arquivo_destino.write)

                except ConnectionError:
                    ftp = ftplib.FTP("ftp.mtps.gov.br", user="anonymous", passwd="", acct="", timeout=1e6)
                    ftp.cwd("pdet/microdados/RAIS")

                    with open(RAW_PATH + f'RAIS/{pasta}/{file}', 'wb') as arquivo_destino:
                        ftp.retrbinary(f'RETR {pasta}/{file}', arquivo_destino.write)

                # Extract file
                try:
                    curate.extract_file(
                        file_path=RAW_PATH + f'RAIS/{pasta}/{file}',
                        output_path=RAW_PATH + f'RAIS/{pasta}'
                    )

                except Exception:
                    pass

# Close ftp server
ftp.close()
