"""."""

import pandas as pd
import unicodedata
import py7zr
from tqdm import tqdm
import re

# Variables
table_schema = {
    'rais': {
        'bairros_sp': 'int',
        'bairros_fortaleza': 'int',
        'bairros_rj': 'int',
        'causa_afastamento_1': 'int',
        'causa_afastamento_2': 'int',
        'causa_afastamento_3': 'int',
        'motivo_desligamento': 'int',
        'cbo_ocupacao_2002': 'int',
        'cnae_20_classe': 'int',
        'cnae_95_classe': 'int',
        'distritos_sp': 'int',
        'vinculo_ativo_31_12': 'bool',
        'faixa_etaria': 'int',
        'faixa_hora_contrat': 'int',
        'faixa_remun_dezem_sm': 'int',
        'faixa_remun_media_sm': 'int',
        'faixa_tempo_emprego': 'int',
        'escolaridade_apos_2005': 'int',
        'qtd_hora_contr': 'int',
        'idade': 'int',
        'ind_cei_vinculado': 'bool',
        'ind_simples': 'bool',
        'mes_admissao': 'int',
        'mes_desligamento': 'int',
        'mun_trab': 'int',
        'municipio': 'int',
        'nacionalidade': 'int',
        'natureza_juridica': 'int',
        'ind_portador_defic': 'bool',
        'qtd_dias_afastamento': 'int',
        'raca_cor': 'int',
        'regioes_adm_df': 'int',
        'vl_remun_dezembro_nom': 'float',
        'vl_remun_dezembro_sm': 'float',
        'vl_remun_media_nom': 'float',
        'vl_remun_media_sm': 'float',
        'cnae_20_subclasse': 'int',
        'sexo_trabalhador': 'int',
        'tamanho_estabelecimento': 'int',
        'tempo_emprego': 'float',
        'tipo_admissao': 'int',
        'tipo_estab': 'int',
        'tipo_estab1': 'str',
        'tipo_defic': 'int',
        'tipo_vinculo': 'int',
        'ibge_subsetor': 'int',
        'vl_rem_janeiro_sc': 'float',
        'vl_rem_fevereiro_sc': 'float',
        'vl_rem_marco_sc': 'float',
        'vl_rem_abril_sc': 'float',
        'vl_rem_maio_sc': 'float',
        'vl_rem_junho_sc': 'float',
        'vl_rem_julho_sc': 'float',
        'vl_rem_agosto_sc': 'float',
        'vl_rem_setembro_sc': 'float',
        'vl_rem_outubro_sc': 'float',
        'vl_rem_novembro_sc': 'float',
        'ano_chegada_brasil': 'int',
        'ind_trab_intermitente': 'bool',
        'ind_trab_parcial': 'bool'
    }
}


def normalize_column_name(col_name):
    """Normalize column name."""
    # Convert to lower case
    col_name = col_name.lower()
    # Replace accented characters with their unicode equivalent
    col_name = unicodedata.normalize('NFKD', col_name).encode('ASCII', 'ignore').decode('utf-8')
    # Replace spaces and slashes with underscores
    col_name = re.sub(r"[ /]", "_", col_name)
    # Remove other special characters (only allow alphanumeric and underscores)
    col_name = re.sub(r"[^a-zA-Z0-9_]", "", col_name)

    return col_name


def format(temp_data, product):
    """Format table, rename columns and adjust datatypes."""
    # Rename columns and adjust datatypes
    temp_data = (
        temp_data
        .rename(columns=lambda col: normalize_column_name(col))
    )

    # Replace NA columns to -1
    # Pandas can't convert columns to int if there's NA values
    int_columns = [key for key, value in table_schema[product].items() if value == 'int']
    temp_data[int_columns] = temp_data[int_columns].fillna(-1)

    # Convert datatypes
    temp_data = temp_data.astype(table_schema[product])

    return temp_data


def extract_file(file_path, output_path):
    """Extract from 7z compacted file."""
    # Open file in read mode
    archive = py7zr.SevenZipFile(file_path, mode='r')
    # Extract file
    archive.extractall(path=output_path)
    # Close file
    archive.close()

    return True
