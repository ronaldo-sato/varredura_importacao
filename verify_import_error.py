# -*- coding: utf-8 -*-
# author (unless omitted): Ronaldo Mitsuo Sato
# email:                   ronaldo.sato@gmail.com
# created:                 11/06/2021
# modified:                22/06/2021
# python version:          3

import os
# import pathlib
from datetime import datetime
from glob import glob
import re
from dateutil.parser import parse
import pandas as pd

import sys
sys.path.insert(0, os.getcwd())

from __aux__ import decrypt, path, folder, access, schema


def list_files(path, interval, folder='/HIST/ERRO', pattern='*_gz'):
    """
    Listagem de todos arquivos compactados (pattern) dentro do
    intervalo (interval), na pasta (folder) do caminho (path).
    """

    fpath = os.path.join(path, folder)

    fnames = glob(os.path.join(fpath, pattern))

    _interval = [parse(date, dayfirst=True) for date in interval]

    selected = []

    for fname in fnames:

        if not fname.split('\\')[-1][0].isdigit():

            continue

        else:

            fdate = datetime.strptime(
                re.search(r'(\d{4}-\d{2}-\d{2}-\d{2}-\d{2})',
                          fname).group(1),
                r'%Y-%m-%d-%H-%M')

            # Período de busca dos arquivos.
            # Intervalo aberto (devido a data no nome dos arquivos):
            # ]início, fim[
            if (fdate > _interval[0]) & (fdate < _interval[-1]):

                selected.append(fname.replace('\\', '/'))

    return selected


def load_raw_data(start, end, schema=''):
    
    import sqlalchemy as db

    engine = db.create_engine(
        f'oracle://{decrypt(access)}',
        max_identifier_length=128)

    connection = engine.connect()

    query = (
        "SELECT R.FILE_NAME,"
                " R.DT_ACQUISITION,"
                " R.STATUS_PROCESSING,"
                " R.FLG_REPROCESS_RAW_DATA"
        f" FROM {schema.upper()}.TB_RAW_DATA R"
        " WHERE R.DT_ACQUISITION >= TO_DATE("
            f"'{start}', 'DD/MM/YYYY HH24:MI:SS')"
        " AND R.DT_ACQUISITION <= TO_DATE("
            f"'{end}', 'DD/MM/YYYY HH24:MI:SS')"
        " ORDER BY R.DT_ACQUISITION ASC")

    df = pd.read_sql_query(
        query,
        connection,
        index_col='dt_acquisition')
    
    df.columns = df.columns.str.upper()

    return df


if __name__ == "__main__":

    import json

    with open(
        '\\'.join([os.getcwd(), 'input_verify_import_error.json'])
    ) as fjson:

        _input = json.load(fjson)

    date_ini = _input['date_ini']
    date_end = _input['date_end']
    path2save = _input['path2save']

    entries = os.scandir(path)

    # Leitura Tabela Arquivos Importados.

    print('\nLeitura dos arquivos importados. (Aguarde..)\n')

    df = load_raw_data(date_ini, date_end, schema=schema)

    # Nome do relatório.

    if (parse(date_ini, dayfirst=True).date()
        == parse(date_end, dayfirst=True).date()):

        rname = (
            'verificacao_importacao_'
            f'{parse(date_ini, dayfirst=True).strftime(r"%d%m%Y")}.txt')

    else:

        rname = (
            'verificacao_importacao_'
            f'{parse(date_ini, dayfirst=True).strftime(r"%d%m%Y")}'
            f'-{parse(date_end, dayfirst=True).strftime(r"%d%m%Y")}.txt')

    if path2save:

        if os.path.exists(path2save):

            rname = os.path.join(path2save, rname)

    else:

        if not os.path.exists('output/'):

            os.mkdir('output/')

        rname = os.path.join('output/', rname)

    with open(rname, 'w+') as frel:

        header = (
            'Relatório de verificação de falha de importação'
            f' de arquivos presentes em "{folder}".\n'
            f'\nVarredura para cada unidade do caminho "{path}".\n'
            f'\n Período (intervalo aberto): ]{date_ini}, {date_end}[.\n')

        frel.write(header)

        for entry in entries:

            if not entry.is_dir():

                continue

            elif not os.path.isdir(entry.path):

                continue

            else:

                print(f'\n{entry.name}\n')

                if os.path.exists(
                        os.path.join(entry.path, folder)):

                    fnames = list_files(
                        entry.path, folder, [date_ini, date_end])

                    if fnames:

                        frel.write(f'\n\n{entry.name}\n')

                        print('\n\tVerificando arquivos:\n')

                        for fname in fnames:

                            print(f'\n\t\t{fname}')

                            _fname = fname.split('/')[-1]

                            # source = fname

                            destination = re.sub('/ERRO', '', fname)

                            if (df['FILE_NAME'] == _fname).any():

                                print(' -> Já importado\n')

                                frel.write(
                                    f'\n{fname.replace(path, "")}'
                                    ' -> '
                                    'Já importado')

                            else:

                                print(f' -> Movendo para /HIST\n')

                                frel.write(
                                    f'\n{fname.replace(path, "")}'
                                    ' -> '
                                    f'{destination.replace(path, "")}')

                                os.rename(fname, destination)

                    else:

                        msg = '\nNão foi encontrado nenhum arquivo.'
                        
                        print(msg)

                        frel.write(msg)

    if path2save:

        if os.path.exists(path2save):

            print(f'Relatório "{rname}" salvo em "{path2save}".')

        else:

            print(
                f'O caminho "{path2save}" não existe.\n'
                f'\nRelatório "{rname}" salvo localmente em'
                f' "{os.getcwd()}".')

    else:

        print(
            f'Relatório "{rname}" salvo localmente em "{os.getcwd()}".')
