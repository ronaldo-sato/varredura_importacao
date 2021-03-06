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
sys.path.insert(1, '\\'.join(os.getcwd().split('\\')[:-1]))

from __path__ import prod as path
from __path__ import erro as folder
from __func__ import decrypt
from __access__ import prod as access
from __access__ import schema


def list_files(interval, path, folder='HIST/ERRO', pattern='*_gz'):
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
        f'{decrypt(access)}',
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
    rname = _input['report_name']
    path2save = _input['path2save']

    # Leitura Tabela Arquivos Importados.

    print('\nLeitura dos arquivos importados. (Aguarde..)\n')

    df = load_raw_data(date_ini, date_end, schema=schema)

    # Nome do relatório.
    if not rname:

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

    # Varrendo o path.

    entries = os.scandir(path)

    with open(rname, 'w+') as frel:

        header = (
            'Relatório de verificação de falha de importação'
            f' de arquivos presentes em "{folder}".\n'
            f'\nVarredura para cada unidade do caminho "{path}".\n'
            f'\nPeríodo (intervalo aberto): ]{date_ini}, {date_end}[.\n')

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

                    frel.write(f'\n\n{entry.name}\n')
                    
                    fnames = list_files(
                        [date_ini, date_end], entry.path, folder)

                    if fnames:

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

                                try:

                                    os.rename(fname, destination)

                                except FileExistsError:

                                    print(f' -> Já existe em /HIST\n')

                                    frel.write(
                                        f'\n{fname.replace(path, "")}'
                                        ' -> '
                                        'já existe em'
                                        f' {destination.replace(path, "")}')

                                else:

                                    print(f' -> Movendo para /HIST\n')

                                    frel.write(
                                        f'\n{fname.replace(path, "")}'
                                        ' -> '
                                        f'{destination.replace(path, "")}')

                    else:

                        print('Não foi encontrado nenhum arquivo.')
                        
                        frel.write(
                            '\nNão foi encontrado nenhum arquivo.\n')

    if path2save:

        if os.path.exists(path2save):

            print(f'Relatório "{rname}" salvo em "{path2save}".')

        else:

            _dir = os.path.join(os.getcwd(), 'output/').replace('\\', '/')

            print(
                f'O caminho "{path2save}" não existe.\n'
                f'\nRelatório "{rname}" salvo localmente em "{_dir}".')

    else:

        _dir = os.path.join(os.getcwd(), 'output/').replace('\\', '/')

        print(
            f'Relatório "{rname}" salvo localmente em "{_dir}".')
