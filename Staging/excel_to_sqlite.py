import sqlite3
import pandas as pd
from pathlib import Path

def excel_to_sqlite(excel_path: str, sqlite_db_path: str, table_name: str,
                    sheet_name=0, if_exists='fail', index=False, dtype_overrides: dict = None):
    """
    Lê excel e insere em banco sqlite.
    - excel_path: caminho do arquivo .xlsx
    - sqlite_db_path: caminho do arquivo .db (cria se não existir)
    - table_name: nome da tabela no sqlite
    - sheet_name: nome da planilha ou índice (padrão 0)
    - if_exists: 'fail' | 'replace' | 'append' (padrão 'fail')
    - index: se True, grava o índice do DataFrame como coluna
    - dtype_overrides: dict {col_name: sqlalchemy_type_or_str} (opcional)
    """

    # Ler planilha
    print(f"Lendo Excel: {excel_path} (sheet={sheet_name}) ...")
    df = pd.read_excel(excel_path, sheet_name=sheet_name, engine='openpyxl')

    if df.empty:
        print("A planilha está vazia. Nada a inserir.")
        return

    # Conectar SQLite
    conn = sqlite3.connect(sqlite_db_path)

    try:
        # pandas.DataFrame.to_sql usa o sqlite3.Connection sem sqlalchemy para operações simples
        # if_exists: 'fail' (padrão) | 'replace' | 'append'
        print(f"Inserindo dados na tabela '{table_name}' no banco '{sqlite_db_path}' (if_exists={if_exists}) ...")
        df.to_sql(table_name, conn, if_exists=if_exists, index=index)
        print("Inserção concluída com sucesso.")
    except Exception as e:
        print("Erro ao inserir dados:", e)
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    # parâmetros padrão; mude conforme necessário
    sheet_name = 0
    if_exists = 'replace'   # pode ser 'replace' para recriar, ou 'append' para acrescentar
    index = False

    BASE_DIR = Path(__file__).resolve().parent   # pasta /Staging
    excel_path = BASE_DIR.parent / "Fonte_Dados" / "Centro_Custo.xlsx"
    excel_to_sqlite(excel_path, "./Staging/Staging.db", "CENTRO_CUSTO", sheet_name=sheet_name, if_exists=if_exists, index=index)

    excel_path = BASE_DIR.parent / "Fonte_Dados" / "Despesas.xlsx"
    excel_to_sqlite(excel_path, "./Staging/Staging.db", "DESPESA", sheet_name=sheet_name, if_exists=if_exists, index=index)

    excel_path = BASE_DIR.parent / "Fonte_Dados" / "Faturamento_09_10_2025.xlsx"
    excel_to_sqlite(excel_path, "./Staging/Staging.db", "FATURAMENTO", sheet_name=sheet_name, if_exists=if_exists, index=index)
