import pandas as pd
from pathlib import Path
import sqlite3

staging_db = "./Staging/staging.db"
storage_db = "./Storage/storage.db"

def carregar_fontes():
    conn_stg = sqlite3.connect(staging_db)

    with open("./sql/centrocusto.sql", "r") as file:
        sql_centro_custo = file.read()    
        df_centro_custo = pd.read_sql_query(sql_centro_custo, conn_stg)

    with open("./sql/resultado.sql", "r") as file:
        sql_resultado = file.read()    
        df_resultado = pd.read_sql_query(sql_resultado, conn_stg)

    conn_stg.close()
    return df_centro_custo, df_resultado

def copia_com_pandas(df: pd.DataFrame, table_name: str, db_path: Path):
    # cria a conexao com o banco de dados SQLite
    conn = sqlite3.connect(db_path)
    
    try:
        # escreve o DataFrame na tabela especificada
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"DataFrame copied to table '{table_name}' in database '{db_path}'.")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
    finally:
        # fecha a conexao com o banco de dados
        conn.close()


def transformar_resultado(df: pd.DataFrame):
    # Exemplo de transformação: adicionar uma coluna calculada
    df['DESC_INCONDICIONAL'] = df['FAT_BRUTO'] - df['FAT_LIQUIDO']
    df['VALOR_IMPOSTOS'] = df['FAT_LIQUIDO'] * 0.0665  # pis cofins iss
    df['TICKET_BRUTO'] = df['FAT_BRUTO'] / df['QTD_ALUNO']
    df['TICKET_LIQUIDO'] = df['FAT_LIQUIDO'] / df['QTD_ALUNO']
    df['MARGEM_OPERACIONAL'] = (((df['FAT_LIQUIDO'] - (df['VALOR_DESPESAS'] + df['VALOR_IMPOSTOS'] + df['VALOR_PESSOAL'])) / df['FAT_LIQUIDO']) * 100)
    return df

def construir_dim_tempo(df_resultado):
    df_datas = pd.to_datetime(df_resultado["COMPETENCIA"]).drop_duplicates().to_frame(name="data")
    df_datas = df_datas.sort_values("data").reset_index(drop=True)

    # Cria colunas de ano, mes, dia e dia da semana
    df_datas["ano"] = df_datas["data"].dt.year
    df_datas["mes"] = df_datas["data"].dt.month
    df_datas["dia"] = df_datas["data"].dt.day
    df_datas["dia_semana"] = df_datas["data"].dt.day_name(locale="pt_BR") if hasattr(df_datas["data"].dt, "day_name") else df_datas["data"].dt.dayofweek

    # Cria SK da dimensão de tempo
    #df_dim_tempo = df_datas.copy()
    #df_dim_tempo.insert(0, "sk_tempo", range(1, len(df_dim_tempo) + 1))

    return df_datas

if __name__ == '__main__':
    df_centro_custo, df_resultado = carregar_fontes()

    copia_com_pandas(df_centro_custo, "DIM_CENTRO_CUSTO", Path(storage_db))

    df_resultado = transformar_resultado(df_resultado)
    copia_com_pandas(df_resultado, "FATO_RESULTADO", Path(storage_db))   

    df_dim_tempo = construir_dim_tempo(df_resultado)
    copia_com_pandas(df_dim_tempo, "DIM_TEMPO", Path(storage_db))
