import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Caminho do banco
storage_db = "./Storage/storage.db"

def carregar_fontes():
    conn_stg = sqlite3.connect(storage_db)

    with open("./sql/view_resultado.sql", "r") as file:
        sql_resultado = file.read()    
        df_resultado = pd.read_sql_query(sql_resultado, conn_stg)

    return df_resultado

def grf_total_ccusto(df_resultado):
    grupo = df_resultado.groupby('DESCRICAO')['FAT_LIQUIDO'].sum().reset_index()
    plt.figure(figsize=(10,5))
    grupo.plot(kind="bar")
    plt.title("Resultado por Centro de Custo")
    plt.xlabel("Centro de Custo")
    plt.ylabel("Valor")
    plt.tight_layout()
    plt.show()

    df_resultado["COMPETENCIA"] = pd.to_datetime(df_resultado["COMPETENCIA"])
    grupo = df_resultado.groupby("COMPETENCIA")["FAT_LIQUIDO"].sum()

    grupo.plot(kind="line", figsize=(10,5))
    plt.title("Evolução Mensal")
    plt.ylabel("Valor")
    plt.show()

if __name__ == '__main__':
    df_resultado = carregar_fontes()
    grf_total_ccusto(df_resultado)

    df_resultado.to_excel("./Visualizador/resultado.xlsx", index=False)
    print("Arquivo resultado.xlsx gerado com sucesso.")



