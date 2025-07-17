import pandas as pd
import unicodedata

def cargar_datos(filepath):
    # lee con utf-8 y reemplaza caracteres no decodificables con el caracter �
    return pd.read_csv(filepath, encoding='utf-8')

def quitar_tildes(texto):
    if isinstance(texto, str):
        nfkd = unicodedata.normalize('NFKD', texto)
        return "".join([c for c in nfkd if not unicodedata.combining(c)])
    return texto

def limpiar_texto(texto):
    texto = quitar_tildes(texto)
    if isinstance(texto, str):
        texto = texto.replace('�', '')  # quita el caracter rombo blanco
    return texto

def limpiar_dataframe(df):
    # limpia cada celda y nombre de columna
    df.columns = [limpiar_texto(col) for col in df.columns]
    return df.applymap(limpiar_texto)

def analisis_exploratorio(df):
    print("\n--- Estadisticas descriptivas ---")
    print(df.describe(include='all'))
    print("\n--- Informacion general ---")
    print(df.info())
    print("\n--- Primeras filas ---")
    print(df.head())

def identificar_problemas(df):
    print("\n--- Valores faltantes por columna ---")
    print(df.isnull().sum())
    
    print("\n--- Registros duplicados ---")
    print(f"Duplicados: {df.duplicated().sum()}")
    
    print("\n--- Outliers por columna numerica (basado en IQR) ---")
    numeric_cols = df.select_dtypes(include='number').columns
    for col in numeric_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
        print(f"{col}: {outliers} posibles outliers")

def limpiar_datos(df):
    df_clean = df.drop_duplicates()
    
    for col in df_clean.columns:
        if df_clean[col].isnull().sum() > 0:
            if df_clean[col].dtype == 'object':
                df_clean[col].fillna(df_clean[col].mode()[0], inplace=True)
            else:
                df_clean[col].fillna(df_clean[col].median(), inplace=True)
    return df_clean

def guardar_datos(df, output_path):
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"\nDatos limpios guardados en '{output_path}'")

def documentacion():
    doc = """
    En este analisis exploratorio inicial se cargaron los datos tratando caracteres incorrectos.
    Se eliminaron las tildes y caracteres especiales (como el rombo blanco �) de todas las celdas y nombres de columna.
    Se calcularon estadisticas descriptivas, se identificaron valores faltantes, duplicados y posibles outliers.
    La limpieza incluyo eliminar duplicados y rellenar valores faltantes.
    El conjunto limpio fue guardado para su uso futuro.
    """
    print(doc)


if __name__ == "__main__":
    path = "polizas_full.csv"
    output_path = "polizas_full_clean.csv"
    
    df = cargar_datos(path)
    df = limpiar_dataframe(df)
    
    analisis_exploratorio(df)
    identificar_problemas(df)
    
    df_clean = limpiar_datos(df)
    guardar_datos(df_clean, output_path)
    
    documentacion()
