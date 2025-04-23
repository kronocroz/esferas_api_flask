import pandas as pd
import sqlite3
import os

# Lista de archivos a convertir (ajusta los nombres aquí)
archivos = [
    "esferas_med.xlsx",     
    "esferas_bog.xlsx",
    "esferas_cal.xlsx",
    "esferas_baq.xlsx",
    "esferas_car.xlsx"
    
]

# Ruta donde están los archivos y se guardará la base de datos
ruta_base = "D:/Desktop/MiAPIFlask2/"
datos_combinados = pd.DataFrame()

for archivo in archivos:
    ruta = os.path.join(ruta_base, archivo)
    if not os.path.exists(ruta):
        print(f"⚠️ Archivo no encontrado: {ruta}")
        continue

    print(f"📥 Leyendo archivo: {archivo}")
    df = pd.read_excel(ruta)

    # Convertir nombres de columnas a texto y limpiar
    df.columns = df.columns.map(str)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed|^nan$', case=False, na=False)]

    # Renombrar columnas que son solo números
    df.columns = [
    f"c_{col}" if col.isdigit() else col
    for col in df.columns]

    # Renombrar columnas duplicadas si las hay
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        cols[cols[cols == dup].index.values.tolist()] = [f"{dup}_{i}" for i in range(sum(cols == dup))]
    df.columns = cols

    datos_combinados = pd.concat([datos_combinados, df], ignore_index=True)

# Limitar a las primeras 60 columnas
datos_combinados = datos_combinados.iloc[:, :60]

# Guardar en SQLite
print("💾 Guardando base de datos en SQLite...")
conn = sqlite3.connect(os.path.join(ruta_base, "ventas.db"))
datos_combinados.to_sql(name="ventas", con=conn, if_exists="replace", index=False)
conn.close()
print("✅ Base de datos 'ventas.db' creada exitosamente con 60 columnas.")