from flask import Flask, request, jsonify
import sqlite3
import pandas as pd

app = Flask(__name__)

@app.route("/")
def home():
    return "✅ API de ventas está activa."

@app.route("/ventas_vendedor", methods=["GET"])
def ventas_vendedor():
    nombre = request.args.get("vendedor")
    año = int(request.args.get("año"))

    conn = sqlite3.connect("ventas.db")
    df = pd.read_sql_query("SELECT * FROM ventas", conn)
    conn.close()

    total = df[
        (df["Vendedor"].str.lower() == nombre.lower()) & (df["Año"] == año)
    ]["Venta Total"].sum()

    return jsonify({
        "vendedor": nombre,
        "año": año,
        "venta_total": round(float(total), 2)
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

@app.route("/buscar_por_nit", methods=["GET"])
def buscar_por_nit():
    nit = request.args.get("nit")
    if not nit:
        return jsonify({"error": "Debes proporcionar un NIT para buscar"}), 400

    conn = sqlite3.connect("ventas.db")
    df = pd.read_sql_query("SELECT * FROM ventas", conn)
    conn.close()

    # Filtrar por NIT y Año 2025
    df_filtrado = df[(df["Nit"] == nit) & (df["Año"] == 2025)]

    if df_filtrado.empty:
        return jsonify([])

    # Ordenar por Cod y Suc
    df_ordenado = df_filtrado.sort_values(by=["Cod", "Suc"])

    # Seleccionar columnas desde D04 en adelante
    columnas_d04_en_adelante = [col for col in df_ordenado.columns if col >= "D04"]

    # Aplicar el filtro: dejar solo columnas donde los valores no sean 3
    datos_filtrados = []
    for _, fila in df_ordenado.iterrows():
        fila_resultado = {
            "Nit": fila["Nit"],
            "Cod": fila["Cod"],
            "Suc": fila["Suc"]
        }
        for col in columnas_d04_en_adelante:
            if fila[col] != 3:
                fila_resultado[col] = fila[col]
        datos_filtrados.append(fila_resultado)

    return jsonify(datos_filtrados)
