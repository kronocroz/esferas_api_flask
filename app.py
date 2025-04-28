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

    # Agrupar por Cod
    resultado_final = {}

    for _, fila in df_ordenado.iterrows():
        cod = fila["Cod"]
        suc = fila["Suc"]

        # Inicializar grupo si no existe
        if cod not in resultado_final:
            resultado_final[cod] = {
                "Cod": cod,
                "Sucursales": []
            }

        # Procesar datos de D04 en adelante (solo si ≠ 3)
        datos_sucursal = {"Suc": suc}
        for col in columnas_d04_en_adelante:
            valor = fila[col]
            if pd.notnull(valor) and valor != 3:
                datos_sucursal[col] = valor

        resultado_final[cod]["Sucursales"].append(datos_sucursal)

    # Convertir a lista para jsonify
    return jsonify(list(resultado_final.values()))

