from flask import Flask, request, jsonify
import sqlite3
import pandas as pd
from collections import OrderedDict

app = Flask(__name__)

# HOME
@app.route("/")
def home():
    return "✅ API de ventas está activa."

# ENDPOINT 1: Buscar ventas por vendedor y año
@app.route("/ventas_vendedor", methods=["GET"])
def ventas_vendedor():
    try:
        nombre = request.args.get("vendedor")
        año = int(request.args.get("año"))

        conn = sqlite3.connect("ventas.db")
        df = pd.read_sql_query("SELECT * FROM ventas", conn)
        conn.close()

        total = df[
            (df["Vendedor"].str.lower() == nombre.lower()) & (df["year"] == año)
        ]["Venta Total"].sum()

        return jsonify({
            "vendedor": nombre,
            "año": año,
            "venta_total": round(float(total), 2)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ENDPOINT 2 Buscar x Nit
@app.route("/buscar_por_nit", methods=["GET"])
def buscar_por_nit():
    try:
        nit = request.args.get("nit")
        if not nit:
            return jsonify({"error": "Debes proporcionar un NIT para buscar"}), 400

        conn = sqlite3.connect("ventas.db")
        df = pd.read_sql_query("SELECT * FROM ventas", conn)
        conn.close()

        # Filtrar por NIT
        df_filtrado = df[df["Nit"] == nit]

        if df_filtrado.empty:
            return jsonify([])

        resultados = []
        for _, fila in df_filtrado.iterrows():
            fila_resultado = OrderedDict()

            # --- Paso 1: agregar primero las columnas principales ---
            columnas_principales = df.columns[:6]
            for col in columnas_principales:
                fila_resultado[col] = fila[col]

            # --- Paso 2: agrupar dinámicamente ---
            columnas_dinamicas = df.columns[6:]
            agrupado = {}

            for col in columnas_dinamicas:
                valor = fila[col]
                if pd.notnull(valor):
                    valor = int(valor)
                    if valor not in agrupado:
                        agrupado[valor] = []
                    agrupado[valor].append(col)

            # --- Paso 3: agregar los valores dinámicos ordenadamente ---
            for valor in sorted(agrupado.keys()):
                fila_resultado[str(valor)] = agrupado[valor]

            resultados.append(fila_resultado)

        return jsonify(resultados)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# Configuración para correr en Render
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
