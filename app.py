from flask import Flask, request, jsonify
import sqlite3
import pandas as pd
from collections import OrderedDict
import json

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

        # Diccionario de traducción actualizado con íconos
        nombres_grupo = {
            3: "Dptos sin Venta 🔴",
            4: "Dptos perdidos ⚫",
            5: "Dptos Venta estable 🟢",
            6: "Dptos venta recuperadas o nuevas ✅"
        }

        resultados = []
        for _, fila in df_filtrado.iterrows():
            fila_resultado = OrderedDict()

            # Agregar primero las columnas principales
            columnas_principales = df.columns[:6]
            for col in columnas_principales:
                fila_resultado[col] = fila[col]

            # Agrupar dinámicamente
            columnas_dinamicas = df.columns[6:]
            agrupado = {}

            for col in columnas_dinamicas:
                valor = fila[col]
                if pd.notnull(valor):
                    valor = int(valor)
                    if valor not in agrupado:
                        agrupado[valor] = []
                    
                    # 🔥 Aquí limpiamos la "D" del nombre de columna
                    nombre_limpio = col.lstrip('D')  # elimina la letra D solo si está al principio
                    agrupado[valor].append(nombre_limpio)

            # Añadir agrupaciones con nombres en lugar de números
            for valor in sorted(agrupado.keys()):
                nombre = nombres_grupo.get(valor, f"Grupo {valor}")
                fila_resultado[nombre] = agrupado[valor]

            resultados.append(fila_resultado)

        respuesta_json = json.dumps(resultados, ensure_ascii=False, indent=2)
        return app.response_class(respuesta_json, mimetype="application/json")

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Configuración para correr en Render
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
