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

    # Filtrar por NIT
    df_filtrado = df[df["Nit"] == nit]

    # Ordenar primero por 'Cod' y luego por 'Suc'
    df_ordenado = df_filtrado.sort_values(by=["Cod", "Suc"])

    # Convertir a lista de diccionarios
    resultado = df_ordenado.to_dict(orient="records")

    return jsonify(resultado)
