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
