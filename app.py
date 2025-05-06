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

# ENDPOINT 3 BUSCAR X RAZON SOCIAL 
@app.route('/buscar_cliente', methods=['GET'])
def buscar_cliente():
    try:
        nombre = request.args.get('nombre')
        if not nombre:
            return jsonify({"error": "Parámetro 'nombre' es obligatorio"}), 400

        tipo_busqueda = request.args.get('tipo', 'all')  # 'all' o 'any'
        operador = " AND " if tipo_busqueda == 'all' else " OR "

        palabras = nombre.strip().lower().split()
        if not palabras:
            return jsonify({"error": "No se ingresaron palabras relevantes"}), 400

        condiciones = [
            "REPLACE(REPLACE(LOWER(`Razon Social`), '-', ''), '/', '') LIKE ?"
            for _ in palabras
        ]
        condicion_final = operador.join(condiciones)
        parametros = [f"%{p.replace('-', '').replace('/', '')}%" for p in palabras]

        conn = sqlite3.connect('ventas.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Buscar todos los registros que coincidan con el nombre (máx 5 NIT distintos)
        param_orden = nombre.lower()
        cursor.execute(f"""
            SELECT DISTINCT `Nit`
            FROM ventas
            WHERE {condicion_final}
            LIMIT 5
        """, parametros)
        nits = [row["Nit"] for row in cursor.fetchall()]

        if not nits:
            return jsonify({"mensaje": "No se encontraron coincidencias"}), 404

        # Diccionario de traducción de valores con íconos
        nombres_grupo = {
            3: "Dptos sin Venta (🔴 esfera roja)",
            4: "Dptos perdidos (⚫ esfera negra)",
            5: "Dptos Venta estable (🟢 esfera verde)",
            6: "Dptos venta recuperadas o nuevas (✅🟢 esfera verde con check)"
        }

        resultados = []

        for nit in nits:
            cursor.execute("SELECT * FROM ventas WHERE Nit = ?", (nit,))
            filas = cursor.fetchall()

            for fila in filas:
                fila_resultado = OrderedDict()

                # Datos principales
                fila_resultado["Cod"] = fila["Cod"]
                fila_resultado["Nit"] = fila["Nit"]
                fila_resultado["Razon Social"] = fila["Razon Social"]
                fila_resultado["Suc"] = fila["Suc"]
                fila_resultado["Vendedor"] = fila["Vendedor"]
                fila_resultado["year"] = fila["year"]

                # Agrupar columnas dinámicas
                agrupado = {}
                columnas_dinamicas = fila.keys()[6:]  # desde la séptima en adelante
                for col in columnas_dinamicas:
                    valor = fila[col]
                    if pd.notnull(valor):
                        valor = int(valor)
                        nombre_col = col.lstrip("D")  # eliminar letra D
                        if valor not in agrupado:
                            agrupado[valor] = []
                        agrupado[valor].append(nombre_col)

                for valor in sorted(agrupado.keys()):
                    nombre = nombres_grupo.get(valor, f"Grupo {valor}")
                    fila_resultado[nombre] = agrupado[valor]

                resultados.append(fila_resultado)

        conn.close()
        return app.response_class(
            json.dumps(resultados, ensure_ascii=False, indent=2),
            mimetype="application/json"
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ENDPOINT 4: Buscar clientes únicos por Cod
@app.route("/clientes_por_cod", methods=["GET"])
def clientes_por_cod():
    try:
        cod = request.args.get("cod")
        if cod is None:
            return jsonify({"error": "Parámetro 'cod' es obligatorio"}), 400

        # Convertir el parámetro a entero
        try:
            cod_int = int(float(cod))
        except ValueError:
            return jsonify({"error": "El parámetro 'cod' debe ser un número"}), 400

        conn = sqlite3.connect("ventas.db")
        df = pd.read_sql_query("SELECT Cod, Nit, `Razon Social` FROM ventas", conn)
        conn.close()

        # Convertir Cod a entero antes de filtrar
        df["Cod"] = df["Cod"].astype("Int64")  # admite nulos
        df_filtrado = df[df["Cod"] == cod_int]

        df_unicos = df_filtrado[["Nit", "Razon Social"]].drop_duplicates()

        return jsonify(df_unicos.to_dict(orient="records"))

    except Exception as e:
        return jsonify({"error": str(e)}), 500

#ENDPOINT 5 BUSCAR CLIENTES X DPTOS
@app.route("/clientes_por_departamento", methods=["GET"])
def clientes_por_departamento():
    try:
        depto = request.args.get("departamento")
        esfera = request.args.get("esfera")
        cod = request.args.get("cod")

        if not depto or not esfera:
            return jsonify({"error": "Se requiere 'departamento' y 'esfera'"}), 400

        # Normalizar valores
        columna = f"D{int(depto):02d}"  # ejemplo: 76 → D76
        valor_esfera = int(esfera)
        cod_vendedor = int(cod) if cod else None

        # Diccionario visual
        mapa_esferas = {
            3: "esfera roja (🔴)",
            4: "esfera negra (⚫)",
            5: "esfera verde (🟢)",
            6: "esfera verde con check (✅)"
        }
        descripcion = mapa_esferas.get(valor_esfera, f"esfera tipo {valor_esfera}")

        conn = sqlite3.connect("ventas.db")
        df = pd.read_sql_query("SELECT * FROM ventas", conn)
        conn.close()

        if columna not in df.columns:
            return jsonify({"error": f"Departamento '{columna}' no encontrado en columnas"}), 400

        # Convertir columna a entero antes de filtrar
        df[columna] = pd.to_numeric(df[columna], errors="coerce").fillna(0).astype(int)

        # Filtrar por esfera
        df_filtrado = df[df[columna] == valor_esfera]

        # Filtro opcional por código de vendedor
        if cod_vendedor is not None:
            df_filtrado = df_filtrado[df_filtrado["Cod"].astype(str) == str(cod_vendedor)]

        if df_filtrado.empty:
            return jsonify([])

        # Formatear resultado
        resultado = []
        for _, row in df_filtrado.iterrows():
            resultado.append({
                "Nit": row["Nit"],
                "Razon_Social": row["Razon Social"],
                "Esfera": descripcion
            })

        return jsonify(resultado)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

       
# Configuración para correr en Render
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
