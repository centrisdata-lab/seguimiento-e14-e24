import os
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

app = Flask(__name__, static_folder=".")
CORS(app)

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS votos (
                    id SERIAL PRIMARY KEY,
                    municipio TEXT NOT NULL,
                    zona INTEGER NOT NULL,
                    cod_puesto INTEGER NOT NULL,
                    nom_puesto TEXT,
                    mesa INTEGER NOT NULL,
                    e14_ahora_camara INTEGER DEFAULT 0,
                    e14_ahora_senado INTEGER DEFAULT 0,
                    e24_ahora_camara INTEGER DEFAULT 0,
                    e24_ahora_senado INTEGER DEFAULT 0,
                    observacion TEXT DEFAULT '',
                    usuario TEXT DEFAULT '',
                    fecha_registro TIMESTAMP DEFAULT NOW(),
                    fecha_actualizacion TIMESTAMP DEFAULT NOW(),
                    UNIQUE(municipio, zona, cod_puesto, mesa)
                )
            """)
        conn.commit()


# Inicializar DB al arrancar
try:
    init_db()
    print("DB inicializada OK")
except Exception as e:
    print(f"Error init DB: {e}")


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/estructura.json")
def estructura():
    return send_from_directory(".", "estructura.json")


@app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.now().isoformat()})


# ── Obtener votos de una mesa ──────────────────────────────────────────────────
@app.route("/votos", methods=["GET"])
def get_votos():
    municipio = request.args.get("municipio", "").strip().upper()
    zona = request.args.get("zona", type=int)
    cod_puesto = request.args.get("cod_puesto", type=int)
    mesa = request.args.get("mesa", type=int)

    if not municipio or zona is None or cod_puesto is None or mesa is None:
        return jsonify({"error": "Faltan parametros"}), 400

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM votos
                    WHERE municipio=%s AND zona=%s AND cod_puesto=%s AND mesa=%s
                """, (municipio, zona, cod_puesto, mesa))
                row = cur.fetchone()
        if row:
            return jsonify(dict(row))
        return jsonify({})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Guardar / actualizar votos de una mesa ────────────────────────────────────
@app.route("/votos", methods=["POST"])
def save_votos():
    body = request.get_json(force=True) or {}
    municipio   = str(body.get("municipio", "")).strip().upper()
    zona        = body.get("zona")
    cod_puesto  = body.get("cod_puesto")
    nom_puesto  = str(body.get("nom_puesto", "")).strip()
    mesa        = body.get("mesa")
    e14_camara  = int(body.get("e14_ahora_camara") or 0)
    e14_senado  = int(body.get("e14_ahora_senado") or 0)
    e24_camara  = int(body.get("e24_ahora_camara") or 0)
    e24_senado  = int(body.get("e24_ahora_senado") or 0)
    observacion = str(body.get("observacion", "")).strip()
    usuario     = str(body.get("usuario", "")).strip()

    if not municipio or zona is None or cod_puesto is None or mesa is None:
        return jsonify({"error": "Faltan campos obligatorios"}), 400

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO votos
                        (municipio, zona, cod_puesto, nom_puesto, mesa,
                         e14_ahora_camara, e14_ahora_senado,
                         e24_ahora_camara, e24_ahora_senado,
                         observacion, usuario, fecha_actualizacion)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (municipio, zona, cod_puesto, mesa)
                    DO UPDATE SET
                        e14_ahora_camara   = EXCLUDED.e14_ahora_camara,
                        e14_ahora_senado   = EXCLUDED.e14_ahora_senado,
                        e24_ahora_camara   = EXCLUDED.e24_ahora_camara,
                        e24_ahora_senado   = EXCLUDED.e24_ahora_senado,
                        observacion        = EXCLUDED.observacion,
                        usuario            = EXCLUDED.usuario,
                        fecha_actualizacion = NOW()
                    RETURNING *
                """, (municipio, zona, cod_puesto, nom_puesto, mesa,
                      e14_camara, e14_senado, e24_camara, e24_senado,
                      observacion, usuario))
                row = cur.fetchone()
            conn.commit()
        return jsonify({"ok": True, "data": dict(row)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Listar todos los votos registrados (para resumen) ─────────────────────────
@app.route("/votos/todos", methods=["GET"])
def get_todos():
    municipio = request.args.get("municipio", "").strip().upper()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if municipio:
                    cur.execute("""
                        SELECT * FROM votos WHERE municipio=%s
                        ORDER BY zona, cod_puesto, mesa
                    """, (municipio,))
                else:
                    cur.execute("""
                        SELECT * FROM votos ORDER BY municipio, zona, cod_puesto, mesa
                    """)
                rows = cur.fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Resumen por municipio ──────────────────────────────────────────────────────
@app.route("/resumen", methods=["GET"])
def get_resumen():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        municipio,
                        COUNT(*) AS mesas_registradas,
                        SUM(e14_ahora_camara) AS total_e14_camara,
                        SUM(e14_ahora_senado) AS total_e14_senado,
                        SUM(e24_ahora_camara) AS total_e24_camara,
                        SUM(e24_ahora_senado) AS total_e24_senado
                    FROM votos
                    GROUP BY municipio
                    ORDER BY municipio
                """)
                rows = cur.fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
