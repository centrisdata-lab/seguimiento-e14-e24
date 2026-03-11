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
                    comision TEXT DEFAULT '',
                    mesa INTEGER NOT NULL,
                    e14_ahora INTEGER DEFAULT 0,
                    e14_conservador INTEGER DEFAULT 0,
                    e24_ahora INTEGER DEFAULT 0,
                    e24_conservador INTEGER DEFAULT 0,
                    observacion TEXT DEFAULT '',
                    usuario TEXT DEFAULT '',
                    fecha_registro TIMESTAMP DEFAULT NOW(),
                    fecha_actualizacion TIMESTAMP DEFAULT NOW(),
                    UNIQUE(municipio, zona, cod_puesto, mesa)
                )
            """)
            # Migrar columnas antiguas si existen
            cur.execute("""
                DO $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name='votos' AND column_name='e14_ahora_camara') THEN
                        ALTER TABLE votos RENAME COLUMN e14_ahora_camara TO e14_ahora;
                    END IF;
                    IF EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_name='votos' AND column_name='e24_ahora_camara') THEN
                        ALTER TABLE votos RENAME COLUMN e24_ahora_camara TO e24_ahora;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                   WHERE table_name='votos' AND column_name='e14_conservador') THEN
                        ALTER TABLE votos ADD COLUMN e14_conservador INTEGER DEFAULT 0;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                   WHERE table_name='votos' AND column_name='e24_conservador') THEN
                        ALTER TABLE votos ADD COLUMN e24_conservador INTEGER DEFAULT 0;
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                                   WHERE table_name='votos' AND column_name='comision') THEN
                        ALTER TABLE votos ADD COLUMN comision TEXT DEFAULT '';
                    END IF;
                END $$;
            """)
        conn.commit()


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
    municipio  = request.args.get("municipio", "").strip().upper()
    zona       = request.args.get("zona", type=int)
    cod_puesto = request.args.get("cod_puesto", type=int)
    mesa       = request.args.get("mesa", type=int)

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
        return jsonify(dict(row) if row else {})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Guardar / actualizar votos de una mesa ────────────────────────────────────
@app.route("/votos", methods=["POST"])
def save_votos():
    body = request.get_json(force=True) or {}
    municipio      = str(body.get("municipio", "")).strip().upper()
    zona           = body.get("zona")
    cod_puesto     = body.get("cod_puesto")
    nom_puesto     = str(body.get("nom_puesto", "")).strip()
    comision       = str(body.get("comision", "")).strip()
    mesa           = body.get("mesa")
    e14_ahora      = int(body.get("e14_ahora") or 0)
    e14_conservador = int(body.get("e14_conservador") or 0)
    e24_ahora      = int(body.get("e24_ahora") or 0)
    e24_conservador = int(body.get("e24_conservador") or 0)
    observacion    = str(body.get("observacion", "")).strip()
    usuario        = str(body.get("usuario", "")).strip()

    if not municipio or zona is None or cod_puesto is None or mesa is None:
        return jsonify({"error": "Faltan campos obligatorios"}), 400

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO votos
                        (municipio, zona, cod_puesto, nom_puesto, comision, mesa,
                         e14_ahora, e14_conservador, e24_ahora, e24_conservador,
                         observacion, usuario, fecha_actualizacion)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (municipio, zona, cod_puesto, mesa)
                    DO UPDATE SET
                        e14_ahora        = EXCLUDED.e14_ahora,
                        e14_conservador  = EXCLUDED.e14_conservador,
                        e24_ahora        = EXCLUDED.e24_ahora,
                        e24_conservador  = EXCLUDED.e24_conservador,
                        comision         = EXCLUDED.comision,
                        observacion      = EXCLUDED.observacion,
                        usuario          = EXCLUDED.usuario,
                        fecha_actualizacion = NOW()
                    RETURNING *
                """, (municipio, zona, cod_puesto, nom_puesto, comision, mesa,
                      e14_ahora, e14_conservador, e24_ahora, e24_conservador,
                      observacion, usuario))
                row = cur.fetchone()
            conn.commit()
        return jsonify({"ok": True, "data": dict(row)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Resumen agrupado (por municipio / comision / puesto) ──────────────────────
@app.route("/resumen", methods=["GET"])
def get_resumen():
    nivel = request.args.get("nivel", "municipio")  # municipio | comision | puesto
    municipio = request.args.get("municipio", "").strip().upper()
    comision  = request.args.get("comision", "").strip().upper()

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if nivel == "puesto":
                    cur.execute("""
                        SELECT municipio, comision, nom_puesto,
                               COUNT(*) AS mesas_registradas,
                               SUM(e14_ahora) AS total_e14_ahora,
                               SUM(e14_conservador) AS total_e14_conservador,
                               SUM(e24_ahora) AS total_e24_ahora,
                               SUM(e24_conservador) AS total_e24_conservador
                        FROM votos
                        WHERE (%s = '' OR UPPER(municipio) = %s)
                          AND (%s = '' OR UPPER(comision)  = %s)
                        GROUP BY municipio, comision, nom_puesto
                        ORDER BY municipio, comision, nom_puesto
                    """, (municipio, municipio, comision, comision))
                elif nivel == "comision":
                    cur.execute("""
                        SELECT municipio, comision,
                               COUNT(*) AS mesas_registradas,
                               SUM(e14_ahora) AS total_e14_ahora,
                               SUM(e14_conservador) AS total_e14_conservador,
                               SUM(e24_ahora) AS total_e24_ahora,
                               SUM(e24_conservador) AS total_e24_conservador
                        FROM votos
                        WHERE (%s = '' OR UPPER(municipio) = %s)
                        GROUP BY municipio, comision
                        ORDER BY municipio, comision
                    """, (municipio, municipio))
                else:  # municipio
                    cur.execute("""
                        SELECT municipio,
                               COUNT(*) AS mesas_registradas,
                               SUM(e14_ahora) AS total_e14_ahora,
                               SUM(e14_conservador) AS total_e14_conservador,
                               SUM(e24_ahora) AS total_e24_ahora,
                               SUM(e24_conservador) AS total_e24_conservador
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
