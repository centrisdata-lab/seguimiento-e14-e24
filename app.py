import os
import time
import threading
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool, OperationalError
from datetime import datetime

app = Flask(__name__, static_folder=".")
CORS(app)

DATABASE_URL = os.environ.get("DATABASE_URL")

# Pool de conexiones: Render free tier permite ~25 conexiones a PG
# Con 400+ usuarios simultáneos el pool se comparte via Flask threads
_pool = None
_pool_lock = threading.Lock()

def get_pool():
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = pool.ThreadedConnectionPool(
                    minconn=5,
                    maxconn=25,
                    dsn=DATABASE_URL,
                    cursor_factory=RealDictCursor,
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=30,       # ping TCP cada 30s si idle
                    keepalives_interval=10,   # reintentar cada 10s
                    keepalives_count=5,       # hasta 5 reintentos
                    options="-c statement_timeout=15000"  # 15s max por query
                )
    return _pool

from contextlib import contextmanager

def _conn_is_alive(conn):
    """Verifica si la conexion sigue viva con un ping ligero."""
    try:
        conn.cursor().execute("SELECT 1")
        return True
    except Exception:
        return False

@contextmanager
def get_conn(retries=4, wait=0.4):
    """Obtiene una conexion del pool con reintentos.
    Descarta conexiones muertas (SSL EOF) y obtiene una nueva."""
    conn = None
    last_err = None
    for attempt in range(retries):
        try:
            conn = get_pool().getconn()
            # Descartar si esta cerrada o muerta (SSL SYSCALL EOF)
            if conn.closed or not _conn_is_alive(conn):
                try:
                    get_pool().putconn(conn, close=True)
                except Exception:
                    pass
                conn = None
                raise OperationalError("conexion muerta, descartada")
            break
        except pool.PoolError as e:
            last_err = e
            conn = None
            if attempt < retries - 1:
                time.sleep(wait * (attempt + 1))
        except OperationalError as e:
            last_err = e
            conn = None
            if attempt < retries - 1:
                time.sleep(wait * (attempt + 1))
    if conn is None:
        raise Exception(f"No se pudo obtener conexion tras {retries} intentos: {last_err}")
    try:
        yield conn
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            # Si la conexion quedo en mal estado, cerrarla en lugar de devolverla
            if conn.closed or conn.status != psycopg2.extensions.STATUS_READY:
                get_pool().putconn(conn, close=True)
            else:
                get_pool().putconn(conn)
        except Exception:
            pass


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Tabla principal E-14 y E-24 por mesa
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
            # Tabla E-26 por municipio
            cur.execute("""
                CREATE TABLE IF NOT EXISTS votos_e26 (
                    id SERIAL PRIMARY KEY,
                    municipio TEXT NOT NULL UNIQUE,
                    e26_ahora INTEGER DEFAULT 0,
                    e26_conservador INTEGER DEFAULT 0,
                    observacion TEXT DEFAULT '',
                    usuario TEXT DEFAULT '',
                    fecha_registro TIMESTAMP DEFAULT NOW(),
                    fecha_actualizacion TIMESTAMP DEFAULT NOW()
                )
            """)
            # Migraciones columnas legacy
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


# ── E-14 / E-24: Obtener votos de una mesa ────────────────────────────────────
@app.route("/votos", methods=["GET"])
def get_votos():
    municipio  = request.args.get("municipio", "").strip().upper()
    zona       = request.args.get("zona", type=int)
    cod_puesto = request.args.get("cod_puesto", type=int)
    mesa       = request.args.get("mesa", type=int)
    todos      = request.args.get("todos")

    if not municipio or zona is None or cod_puesto is None:
        return jsonify({"error": "Faltan parametros"}), 400

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if todos:
                    # Devolver todas las mesas del puesto con datos
                    cur.execute("""
                        SELECT mesa, e14_ahora, e14_conservador, e24_ahora, e24_conservador
                        FROM votos
                        WHERE municipio=%s AND zona=%s AND cod_puesto=%s
                    """, (municipio, zona, cod_puesto))
                    rows = cur.fetchall()
                    return jsonify([dict(r) for r in rows])
                else:
                    if mesa is None:
                        return jsonify({"error": "Falta mesa"}), 400
                    cur.execute("""
                        SELECT * FROM votos
                        WHERE municipio=%s AND zona=%s AND cod_puesto=%s AND mesa=%s
                    """, (municipio, zona, cod_puesto, mesa))
                    row = cur.fetchone()
                    return jsonify(dict(row) if row else {})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── E-14 / E-24: Guardar / actualizar votos de una mesa ───────────────────────
@app.route("/votos", methods=["POST"])
def save_votos():
    body = request.get_json(force=True) or {}
    municipio       = str(body.get("municipio", "")).strip().upper()
    zona            = body.get("zona")
    cod_puesto      = body.get("cod_puesto")
    nom_puesto      = str(body.get("nom_puesto", "")).strip()
    comision        = str(body.get("comision", "")).strip()
    mesa            = body.get("mesa")
    e14_ahora       = int(body.get("e14_ahora") or 0)
    e14_conservador = int(body.get("e14_conservador") or 0)
    e24_ahora       = int(body.get("e24_ahora") or 0)
    e24_conservador = int(body.get("e24_conservador") or 0)
    observacion     = str(body.get("observacion", "")).strip()
    usuario         = str(body.get("usuario", "")).strip()

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
        print(f"ERROR save_votos: {e}")
        return jsonify({"error": str(e)}), 500


# ── E-26: Obtener votos de un municipio ───────────────────────────────────────
@app.route("/votos_e26", methods=["GET"])
def get_votos_e26():
    municipio = request.args.get("municipio", "").strip().upper()
    if not municipio:
        return jsonify({"error": "Falta municipio"}), 400
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM votos_e26 WHERE municipio=%s", (municipio,))
                row = cur.fetchone()
        return jsonify(dict(row) if row else {})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── E-26: Guardar / actualizar votos de un municipio ──────────────────────────
@app.route("/votos_e26", methods=["POST"])
def save_votos_e26():
    body = request.get_json(force=True) or {}
    municipio       = str(body.get("municipio", "")).strip().upper()
    e26_ahora       = int(body.get("e26_ahora") or 0)
    e26_conservador = int(body.get("e26_conservador") or 0)
    observacion     = str(body.get("observacion", "")).strip()
    usuario         = str(body.get("usuario", "")).strip()

    if not municipio:
        return jsonify({"error": "Falta municipio"}), 400

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO votos_e26
                        (municipio, e26_ahora, e26_conservador, observacion, usuario, fecha_actualizacion)
                    VALUES (%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (municipio)
                    DO UPDATE SET
                        e26_ahora       = EXCLUDED.e26_ahora,
                        e26_conservador = EXCLUDED.e26_conservador,
                        observacion     = EXCLUDED.observacion,
                        usuario         = EXCLUDED.usuario,
                        fecha_actualizacion = NOW()
                    RETURNING *
                """, (municipio, e26_ahora, e26_conservador, observacion, usuario))
                row = cur.fetchone()
                conn.commit()
        return jsonify({"ok": True, "data": dict(row)})
    except Exception as e:
        print(f"ERROR save_votos_e26: {e}")
        return jsonify({"error": str(e)}), 500


# ── Resumen detallado E-14 vs E-24 (por municipio/zona/comision/puesto/mesa) ──
@app.route("/resumen", methods=["GET"])
def get_resumen():
    nivel     = request.args.get("nivel", "municipio")
    municipio = request.args.get("municipio", "").strip().upper()
    comision  = request.args.get("comision", "").strip().upper()

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                AMBOS = "(e14_ahora > 0 OR e14_conservador > 0) AND (e24_ahora > 0 OR e24_conservador > 0)"
                if nivel == "zona":
                    cur.execute(f"""
                        SELECT municipio, zona,
                               COUNT(*) AS mesas_registradas,
                               SUM(e14_ahora) AS total_e14_ahora,
                               SUM(e14_conservador) AS total_e14_conservador,
                               SUM(e24_ahora) AS total_e24_ahora,
                               SUM(e24_conservador) AS total_e24_conservador
                        FROM votos
                        WHERE ({AMBOS})
                          AND (%s = '' OR UPPER(municipio) = %s)
                        GROUP BY municipio, zona
                        ORDER BY municipio, zona
                    """, (municipio, municipio))
                elif nivel == "mesa":
                    cur.execute(f"""
                        SELECT id, municipio, zona, cod_puesto, comision, mesa,
                               e14_ahora, e14_conservador, e24_ahora, e24_conservador,
                               observacion, usuario, fecha_actualizacion,
                               1 AS mesas_registradas,
                               e14_ahora AS total_e14_ahora,
                               e14_conservador AS total_e14_conservador,
                               e24_ahora AS total_e24_ahora,
                               e24_conservador AS total_e24_conservador
                        FROM votos
                        WHERE ({AMBOS})
                          AND (%s = '' OR UPPER(municipio) = %s)
                          AND (%s = '' OR UPPER(comision)  = %s)
                        ORDER BY municipio, zona, cod_puesto, comision, mesa
                    """, (municipio, municipio, comision, comision))
                elif nivel == "puesto":
                    cur.execute(f"""
                        SELECT municipio, zona, cod_puesto, comision,
                               COUNT(*) AS mesas_registradas,
                               SUM(e14_ahora) AS total_e14_ahora,
                               SUM(e14_conservador) AS total_e14_conservador,
                               SUM(e24_ahora) AS total_e24_ahora,
                               SUM(e24_conservador) AS total_e24_conservador
                        FROM votos
                        WHERE ({AMBOS})
                          AND (%s = '' OR UPPER(municipio) = %s)
                          AND (%s = '' OR UPPER(comision)  = %s)
                        GROUP BY municipio, zona, cod_puesto, comision
                        ORDER BY municipio, zona, cod_puesto, comision
                    """, (municipio, municipio, comision, comision))
                elif nivel == "comision":
                    cur.execute(f"""
                        SELECT municipio, zona, comision,
                               COUNT(*) AS mesas_registradas,
                               SUM(e14_ahora) AS total_e14_ahora,
                               SUM(e14_conservador) AS total_e14_conservador,
                               SUM(e24_ahora) AS total_e24_ahora,
                               SUM(e24_conservador) AS total_e24_conservador
                        FROM votos
                        WHERE ({AMBOS})
                          AND (%s = '' OR UPPER(municipio) = %s)
                        GROUP BY municipio, zona, comision
                        ORDER BY municipio, zona, comision
                    """, (municipio, municipio))
                else:  # municipio
                    cur.execute(f"""
                        SELECT municipio,
                               COUNT(*) AS mesas_registradas,
                               SUM(e14_ahora) AS total_e14_ahora,
                               SUM(e14_conservador) AS total_e14_conservador,
                               SUM(e24_ahora) AS total_e24_ahora,
                               SUM(e24_conservador) AS total_e24_conservador
                        FROM votos
                        WHERE ({AMBOS})
                        GROUP BY municipio
                        ORDER BY municipio
                    """)
                rows = cur.fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Resumen municipal: E-14 + E-24 + E-26 comparados ─────────────────────────
@app.route("/resumen_municipal", methods=["GET"])
def get_resumen_municipal():
    municipio = request.args.get("municipio", "").strip().upper()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        v.municipio,
                        SUM(v.e14_ahora)       AS e14_ahora,
                        SUM(v.e14_conservador)  AS e14_conservador,
                        SUM(v.e24_ahora)        AS e24_ahora,
                        SUM(v.e24_conservador)  AS e24_conservador,
                        e26.e26_ahora,
                        e26.e26_conservador
                    FROM votos v
                    INNER JOIN votos_e26 e26 ON UPPER(e26.municipio) = UPPER(v.municipio)
                    WHERE (%s = '' OR UPPER(v.municipio) = %s)
                      AND (v.e14_ahora > 0 OR v.e14_conservador > 0)
                      AND (v.e24_ahora > 0 OR v.e24_conservador > 0)
                      AND (e26.e26_ahora > 0 OR e26.e26_conservador > 0)
                    GROUP BY v.municipio, e26.e26_ahora, e26.e26_conservador
                    ORDER BY v.municipio
                """, (municipio, municipio))
                rows = cur.fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Cobertura: cuantas mesas/municipios registrados vs total mapeado ──────────
@app.route("/cobertura", methods=["GET"])
def get_cobertura():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Mesas con E-14 digitado (e14_ahora > 0 o e14_conservador > 0)
                cur.execute("SELECT COUNT(*) AS n FROM votos WHERE e14_ahora > 0 OR e14_conservador > 0")
                e14 = cur.fetchone()["n"]
                # Mesas con E-24 digitado
                cur.execute("SELECT COUNT(*) AS n FROM votos WHERE e24_ahora > 0 OR e24_conservador > 0")
                e24 = cur.fetchone()["n"]
                # Total mesas registradas (cualquier dato)
                cur.execute("SELECT COUNT(*) AS n FROM votos")
                total_reg = cur.fetchone()["n"]
                # Municipios con E-26
                cur.execute("SELECT COUNT(*) AS n FROM votos_e26")
                e26 = cur.fetchone()["n"]
        return jsonify({
            "e14_mesas": e14,
            "e24_mesas": e24,
            "e26_municipios": e26,
            "total_registros": total_reg
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Faltantes: mesas/municipios sin datos por formato ────────────────────────
@app.route("/faltantes", methods=["GET"])
def get_faltantes():
    import json, os
    fmt = request.args.get("fmt", "e14")  # e14 | e24 | e26
    try:
        # Leer estructura
        est_path = os.path.join(os.path.dirname(__file__), "estructura.json")
        with open(est_path, encoding="utf-8") as f:
            estructura = json.load(f)

        with get_conn() as conn:
            with conn.cursor() as cur:
                if fmt == "e26":
                    cur.execute("SELECT UPPER(municipio) AS municipio FROM votos_e26")
                    registrados = {r["municipio"] for r in cur.fetchall()}
                    todos = sorted(estructura.keys())
                    faltantes = [{"municipio": m} for m in todos if m.upper() not in registrados]
                else:
                    col = "e14_ahora" if fmt == "e14" else "e24_ahora"
                    col2 = "e14_conservador" if fmt == "e14" else "e24_conservador"
                    cur.execute(f"""
                        SELECT UPPER(municipio) AS municipio, zona, cod_puesto, mesa
                        FROM votos WHERE {col} > 0 OR {col2} > 0
                    """)
                    # registrados usa (municipio_upper, zona_int, cod_puesto_int, mesa_int)
                    registrados = {(r["municipio"], int(r["zona"]), int(r["cod_puesto"]), int(r["mesa"]))
                                   for r in cur.fetchall()}
                    faltantes = []
                    for mun, zonas in estructura.items():
                        for zona, puestos in zonas.items():
                            try:
                                zona_int = int(zona)
                            except (ValueError, TypeError):
                                continue
                            for cod, pdata in puestos.items():
                                try:
                                    cod_int = int(cod)
                                except (ValueError, TypeError):
                                    continue
                                for mesa in pdata["mesas"]:
                                    try:
                                        mesa_int = int(mesa)
                                    except (ValueError, TypeError):
                                        continue
                                    key = (mun.upper(), zona_int, cod_int, mesa_int)
                                    if key not in registrados:
                                        faltantes.append({
                                            "municipio": mun,
                                            "zona": zona_int,
                                            "cod_puesto": cod_int,
                                            "mesa": mesa_int,
                                            "comision": pdata.get("comision", "")
                                        })
        return jsonify(faltantes)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Digitados: mesas con cualquier dato E-14 o E-24 (para marcar ✓ en UI) ─────
@app.route("/digitados", methods=["GET"])
def get_digitados():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT municipio, zona, cod_puesto, mesa,
                           (e14_ahora > 0 OR e14_conservador > 0) AS tiene_e14,
                           (e24_ahora > 0 OR e24_conservador > 0) AS tiene_e24
                    FROM votos
                    WHERE e14_ahora > 0 OR e14_conservador > 0
                       OR e24_ahora > 0 OR e24_conservador > 0
                """)
                rows = cur.fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── E-26: Listar todos los registros para control ─────────────────────────────
@app.route("/votos_e26_control", methods=["GET"])
def get_votos_e26_control():
    municipio = request.args.get("municipio", "").strip().upper()
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if municipio:
                    cur.execute("""
                        SELECT * FROM votos_e26
                        WHERE UPPER(municipio) LIKE %s
                        ORDER BY municipio
                    """, (f"%{municipio}%",))
                else:
                    cur.execute("SELECT * FROM votos_e26 ORDER BY municipio")
                rows = cur.fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Eliminar registro E-26 de un municipio ────────────────────────────────────
@app.route("/votos_e26/<int:voto_id>", methods=["DELETE"])
def delete_voto_e26(voto_id):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM votos_e26 WHERE id=%s RETURNING id", (voto_id,))
                row = cur.fetchone()
            conn.commit()
        if row:
            return jsonify({"ok": True})
        return jsonify({"error": "Registro no encontrado"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Eliminar registro E-14/E-24 de una mesa ───────────────────────────────────
@app.route("/votos/<int:voto_id>", methods=["DELETE"])
def delete_voto(voto_id):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM votos WHERE id=%s RETURNING id", (voto_id,))
                row = cur.fetchone()
            conn.commit()
        if row:
            return jsonify({"ok": True})
        return jsonify({"error": "Registro no encontrado"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
