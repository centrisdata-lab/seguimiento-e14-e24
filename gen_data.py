"""
Regenera estructura.json a partir exclusivamente del DIVIPOLA.xlsx.
Ejecutar localmente cuando se actualice el Excel:
    python gen_data.py
"""
import openpyxl
import json
import os

BASE = os.path.dirname(os.path.abspath(__file__))
EXCEL_DIVIPOLA = os.path.join(
    os.path.dirname(BASE),
    "Defensa del voto", "analisis",
    "DIVIPOLA.xlsx"
)
OUT = os.path.join(BASE, "estructura.json")


def safe_int(v):
    try:
        return int(float(str(v)))
    except (ValueError, TypeError):
        return None


def main():
    print(f"Leyendo DIVIPOLA: {EXCEL_DIVIPOLA}")
    wb = openpyxl.load_workbook(EXCEL_DIVIPOLA, read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    print(f"  {len(rows)-1} filas totales")

    # Columnas: DEPARTAMENTO, MUNICIPIO, ZONA, PUESTO, dep, mun, nom, tipo_com, nom_com, ini, fin, tot, lug, dir
    # Filas validas: deben tener municipio (col5) e ini/fin (col9/10)
    # Si zona o puesto estan vacios -> zona=0, puesto=0
    # Si nom (col6) esta vacio -> usar lug (col12) como nombre del puesto
    puesto_rows = [r for r in rows[1:] if r[5] and r[9] is not None and r[10] is not None]
    print(f"  Filas de puesto: {len(puesto_rows)}")

    # Estructura: { municipio: { zona: { cod_puesto: { nom, comision, mesas:[] } } } }
    data = {}

    # Municipios cuyo nombre en DIVIPOLA difiere del nombre oficial usado en la app
    RENOMBRAR = {
        "SAN PEDRO": "SAN PEDRO DE LOS MILAGROS",
    }

    for r in puesto_rows:
        mun_raw = str(r[5]).strip().upper()
        mun = RENOMBRAR.get(mun_raw, mun_raw)
        zona   = safe_int(r[2]) if r[2] is not None else 0
        puesto = safe_int(r[3]) if r[3] is not None else 0
        nom_raw = str(r[6]).strip() if r[6] else ""
        lug_raw = str(r[12]).strip() if r[12] else ""
        nom = nom_raw if nom_raw else lug_raw
        comision = str(r[8]).strip() if r[8] else ""
        ini = safe_int(r[9])
        fin = safe_int(r[10])

        if zona is None or puesto is None or ini is None or fin is None:
            continue

        mesas = list(range(ini, fin + 1))

        if mun not in data:
            data[mun] = {}
        zk = str(zona)
        if zk not in data[mun]:
            data[mun][zk] = {}
        pk = str(puesto)
        if pk not in data[mun][zk]:
            data[mun][zk][pk] = {"nom": nom, "comision": comision, "mesas": mesas}

    total_mesas = sum(
        len(data[m][z][p]["mesas"])
        for m in data for z in data[m] for p in data[m][z]
    )
    print(f"  Municipios: {len(data)}")
    print(f"  Total mesas: {total_mesas}")

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"OK -> {OUT}")


if __name__ == "__main__":
    main()
