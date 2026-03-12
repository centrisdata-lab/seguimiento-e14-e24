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

    # Solo filas con zona y puesto definidos (nivel puesto)
    # Columnas: DEPARTAMENTO, MUNICIPIO, ZONA, PUESTO, dep, mun, nom, tipo_com, nom_com, ini, fin, tot, lug, dir
    puesto_rows = [r for r in rows[1:] if r[2] is not None and r[3] is not None and r[5]]
    print(f"  Filas de puesto: {len(puesto_rows)}")

    # Estructura: { municipio: { zona: { cod_puesto: { nom, comision, mesas:[] } } } }
    data = {}

    for r in puesto_rows:
        mun = str(r[5]).strip().upper()
        zona = safe_int(r[2])
        puesto = safe_int(r[3])
        nom = str(r[6]).strip() if r[6] else ""
        comision = str(r[8]).strip() if r[8] else ""
        ini = safe_int(r[9])
        fin = safe_int(r[10])

        if None in (zona, puesto, ini, fin):
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
