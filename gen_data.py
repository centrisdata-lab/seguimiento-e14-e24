"""
Regenera estructura.json a partir del Excel de analisis de mesas.
Ejecutar localmente cuando se actualice el Excel:
    python gen_data.py
"""
import openpyxl
import json
import os

BASE = os.path.dirname(os.path.abspath(__file__))
EXCEL = os.path.join(
    os.path.dirname(BASE),
    "Defensa del voto", "analisis",
    "Análisis mesa Departamento Antioquia (1) (1).xlsx"
)
OUT = os.path.join(BASE, "estructura.json")


def safe_int(val):
    try:
        return int(val) if val is not None else 0
    except (ValueError, TypeError):
        return 0


def main():
    print(f"Leyendo: {EXCEL}")
    wb = openpyxl.load_workbook(EXCEL, read_only=True)
    rows = list(wb.active.iter_rows(values_only=True))
    print(f"  {len(rows)-1} filas de datos")

    # Estructura: { municipio: { zona: { cod_puesto: { nom, mesas:[] } } } }
    data = {}

    for r in rows[1:]:
        mun = str(r[1]).strip() if r[1] else ""
        if not mun:
            continue
        zona     = safe_int(r[2])
        cod_pue  = safe_int(r[3])
        nom_pue  = str(r[4]).strip() if r[4] else ""
        mesa     = safe_int(r[5])
        if not mesa:
            continue

        if mun not in data:
            data[mun] = {}
        zk = str(zona)
        if zk not in data[mun]:
            data[mun][zk] = {}
        pk = str(cod_pue)
        if pk not in data[mun][zk]:
            data[mun][zk][pk] = {"nom": nom_pue, "mesas": []}
        data[mun][zk][pk]["mesas"].append(mesa)

    # Ordenar y deduplicar mesas
    for mun in data:
        for z in data[mun]:
            for p in data[mun][z]:
                data[mun][z][p]["mesas"] = sorted(set(data[mun][z][p]["mesas"]))

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
