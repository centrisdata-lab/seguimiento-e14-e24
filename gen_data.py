"""
Regenera estructura.json a partir del Excel de analisis de mesas.
Ejecutar localmente cuando se actualice el Excel:
    python gen_data.py
"""
import openpyxl
import json
import os

BASE = os.path.dirname(os.path.abspath(__file__))
EXCEL_ANALISIS = os.path.join(
    os.path.dirname(BASE),
    "Defensa del voto", "analisis",
    "Análisis mesa Departamento Antioquia (1) (1).xlsx"
)
EXCEL_DIVIPOLA = os.path.join(
    os.path.dirname(BASE),
    "Defensa del voto", "analisis",
    "DIVIPOLA.xlsx"
)
OUT = os.path.join(BASE, "estructura.json")


def safe_int(val):
    try:
        return int(val) if val is not None else 0
    except (ValueError, TypeError):
        return 0


def main():
    # ── Cargar comisiones de DIVIPOLA ──────────────────────────────────────────
    print(f"Leyendo DIVIPOLA: {EXCEL_DIVIPOLA}")
    wb_div = openpyxl.load_workbook(EXCEL_DIVIPOLA, read_only=True)
    div_rows = list(wb_div.active.iter_rows(values_only=True))

    divipola = {}
    div_mun = {}
    for r in div_rows[1:]:
        dep = str(r[4]).upper().strip() if r[4] else ""
        if dep != "ANTIOQUIA":
            continue
        mun = str(r[5]).upper().strip() if r[5] else ""
        nom_com = str(r[8]).strip() if r[8] else ""
        try:
            zona = int(r[2]) if r[2] is not None else None
        except (ValueError, TypeError):
            zona = None
        try:
            puesto = int(r[3]) if r[3] is not None else None
        except (ValueError, TypeError):
            puesto = None
        if mun and zona is not None and puesto is not None and nom_com:
            key = (mun, zona, puesto)
            if key not in divipola:
                divipola[key] = nom_com
        if mun and zona is None and puesto is None and nom_com and mun not in div_mun:
            div_mun[mun] = nom_com

    def get_com(mun, zona, puesto):
        c = divipola.get((mun, zona, puesto), "")
        return c if c else div_mun.get(mun, "SIN COMISION")

    print(f"  Comisiones cargadas: {len(divipola)}")

    # ── Cargar Excel principal ─────────────────────────────────────────────────
    print(f"Leyendo analisis: {EXCEL_ANALISIS}")
    wb = openpyxl.load_workbook(EXCEL_ANALISIS, read_only=True)
    rows = list(wb.active.iter_rows(values_only=True))
    print(f"  {len(rows)-1} filas de datos")

    # Estructura: { municipio: { zona: { cod_puesto: { nom, comision, mesas:[] } } } }
    data = {}

    for r in rows[1:]:
        mun = str(r[1]).strip() if r[1] else ""
        if not mun:
            continue
        zona    = safe_int(r[2])
        cod_pue = safe_int(r[3])
        nom_pue = str(r[4]).strip() if r[4] else ""
        mesa    = safe_int(r[5])
        if not mesa:
            continue
        comision = get_com(mun.upper(), zona, cod_pue)

        if mun not in data:
            data[mun] = {}
        zk = str(zona)
        if zk not in data[mun]:
            data[mun][zk] = {}
        pk = str(cod_pue)
        if pk not in data[mun][zk]:
            data[mun][zk][pk] = {"nom": nom_pue, "comision": comision, "mesas": []}
        data[mun][zk][pk]["mesas"].append(mesa)

    for mun in data:
        for z in data[mun]:
            for p in data[mun][z]:
                data[mun][z][p]["mesas"] = sorted(set(data[mun][z][p]["mesas"]))

    total_mesas = sum(
        len(data[m][z][p]["mesas"])
        for m in data for z in data[m] for p in data[m][z]
    )
    sin_com = sum(
        1 for m in data for z in data[m] for p in data[m][z]
        if data[m][z][p]["comision"] == "SIN COMISION"
    )
    print(f"  Municipios: {len(data)}")
    print(f"  Total mesas: {total_mesas}")
    print(f"  Puestos sin comision: {sin_com}")

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"OK -> {OUT}")


if __name__ == "__main__":
    main()
