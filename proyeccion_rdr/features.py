from pathlib import Path

import pandas as pd
import polars as pl
import typer
from holidays import country_holidays
from loguru import logger
from tabulate import tabulate
from tqdm import tqdm

from proyeccion_rdr.config import PROCESSED_DATA_DIR

FERIADOS_CHILE = country_holidays("CL")

app = typer.Typer()


def leer_casos_macroprocesos(ruta_archivo):
    # Obtiene los casos por macroproceso por region
    casos_macroproceso_por_region = pd.read_excel(
        ruta_archivo, sheet_name="casos_macroproceso_por_region"
    )

    # Obtiene los casos por macroproceso consolidados
    casos_macroproceso_consolidados = pd.read_excel(
        ruta_archivo, sheet_name="casos_macroproceso_consolidado"
    )

    # Pone como indice el diagnostico
    casos_macroproceso_por_region = casos_macroproceso_por_region.set_index("Diagnostico")
    casos_macroproceso_consolidados = casos_macroproceso_consolidados.set_index("Diagnostico")

    # Cambia la columna de los diagnosticos contenidos
    casos_macroproceso_por_region["Diagnosticos Contenidos"] = casos_macroproceso_por_region[
        "Diagnosticos Contenidos"
    ].str.split(", ")

    casos_macroproceso_consolidados["Diagnosticos Contenidos"] = casos_macroproceso_consolidados[
        "Diagnosticos Contenidos"
    ].str.split(", ")

    return casos_macroproceso_por_region, casos_macroproceso_consolidados


def obtener_metricas_egresos(df, agrupacion):
    resumen = (
        df.group_by(agrupacion).agg(
            [
                pl.col("DIAG1").count().alias("n_egresos"),
                pl.col("ID_PACIENTE").n_unique().alias("n_pacientes_distintos"),
                pl.col("DIAS_ESTADA").sum().alias("dias_estada_totales"),
            ]
        )
    ).with_columns(
        [
            (pl.col("n_egresos") / pl.col("n_pacientes_distintos")).alias("egresos_por_paciente"),
            (pl.col("dias_estada_totales") / pl.col("n_egresos")).alias("dias_estada_promedio"),
        ]
    )

    return resumen


def calcular_resumen_metricas_desagregadas_y_agrupadas_en_anios(df, ano_inicio, ano_termino):
    df_filtrada = df.filter(
        (pl.col("ANO_EGRESO") >= ano_inicio) & (pl.col("ANO_EGRESO") <= ano_termino)
    )

    # Obtiene las metricas desagregadas por anio
    metricas_desagregadas = obtener_metricas_egresos(df_filtrada, ["ANO_EGRESO", "DIAG1"])
    metricas_desagregadas = metricas_desagregadas.sort(["ANO_EGRESO", "n_egresos"], descending=True)

    # Obtiene las metricas agrupadas en el periodo determinado
    metricas_agrupadas = obtener_metricas_egresos(df_filtrada, ["DIAG1"])
    metricas_agrupadas = metricas_agrupadas.sort(["DIAG1"], descending=True)

    # Convierte los resumenes a pandas
    metricas_desagregadas = metricas_desagregadas.to_pandas()
    metricas_agrupadas = metricas_agrupadas.to_pandas().set_index("DIAG1")

    # Pivota la tabla desagregada
    metricas_desagregadas = pd.pivot_table(
        metricas_desagregadas,
        index="DIAG1",
        columns="ANO_EGRESO",
        values=["n_egresos", "n_pacientes_distintos", "dias_estada_totales"],
        aggfunc="sum",
        fill_value=0,
    )

    # Cambia las columnas de la tabla agrupada en el periodo
    columnas_agrupado = [
        (f"agrupado_entre_{ano_inicio}_{ano_termino}", col) for col in metricas_agrupadas.columns
    ]
    metricas_agrupadas.columns = pd.MultiIndex.from_tuples(columnas_agrupado)

    # Concatena las metircs desagregadas y agregadas
    resumen = pd.concat([metricas_desagregadas, metricas_agrupadas], axis=1)

    return resumen


def es_feriado(fecha):
    if FERIADOS_CHILE.get(fecha):
        return 1

    else:
        return 0


def obtener_dias_laborales(
    fecha_inicio,
    fecha_termino,
    incluir_sabado=False,
):
    if incluir_sabado:
        fechas = pd.date_range(start=fecha_inicio, end=fecha_termino, freq="D")
        df = pd.DataFrame({"fecha": fechas})
        df["dia_semana"] = df["fecha"].dt.dayofweek  # 0=lunes, ..., 6=domingo
        df = df[df["dia_semana"] <= 5]  # lunes a sábado
    else:
        df = pd.DataFrame(
            {
                "fecha": pd.date_range(
                    start=fecha_inicio, end=fecha_termino, freq="B"
                )  # lunes a viernes
            }
        )
        df["dia_semana"] = df["fecha"].dt.dayofweek

    df["es_feriado"] = df["fecha"].apply(es_feriado)
    df = df[df["es_feriado"] == 0]

    return df


def calcular_horas_laborales(
    anio_inicio,
    anio_termino,
    horas_por_dia_semana=8,
    incluir_sabado=False,
    horas_sabado=6,
):
    """
    Calcula horas laborales por año, con opción de incluir sábados.

    Args:
    anio_inicio (int): Año de inicio.
    anio_termino (int): Año de término.
    horas_por_dia_semana (int): Horas de trabajo de lunes a viernes.
    incluir_sabado (bool): Si se deben incluir los sábados.
    horas_sabado (int): Horas de trabajo los sábados (si se incluyen).

    Returns:
    Series: Horas laborales por año.
    """
    df = obtener_dias_laborales(f"01-01-{anio_inicio}", f"01-01-{anio_termino + 1}", incluir_sabado)

    df["anio"] = df["fecha"].dt.year

    if incluir_sabado:
        df["horas_laborales"] = df["dia_semana"].apply(
            lambda x: horas_sabado if x == 5 else horas_por_dia_semana
        )
    else:
        df["horas_laborales"] = horas_por_dia_semana

    horas_por_anio = df.groupby("anio")["horas_laborales"].sum()
    horas_por_anio.index = horas_por_anio.index.astype(str)

    nombre_columna = (
        f"horas_laborales_con_sabado_{horas_sabado}h"
        if incluir_sabado
        else f"horas_laborales_solo_semana_{horas_por_dia_semana}h"
    )
    horas_por_anio.name = nombre_columna

    print("Horas laborales por año calculadas:")
    print(tabulate(horas_por_anio.head().reset_index(), headers="keys", tablefmt="pretty"))
    print()

    return horas_por_anio


def leer_proyeccion_consultas_medicas_especialidad(ruta, anios_a_sumar):
    # Lee la cantidad de consultas medicas proyectadas
    df = pd.read_excel(ruta, sheet_name="consultas_medicas_proyectadas")

    # Renombra nombre de columna especialidades
    df = df.rename(columns={"Unnamed: 0": "ESTAMENTO/ESPECIALIDAD"})

    # Rellena las celdas de especialidad sin rellenar
    df["ESTAMENTO/ESPECIALIDAD"] = df["ESTAMENTO/ESPECIALIDAD"].ffill()

    # Pone la especialidad como indice y suma las consultas de todas las especialidades
    df = df.groupby("ESTAMENTO/ESPECIALIDAD")[anios_a_sumar].sum().sum()

    return df


def leer_dco_proyectados(ruta, anios_a_sumar):
    egresos_dco = pd.read_excel(ruta, sheet_name="dias_estada_estimados_RDR")
    egresos_dco = egresos_dco[anios_a_sumar].sum()

    return egresos_dco


def leer_consultas_urgencia_proyectadas(ruta):
    # Lee la base de urgencias
    df = (
        pd.read_excel(ruta, sheet_name="proyeccion_consultas")
        .rename(columns={"Unnamed: 0": "tipo_consulta"})
        .set_index(["tipo_consulta"])
    )

    # Aisla solamente las consultas C
    consultas_c1_a_c3 = df.query("tipo_consulta.isin(['C1', 'C2', 'C3'])").sum()
    consultas_c4_y_c5 = df.query("tipo_consulta.isin(['C4', 'C5'])").sum()
    total_consultas = df.query("tipo_consulta == 'Total'").sum()

    return consultas_c1_a_c3, consultas_c4_y_c5, total_consultas


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = PROCESSED_DATA_DIR / "dataset.csv",
    output_path: Path = PROCESSED_DATA_DIR / "features.csv",
    # -----------------------------------------
):
    # ---- REPLACE THIS WITH YOUR OWN CODE ----
    logger.info("Generating features from dataset...")
    for i in tqdm(range(10), total=10):
        if i == 5:
            logger.info("Something happened for iteration 5.")
    logger.success("Features generation complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
