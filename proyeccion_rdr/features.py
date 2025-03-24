from pathlib import Path

import pandas as pd
import polars as pl
import typer
from loguru import logger
from tqdm import tqdm

from proyeccion_rdr.config import PROCESSED_DATA_DIR

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
