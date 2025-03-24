from pathlib import Path

import pandas as pd
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
