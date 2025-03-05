import glob
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import typer
from loguru import logger
from tqdm import tqdm

from proyeccion_rdr.config import PROCESSED_DATA_DIR, RAW_DATA_DIR

app = typer.Typer()

TRANSFORMACION_ESTRATOS_FONASA = TRANSFORMACION_ESTRATOS_FONASA = {
    "DE ANTOFAGASTA": "Antofagasta",
    "DE ARICA Y PARINACOTA": "Arica y Parinacota",
    "DE ATACAMA": "Atacama",
    "DE AYSÉN DEL GRAL. C. IBÁÑEZ DEL CAMPO": "Aysén del General Carlos Ibáñez del Campo",
    "DE COQUIMBO": "Coquimbo",
    "DE LA ARAUCANÍA": "La Araucanía",
    "DE LOS LAGOS": "Los Lagos",
    "DE LOS RÍOS": "Los Ríos",
    "DE MAGALLANES Y DE LA ANTÁRTICA CHILENA": "Magallanes y de la Antártica Chilena",
    "DE TARAPACÁ": "Tarapacá",
    "DE VALPARAÍSO": "Valparaíso",
    "DE ÑUBLE": "Ñuble",
    "DEL BÍOBÍO": "Biobío",
    "DEL LIBERTADOR GENERAL BERNARDO O'HIGGINS": "Libertador General Bernardo O'Higgins",
    "DEL MAULE": "Maule",
    "METROPOLITANA DE SANTIAGO": "Metropolitana de Santiago",
}

CAMBIOS_COLUMNAS_INNOMINADOS = {
    "MES_INFORMACION": "MES_INFORMACION",
    "TITULAR_CARGA": "TITULAR_CARGA",
    "TRAMO_FONASA": "TRAMO",
    "SEXO": "SEXO",
    "EDAD_TRAMO": "EDAD_TRAMO",
    "NACIONALIDAD": "NACIONALIDAD",
    "TIPO_ASEGURADO": "TIPO_ASEGURADO",
    "REGIÓN_BENEFICIARIO": "REGION",
    "COMUNA_BENEFICIARIO": "COMUNA",
}

COLUMNAS_A_OCUPAR_FONASA = [
    "MES_INFORMACION",
    "TITULAR_CARGA",
    "TRAMO",
    "SEXO",
    "EDAD_TRAMO",
    "NACIONALIDAD",
    "REGION",
    "COMUNA",
    "CUENTA_BENEFICIARIOS",
]


def procesar_ine(ruta_base_de_datos):
    print("> Procesando Base de datos INE")
    # Define ruta al archivo INE
    ruta_a_ine = (
        f"{ruta_base_de_datos}/1_poblacion_ine/estimaciones-y-proyecciones-2002-2035-comunas.xlsx"
    )
    # Lee la base de datos
    df = pd.read_excel(ruta_a_ine).iloc[:-2]

    # Elimina el sufijo 'Poblacion' de las columnas
    df.columns = df.columns.str.replace("Poblacion ", "")

    # Renombra columna de hombres o mujeres
    df = df.rename(columns={"Sexo\n1=Hombre\n2=Mujer": "hombre_mujer"})

    # Convierte el nombre de las comunas a title y elimina espacios en blanco
    df["Nombre Comuna"] = df["Nombre Comuna"].str.title().str.strip()

    # Indica si es adulto o infantil
    df["grupo_etario_poblacion"] = np.where(df["Edad"] >= 15, "Adulto", "Infantil")

    return df


def procesar_fonasa(ruta_base_de_datos):
    print("> Procesando Base de datos FONASA")
    # Indica la ruta de los archivos FONASA
    ruta_a_fonasa = f"{ruta_base_de_datos}/2_poblacion_fonasa/*.csv"

    # Encuentra todos los archivos que coinciden con el patrón
    archivos = glob.glob(ruta_a_fonasa)

    # Lee y concatena todos los archivos CSV
    df_fonasa = pd.concat(
        pd.read_csv(archivo, encoding="latin-1", usecols=COLUMNAS_A_OCUPAR_FONASA)
        for archivo in archivos
    )

    # Extrae el año de la columna MES_INFORMACION
    df_fonasa["ANO_INFORMACION"] = df_fonasa["MES_INFORMACION"].astype(str).str[:4]

    # Ordena la base de datos segun el anio de informacion
    df_fonasa = df_fonasa.sort_values("ANO_INFORMACION")

    # Formatea la columna REGION
    df_fonasa["REGION"] = df_fonasa["REGION"].str.upper().str.strip()
    df_fonasa["REGION"] = df_fonasa["REGION"].replace(
        {
            "ÑUBLE": "DE ÑUBLE",
            "DEL LIBERTADOR B. O'HIGGINS": "DEL LIBERTADOR GENERAL BERNARDO O'HIGGINS",
        }
    )

    # Elimina los registros con REGION desconocida o nula
    df_fonasa = df_fonasa.query("REGION != 'DESCONOCIDA'").copy()
    df_fonasa = df_fonasa.query("REGION.notna()").copy()

    # Aplica la transformación de estratos
    df_fonasa["REGION"] = df_fonasa["REGION"].replace(TRANSFORMACION_ESTRATOS_FONASA)

    # Formatea otras columnas
    df_fonasa["SEXO"] = df_fonasa["SEXO"].str.upper().str.strip()
    # df_fonasa["SERVICIO_SALUD"] = df_fonasa["SERVICIO_SALUD"].str.upper().str.strip()
    df_fonasa["COMUNA"] = df_fonasa["COMUNA"].str.title().str.strip()

    # Limpia y transforma la columna EDAD_TRAMO
    df_fonasa["EDAD_TRAMO"] = df_fonasa["EDAD_TRAMO"].replace(
        {"Más de 99 años": "99 años", "S.I.": "-1", "Sin información": "-1"}
    )
    df_fonasa["EDAD_TRAMO"] = df_fonasa["EDAD_TRAMO"].str.split().str[0].astype(int)
    df_fonasa["EDAD_TRAMO"] = df_fonasa["EDAD_TRAMO"].replace(-1, np.nan)

    return df_fonasa


def procesar_FONASA_innominadas(ruta_base_de_datos):
    print("> Procesando bases de datos FONASA innominadas")

    # Define la ruta de archivos innominados
    ruta_a_fonasa = (
        f"{ruta_base_de_datos}/2_poblacion_fonasa/innominados/Beneficiarios Fonasa 2023.csv"
    )

    df = pl.read_csv(ruta_a_fonasa, encoding="latin-1")

    cuenta_fonasa = (
        df.rename(CAMBIOS_COLUMNAS_INNOMINADOS)
        .group_by(CAMBIOS_COLUMNAS_INNOMINADOS.values())
        .len(name="CUENTA_BENEFICIARIOS")
    )

    return cuenta_fonasa


@app.command()
def main(
    # ---- REPLACE DEFAULT PATHS AS APPROPRIATE ----
    input_path: Path = RAW_DATA_DIR,
    output_path: Path = PROCESSED_DATA_DIR,
    # ----------------------------------------------
):
    # ---- REPLACE THIS WITH YOUR OWN CODE ----
    logger.info("Processing dataset...")
    # Procesa Y guarda FONASA innominados
    fonasa_innonimados_procesada = procesar_FONASA_innominadas(input_path)
    ruta_output_fonasa_innominados = (
        f"{input_path}/2_poblacion_fonasa/Beneficiarios Fonasa 2023.csv"
    )
    fonasa_innonimados_procesada.to_pandas().to_csv(
        ruta_output_fonasa_innominados, encoding="latin-1"
    )

    # Procesa INE y FONASA
    ine_procesada = procesar_ine(input_path)
    fonasa_procesada = procesar_fonasa(input_path)

    # Define nombres de archivos output de INE y FONASA
    ruta_output_ine = f"{output_path}/df_ine.csv"
    ruta_output_fonasa = f"{output_path}/df_fonasa.csv"

    # Guarda base INE y FONASA
    ine_procesada.to_csv(ruta_output_ine)
    fonasa_procesada.to_csv(ruta_output_fonasa)
    logger.success("Processing dataset complete.")
    # -----------------------------------------


if __name__ == "__main__":
    app()
