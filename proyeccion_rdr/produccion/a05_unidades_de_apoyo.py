import numpy as np
import pandas as pd
import polars as pl


def leer_cae(ruta):
    df = pd.read_csv(ruta, dtype={"id_paciente": str})
    return df


def leer_grd(ruta):
    df = pd.read_csv(ruta, dtype={"id_paciente": str})
    return df
