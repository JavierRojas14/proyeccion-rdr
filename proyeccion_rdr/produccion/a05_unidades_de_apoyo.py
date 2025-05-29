import numpy as np
import pandas as pd


def leer_cae(ruta):
    df = pd.read_csv(ruta, dtypes={"id_paciente": str})
    return df


def leer_grd(ruta):
    df = pd.read_csv(ruta, dtypes={"id_paciente": str})
