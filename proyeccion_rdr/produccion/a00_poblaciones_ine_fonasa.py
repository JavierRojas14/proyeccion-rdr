import pandas as pd

COMUNAS_SSMC = [
    "Cerrillos",
    "Estación Central",
    "Maipú",
    "Santiago",
]

COMUNAS_SSMN = [
    "Colina",
    "Conchalí",
    "Huechuraba",
    "Independencia",
    "Lampa",
    "Quilicura",
    "Recoleta",
    "Tiltil",
]

COMUNAS_SSMO = [
    "Isla De Pascua",
    "La Reina",
    "Las Condes",
    "Lo Barnechea",
    "Macul",
    "Ñuñoa",
    "Peñalolén",
    "Providencia",
    "Vitacura",
]

COMUNAS_SSMOC = [
    "Cerro Navia",
    "Lo Prado",
    "Melipilla",
    "Peñaflor",
    "Pudahuel",
    "Quinta Normal",
    "Renca",
    "Talagante",
]

COMUNAS_SSMS = [
    "Buin",
    "Calera De Tango",
    "El Bosque",
    "La Cisterna",
    "Lo Espejo",
    "Paine",
    "Pedro Aguirre Cerda",
    "San Bernardo",
    "San Joaquín",
    "San Miguel",
]

COMUNAS_SSMSO = [
    "La Florida",
    "La Granja",
    "La Pintana",
    "Pirque",
    "Puente Alto",
    "San José De Maipo",
    "San Ramón",
]

COMUNAS_RM_SIN_SS = [
    "Alhué",
    "Curacaví",
    "El Monte",
    "Isla De Maipo",
    "María Pinto",
    "Padre Hurtado",
    "San Pedro",
]

TODOS_LOS_SERVICIOS_RM = {
    "COMUNAS_SIN_SS_EN_RM": COMUNAS_RM_SIN_SS,
    "SSMC": COMUNAS_SSMC,
    "SSMN": COMUNAS_SSMN,
    "SSMO": COMUNAS_SSMO,
    "SSMOC": COMUNAS_SSMOC,
    "SSMS": COMUNAS_SSMS,
    "SSMSO": COMUNAS_SSMSO,
}

ANIO_INICIO = 2017
ANIO_TERMINO = 2035

COLUMNAS_POBLACION_INE = [f"{i}" for i in range(ANIO_INICIO, ANIO_TERMINO + 1)]
COLUMNAS_POBLACION_FONASA = ["2018", "2019", "2020", "2021", "2022", "2023"]
COLUMNAS_POBLACION_INE_NUEVO = [f"{i}" for i in [2025, 2030, 2040]]


def procesar_resultados_por_estrato_y_grupos_etarios(
    dataframes, consultas, columnas, tipo_poblacion
):
    """
    Procesa los resultados por estratos y grupos etarios para un tipo de población dado.

    Parámetros:
        dataframes (dict): Diccionario de DataFrames de pandas. Contiene los distintos estratos
        a calcular ("Pais", "Region", "SSMO", etc)
        consultas (dict): Diccionario de consultas. Filtra por Grupos etarios
        (">15 anios", "entre 20 y 25", etc)
        columnas (list): Lista de nombres de columnas para calcular la suma. Puede ser
        "CUENTA BENEFICIARIO" o los anios a sumar en INE
        tipo_poblacion (str): Tipo de población ("INE" o "FONASA").

    Retorna:
        DataFrame: DataFrame concatenado con los resultados procesados y el índice restablecido.
    """
    print(">> Procesando (Filtrar por edad y sexo) las consultas para cada uno de los estratos")
    # Obtener resultados por estratos y grupos etarios
    poblaciones_estratos_calculados = iterar_consultas(
        dataframes, consultas, columnas, tipo_poblacion
    )

    # Unir todos los resultados en un único DataFrame
    poblaciones_estratos_calculados = pd.concat(poblaciones_estratos_calculados)
    poblaciones_estratos_calculados = poblaciones_estratos_calculados.reset_index(
        names=["Edad Incidencia", "Estrato"]
    )

    # Pone como indice la edad y el estrato
    poblaciones_estratos_calculados = poblaciones_estratos_calculados.set_index(
        ["Edad Incidencia", "Estrato"]
    )

    return poblaciones_estratos_calculados


def iterar_consultas(dataframes, consultas, columnas_a_sumar, tipo_poblacion="INE"):
    """
    Iterar sobre un diccionario de consultas, filtrar DataFrames y calcular la suma de columnas
    especificadas.

    Parámetros:
        dataframes (dict): Diccionario de DataFrames de pandas.
        consultas (dict): Diccionario de consultas.
        columnas_a_sumar (list): Lista de nombres de columnas para calcular la suma.

    Retorna:
        dict: Diccionario que contiene DataFrames filtrados y sus sumas para cada consulta.
    """
    resultado = {}

    for nombre_consulta, consulta in consultas.items():
        print(f">>> Filtro a realizar: {nombre_consulta} - {consulta}")
        # Filtrar DataFrames
        if consulta:
            dfs_filtrados = filtrar_dataframes(dataframes, consulta)
        else:
            dfs_filtrados = dataframes

        # Calcular suma de columnas según el tipo de población
        if tipo_poblacion == "INE":
            suma_dfs = calcular_suma_columnas(dfs_filtrados, columnas_a_sumar)
        elif tipo_poblacion == "FONASA":
            suma_dfs = calcular_suma_columnas_fonasa(dfs_filtrados, columnas_a_sumar)

        resultado[nombre_consulta] = suma_dfs

    return resultado


def calcular_suma_columnas(dataframes_dict, columnas):
    """
    Calcular la suma de columnas especificadas para cada DataFrame en un diccionario.

    Parámetros:
        dataframes_dict (dict): Diccionario de DataFrames de pandas.
        columnas (list): Lista de nombres de columnas para calcular la suma.

    Retorna:
        DataFrame: DataFrame que contiene las sumas de las columnas especificadas para
        cada DataFrame, con claves del DataFrame como índice.
    """
    sumas = {key: df[columnas].sum() for key, df in dataframes_dict.items()}
    return pd.DataFrame(sumas).T


def calcular_suma_columnas_fonasa(dataframes_dict, columnas):
    """
    Calcular la suma de columnas especificadas para cada DataFrame en un diccionario agrupado
    por 'ANO_INFORMACION'.

    Parámetros:
        dataframes_dict (dict): Diccionario de DataFrames de pandas.
        columnas (list): Lista de nombres de columnas para calcular la suma.

    Retorna:
        DataFrame: DataFrame que contiene las sumas de las columnas especificadas para
        cada DataFrame, con claves del DataFrame como índice.
    """

    sumas = {
        key: df.groupby("ANO_INFORMACION")[columnas].sum() for key, df in dataframes_dict.items()
    }
    return pd.DataFrame(sumas).T


def filtrar_dataframes(dataframes, consulta):
    """
    Filtrar una lista de DataFrames usando una consulta.

    Parámetros:
        dataframes (dict): Diccionario de DataFrames de pandas.
        consulta (str): Consulta para filtrar los DataFrames.

    Retorna:
        dict: Diccionario de DataFrames filtrados.
    """
    return {key: df.query(consulta).copy() for key, df in dataframes.items()}


def cargar_datos(ruta_ine, ruta_fonasa, ruta_ine_nuevo):
    """
    Carga los datos desde archivos CSV para INE y FONASA.

    Args:
        ruta_ine (str): Ruta al archivo CSV de datos INE.
        ruta_fonasa (str): Ruta al archivo CSV de datos FONASA.

    Returns:
        tuple: DataFrames de INE y FONASA.
    """
    print("> Cargando datos INE y FONASA")
    df_ine = pd.read_csv(ruta_ine)
    df_fonasa = pd.read_csv(ruta_fonasa, dtype={"ANO_INFORMACION": str})
    df_ine_nuevo = pd.read_csv(ruta_ine_nuevo)
    return df_ine, df_fonasa, df_ine_nuevo


def preparar_estratos_ine(df_ine, todos_los_servicios_rm):
    """
    Prepara los estratos para los datos de INE.

    Args:
        df_ine (pd.DataFrame): DataFrame de datos INE.
        todos_los_servicios_rm (dict): Diccionario de servicios por comuna.

    Returns:
        dict: Diccionario de estratos para INE.
    """
    print("> Filtrando por estratos (Regiones, Servicios y Comunas) en INE")
    estratos = {"Pais": df_ine.copy()}

    # Por región
    for region in sorted(df_ine["M"].unique()):
        estratos[region] = df_ine.query("M == @region").copy()

    # Por servicios
    for nombre_servicio, comunas in todos_los_servicios_rm.items():
        estratos[nombre_servicio] = df_ine.query("`Nombre Comuna`.isin(@comunas)")

    # Por comuna en SSMN
    for comuna in todos_los_servicios_rm["SSMN"]:
        estratos[comuna] = df_ine.query("`Nombre Comuna` == @comuna")

    return estratos


def preparar_estratos_fonasa(df_fonasa, todos_los_servicios_rm):
    """
    Prepara los estratos para los datos de FONASA.

    Args:
        df_fonasa (pd.DataFrame): DataFrame de datos FONASA.
        todos_los_servicios_rm (dict): Diccionario de servicios por comuna.

    Returns:
        dict: Diccionario de estratos para FONASA.
    """
    print("> Filtrando por estratos (Regiones, Servicios y Comunas) en FONASA")
    estratos = {"Pais": df_fonasa.copy()}

    # Por región
    for region in sorted(df_fonasa["REGION"].fillna("").unique()):
        if region:
            estratos[region] = df_fonasa.query("REGION == @region").copy()

    # Por servicios
    for nombre_servicio, comunas in todos_los_servicios_rm.items():
        estratos[nombre_servicio] = df_fonasa.query("COMUNA.isin(@comunas)")

    # Por comuna en SSMN
    for comuna in todos_los_servicios_rm["SSMN"]:
        estratos[comuna] = df_fonasa.query("COMUNA == @comuna")

    return estratos


def calcular_porcentaje_fonasa(poblaciones_ine, poblaciones_fonasa, columnas_anios_a_calcular):
    """
    Calcula el porcentaje FONASA basado en las poblaciones INE y FONASA.

    Args:
        poblaciones_ine (pd.DataFrame): DataFrame de poblaciones INE.
        poblaciones_fonasa (pd.DataFrame): DataFrame de poblaciones FONASA.
        columnas_anios_a_calcular (list): Lista con los anios que se utilizar para calcular los %

    Returns:
        pd.DataFrame: DataFrame con los porcentajes FONASA.
    """
    print("> Calculando cada uno de los porcentajes por estratos y grupos etarios")
    # Solamente deja las columnas que se quieran utilizar para calcular el %
    poblaciones_ine_anios_interes = poblaciones_ine[columnas_anios_a_calcular].copy()
    poblaciones_fonasa_anios_interes = poblaciones_fonasa[columnas_anios_a_calcular].copy()

    # Calcula el acumulado del periodo
    poblaciones_ine_anios_interes["acumulado"] = poblaciones_ine_anios_interes.sum(axis=1)
    poblaciones_fonasa_anios_interes["acumulado"] = poblaciones_fonasa_anios_interes.sum(axis=1)

    # Calcula el porcentaje para cada estrato
    porcentaje = poblaciones_fonasa_anios_interes / poblaciones_ine_anios_interes
    porcentaje.columns = "porcentaje_fonasa_" + porcentaje.columns
    return porcentaje


def extrapolar_poblacion_fonasa(poblaciones_ine, porcentaje_fonasa, columna_porcentaje):
    """
    Extrapola las poblaciones FONASA usando las poblaciones INE y los porcentajes FONASA.

    Args:
        poblaciones_ine (pd.DataFrame): DataFrame de poblaciones INE.
        porcentaje_fonasa (pd.DataFrame): DataFrame de porcentajes FONASA.
        columna_porcentaje (str): Nombre de la columna de porcentaje a usar.

    Returns:
        pd.DataFrame: DataFrame de poblaciones extrapoladas.
    """
    print(f"> Extrapolando poblaciones FONASA con {columna_porcentaje}")
    return poblaciones_ine.mul(porcentaje_fonasa[columna_porcentaje], axis=0)


def resetear_indices_dataframes(
    poblaciones_ine,
    poblaciones_fonasa,
    poblaciones_ine_nuevo,
    poblaciones_fonasa_extrapoladas,
    poblaciones_fonasa_extrapoladas_ine_nuevo,
    nombre_nuevas_cols,
):
    # Crea copia de las poblaciones para modificarlas
    df_ine = poblaciones_ine.copy()
    df_fonasa = poblaciones_fonasa.copy()
    df_ine_nuevo = poblaciones_ine_nuevo.copy()
    df_fonasa_extrapol = poblaciones_fonasa_extrapoladas.copy()
    df_fonasa_extrapol_ine_nuevo = poblaciones_fonasa_extrapoladas_ine_nuevo.copy()

    # Resetea los indices de los 3 dataframes
    df_ine = df_ine.reset_index(names=nombre_nuevas_cols)
    df_fonasa = df_fonasa.reset_index(names=nombre_nuevas_cols)
    df_ine_nuevo = df_ine_nuevo.reset_index(names=nombre_nuevas_cols)
    df_fonasa_extrapol = df_fonasa_extrapol.reset_index(names=nombre_nuevas_cols)
    df_fonasa_extrapol_ine_nuevo = df_fonasa_extrapol_ine_nuevo.reset_index(
        names=nombre_nuevas_cols
    )

    return df_ine, df_fonasa, df_ine_nuevo, df_fonasa_extrapol, df_fonasa_extrapol_ine_nuevo


def procesar_poblaciones(
    ruta_ine,
    ruta_fonasa,
    ruta_ine_nuevo,
    todos_los_servicios_rm,
    query_strings_ine,
    query_strings_fonasa,
    columnas_poblacion_ine,
    columnas_poblacion_ine_nuevo,
):
    """
    Procesa los datos de INE y FONASA para obtener las poblaciones extrapoladas.

    Args:
        ruta_ine (str): Ruta al archivo CSV de datos INE.
        ruta_fonasa (str): Ruta al archivo CSV de datos FONASA.
        todos_los_servicios_rm (dict): Diccionario de servicios por comuna.
        query_strings_ine (dict): Consultas de grupos etarios para INE.
        query_strings_fonasa (dict): Consultas de grupos etarios para FONASA.
        columnas_poblacion_ine (list): Columnas a sumar para INE.

    Returns:
        tuple: DataFrames de poblaciones extrapoladas y porcentajes FONASA.
    """
    # Cargar datos
    df_ine, df_fonasa, df_ine_nuevo = cargar_datos(ruta_ine, ruta_fonasa, ruta_ine_nuevo)

    # Preparar estratos
    estratos_ine = preparar_estratos_ine(df_ine, todos_los_servicios_rm)
    estratos_fonasa = preparar_estratos_fonasa(df_fonasa, todos_los_servicios_rm)
    estratos_ine_nuevo = preparar_estratos_ine(df_ine_nuevo, todos_los_servicios_rm)

    # Procesar resultados por estratos y grupos etarios
    poblaciones_ine = procesar_resultados_por_estrato_y_grupos_etarios(
        estratos_ine, query_strings_ine, columnas_poblacion_ine, "INE"
    )

    poblaciones_fonasa = procesar_resultados_por_estrato_y_grupos_etarios(
        estratos_fonasa, query_strings_fonasa, "CUENTA_BENEFICIARIOS", "FONASA"
    )

    poblaciones_ine_nuevo = procesar_resultados_por_estrato_y_grupos_etarios(
        estratos_ine_nuevo, query_strings_ine, columnas_poblacion_ine_nuevo, "INE"
    )

    # Calcular porcentaje FONASA y cambia el porcentaje de los recien nacidos vivos
    columnas_anios_a_calcular = ["2018", "2019", "2020", "2021", "2022"]
    porcentajes_fonasa = calcular_porcentaje_fonasa(
        poblaciones_ine, poblaciones_fonasa, columnas_anios_a_calcular
    )

    # Extrae el porcentaje fonasa del grupo etario de 0 a 19
    porcentaje_fonasa_0_a_19 = porcentajes_fonasa.loc["entre_0_y_19"].values

    # Asigna el porcentaje anterior a tales estratos
    porcentajes_fonasa.loc["recien_nacidos_vivos"] = porcentaje_fonasa_0_a_19
    porcentajes_fonasa.loc["entre_1_y_14"] = porcentaje_fonasa_0_a_19
    porcentajes_fonasa.loc["entre_15_y_18"] = porcentaje_fonasa_0_a_19
    porcentajes_fonasa.loc["entre_0_y_18"] = porcentaje_fonasa_0_a_19
    porcentajes_fonasa.loc["recien_nacidos_vivos_hombres"] = porcentaje_fonasa_0_a_19
    porcentajes_fonasa.loc["entre_10_y_18"] = porcentaje_fonasa_0_a_19
    porcentajes_fonasa.loc["hasta_10"] = porcentaje_fonasa_0_a_19
    porcentajes_fonasa.loc["entre_0_y_15"] = porcentaje_fonasa_0_a_19

    # Extrapolar poblaciones FONASA
    PORCENTAJE_FONASA_A_UTILIZAR = "porcentaje_fonasa_acumulado"
    poblaciones_fonasa_extrapoladas = extrapolar_poblacion_fonasa(
        poblaciones_ine, porcentajes_fonasa, PORCENTAJE_FONASA_A_UTILIZAR
    )

    # Extrapola poblaciones FONASA con el nuevo INE
    poblaciones_fonasa_extrapoladas_ine_nuevo = extrapolar_poblacion_fonasa(
        poblaciones_ine_nuevo, porcentajes_fonasa, PORCENTAJE_FONASA_A_UTILIZAR
    )

    # Resetea el indice de los DataFrames de poblaciones
    COLS_NUEVAS = ["Edad Incidencia", "Estrato"]
    (
        poblaciones_ine,
        poblaciones_fonasa,
        poblaciones_ine_nuevo,
        poblaciones_fonasa_extrapoladas,
        poblaciones_fonasa_extrapoladas_ine_nuevo,
    ) = resetear_indices_dataframes(
        poblaciones_ine,
        poblaciones_fonasa,
        poblaciones_ine_nuevo,
        poblaciones_fonasa_extrapoladas,
        poblaciones_fonasa_extrapoladas_ine_nuevo,
        COLS_NUEVAS,
    )

    return (
        df_ine,
        df_fonasa,
        poblaciones_ine,
        poblaciones_fonasa,
        poblaciones_ine_nuevo,
        porcentajes_fonasa,
        poblaciones_fonasa_extrapoladas,
        poblaciones_fonasa_extrapoladas_ine_nuevo,
    )


if __name__ == "__main__":
    # Definir rutas y parámetros
    RUTA_INE = "data/processed/df_ine.csv"
    RUTA_FONASA = "data/processed/df_fonasa.csv"
    RUTA_INE_NUEVO = "data/processed/df_ine_nuevo.csv"
    QUERY_STRINGS_INE = {
        "todos": "",  # Todo el pais
        "hombres": "hombre_mujer == 1",  # Hombres
        "mujeres": "hombre_mujer == 2",  # Mujeres
        "recien_nacidos_vivos_hombres": "Edad == 0 and hombre_mujer == 1",  # Recien Nacidos Vivos Hombres
        "recien_nacidos_vivos": "Edad == 0",  # Recien Nacidos Vivos
        "entre_1_y_14": "Edad >= 1 and Edad <= 14",  # Entre 1 y 14 anios de edad
        "entre_15_y_18": "Edad >= 15 and Edad <= 18",  # Entre 15 y 18 anios de edad
        "entre_0_y_19": "Edad <= 19",  # Entre los 0 y 19 anios
        "entre_0_y_18": "Edad <= 18",  # Entre los 0 y 18 anios
        "hasta_3": "Edad <= 3",  # Hasta 3 anios
        "entre_10_y_18": "Edad >= 10 and Edad <= 18",  # Entre 10 y 18
        "hasta_10": "Edad <= 10",  # Hasta los 10
        "entre_0_y_15": "Edad >= 0 and Edad <= 15",
    }
    QUERY_STRINGS_FONASA = {
        "todos": "",  # Todo el pais
        "hombres": "SEXO == 'HOMBRE'",  # Hombres
        "mujeres": "SEXO == 'MUJER'",  # Mujeres
        "recien_nacidos_vivos_hombres": "EDAD_TRAMO == 0 and SEXO == 'HOMBRE'",  # RNV Hombres
        "recien_nacidos_vivos": "EDAD_TRAMO == 0",  # Recien Nacidos Vivos (Incluye los de 1 y 2 anios)
        "entre_1_y_14": "EDAD_TRAMO < 15",  # Entre 0 y 14 anios de edad
        "entre_15_y_18": "EDAD_TRAMO == 15",  # Entre 15 y 19 anios
        "entre_0_y_19": "EDAD_TRAMO < 20",  # Entre 0 y 19 anios
        "entre_0_y_18": "EDAD_TRAMO < 20",  # Es igual a entre 0 y 19 anios
        "hasta_3": "EDAD_TRAMO == 0",  # Hasta 3 anios (0, 1 y 2 anios)
        "entre_10_y_18": "EDAD_TRAMO == 0",  # Entre 10 y 18
        "hasta_10": "EDAD_TRAMO == 0",  # Hasta los 10
        "entre_0_y_15": "EDAD_TRAMO < 15",
    }

    # Procesar datos
    (
        df_ine,
        df_fonasa,
        poblacion_ine,
        poblacion_fonasa,
        poblaciones_ine_nuevo,
        porcentaje_fonasa,
        poblaciones_fonasa_extrapoladas,
        poblaciones_fonasa_extrapoladas_ine_nuevo,
    ) = procesar_poblaciones(
        RUTA_INE,
        RUTA_FONASA,
        RUTA_INE_NUEVO,
        TODOS_LOS_SERVICIOS_RM,
        QUERY_STRINGS_INE,
        QUERY_STRINGS_FONASA,
        COLUMNAS_POBLACION_INE,
        COLUMNAS_POBLACION_INE_NUEVO,
    )

    # Guardar o analizar resultados
    # print(poblaciones_fonasa_extrapoladas.head())
    # print(porcentaje_fonasa.head())
    with pd.ExcelWriter("data/interim/0.0_poblaciones_ine_y_fonasa_a_utilizar.xlsx") as file:
        poblacion_ine.to_excel(file, sheet_name="poblacion_INE", index=False)
        poblacion_fonasa.to_excel(file, sheet_name="poblacion_FONASA", index=False)
        poblaciones_ine_nuevo.to_excel(file, sheet_name="poblacion_INE_censo_2024", index=False)
        porcentaje_fonasa.reset_index().to_excel(
            file, sheet_name="porcentaje_FONASA_por_estrato", index=False
        )
        poblaciones_fonasa_extrapoladas.to_excel(
            file, sheet_name="poblaciones_fonasa_extrapoladas", index=False
        )
        poblaciones_fonasa_extrapoladas_ine_nuevo.to_excel(
            file, sheet_name="pobl_fonasa_extrapol_censo_2024", index=False
        )
