import pandas as pd

RUTA_PLANILLA_POBLACIONES = "data/interim/0.0_poblaciones_ine_y_fonasa_a_utilizar.xlsx"
RUTA_PLANILLA_INCIDENCIAS = (
    "data/raw/3_incidencias_y_porcentajes_marcoprocesos/incidencias_y_prevalencias_RDR.xlsx"
)
RUTA_PLANILLA_OUTPUT = "data/interim/0.1_casos_teoricos_diagnosticos.xlsx"

COLUMNAS_INCIDENCIA = [
    "Diagnostico",
    "Diagnosticos Contenidos",
    "Diagnostico agregado en el macroproceso",
    "Estadística",
    "Casos (Cada 100.000)",
    "Edad Incidencia",
    "Área de Influencia Formal",
    "Área de Influencia Propuesta",
    "Casos a hacerse cargo del Área de Influencia Propuesta",
    "Porcentaje Pacientes Hospitalizados",
    "Porcentaje Pacientes Quirúrgicos",
    "Especialidad Quirúrgica",
    "Porcentaje Pacientes Hemodinamia",
]

ANIO_INICIO = 2017
ANIO_TERMINO = 2035
COLUMNAS_POBLACION_INE = [f"{i}" for i in range(ANIO_INICIO, ANIO_TERMINO + 1)]


def leer_planilla_poblaciones(ruta_planilla):
    """Lee la planilla de poblaciones INE y FONASA. Contiene multiples hojas."""
    dfs = pd.read_excel(ruta_planilla, sheet_name=None)

    return dfs.values()


def filtrar_diagnosticos_limitados_por_oferta(df_incidencias):
    """
    Filtra los diagnósticos limitados por oferta de un DataFrame de incidencias.
    """
    incidencias_sin_acotados_por_oferta = df_incidencias.query(
        "`Estadística` != 'Acotado por oferta'"
    )
    acotados_por_oferta = df_incidencias.query("`Estadística` == 'Acotado por oferta'")
    return incidencias_sin_acotados_por_oferta, acotados_por_oferta


def convertir_incidencia_a_numeros(df_incidencias):
    """
    Convierte las incidencias a números e imputa las faltantes como NaN.
    """
    df_incidencias["Casos (Cada 100.000)"] = pd.to_numeric(
        df_incidencias["Casos (Cada 100.000)"], errors="coerce"
    )
    return df_incidencias


def calcular_rate_incidencia(df_incidencias):
    """
    Calcula la fracción de la incidencia para multiplicar con la población directamente.
    """
    df_incidencias["rate_incidencia"] = df_incidencias["Casos (Cada 100.000)"] / 100000
    return df_incidencias


def corregir_prevalencias(df_incidencias):
    """
    Corrige las prevalencias en 5 años.
    """
    idx_prevalencias = df_incidencias["Estadística"] == "Prevalencia"
    df_incidencias.loc[idx_prevalencias, "rate_incidencia"] = (
        df_incidencias.loc[idx_prevalencias, "rate_incidencia"] / 5
    )
    return df_incidencias


def leer_planilla_incidencias(ruta_planilla, columnas_a_utilizar):
    """
    Procesa las incidencias desde la lectura hasta la corrección de prevalencias.
    """
    incidencias = pd.read_excel(ruta_planilla, usecols=columnas_a_utilizar)
    incidencias, limitados_por_oferta = filtrar_diagnosticos_limitados_por_oferta(incidencias)
    incidencias = convertir_incidencia_a_numeros(incidencias)
    incidencias = calcular_rate_incidencia(incidencias)
    incidencias = corregir_prevalencias(incidencias)
    return incidencias, limitados_por_oferta


# Función para separar y limpiar las áreas de influencia propuestas
def procesar_areas_de_influencia(df, columna_areas):
    """
    Separa las áreas de influencia por comas, las convierte en una lista,
    y las limpia quitando espacios extra.

    Args:
        dataframe (pd.DataFrame): DataFrame original.
        columna_areas (str): Nombre de la columna con las áreas de influencia propuestas.

    Returns:
        pd.DataFrame: DataFrame con las áreas de influencia separadas.
    """
    df = df.copy()
    df[columna_areas] = df[columna_areas].str.split(",")
    df = df.explode(columna_areas)
    df[columna_areas] = df[columna_areas].str.strip()
    return df


# Función para filtrar filas donde las áreas de influencia coinciden con el estrato
def filtrar_por_estrato(dataframe, columna_areas, columna_estrato):
    """
    Filtra las filas donde el área de influencia propuesta coincide con el estrato.

    Args:
        dataframe (pd.DataFrame): DataFrame procesado.
        columna_areas (str): Nombre de la columna con las áreas de influencia propuestas.
        columna_estrato (str): Nombre de la columna con los estratos.

    Returns:
        pd.DataFrame: DataFrame filtrado.
    """
    return dataframe.query(f"`{columna_areas}` == `{columna_estrato}`")


# Función para calcular casos asignados por área de influencia
def calcular_casos_a_hacerse_cargo(dataframe, columnas_poblacion, columna_casos):
    """
    Multiplica las columnas de población por los casos a hacerse cargo.

    Args:
        dataframe (pd.DataFrame): DataFrame filtrado.
        columnas_poblacion (list): Lista de nombres de columnas de población.
        columna_casos (str): Columna que contiene los factores de casos a hacerse cargo.

    Returns:
        pd.DataFrame: DataFrame actualizado con los valores calculados.
    """
    df = dataframe.copy()
    df[columnas_poblacion] = df[columnas_poblacion].mul(df[columna_casos], axis=0)
    return df


# Función para consolidar datos por diagnóstico
def consolidar_por_diagnostico(dataframe, columnas_poblacion):
    """
    Agrupa las columnas de población por diagnóstico y calcula la suma.

    Args:
        dataframe (pd.DataFrame): DataFrame con datos por diagnóstico.
        columnas_poblacion (list): Lista de nombres de columnas de población.

    Returns:
        pd.DataFrame: DataFrame consolidado por diagnóstico.
    """
    return dataframe.groupby("Diagnostico")[columnas_poblacion].sum()


# Función para unir incidencias con áreas de influencia
def unir_con_incidencias(incidencias, dataframe, indice):
    """
    Une un DataFrame con incidencias a otro DataFrame basado en el índice.

    Args:
        incidencias (pd.DataFrame): DataFrame con datos de incidencia.
        dataframe (pd.DataFrame): DataFrame con datos calculados por diagnóstico.
        indice (str): Nombre de la columna índice para unir.

    Returns:
        pd.DataFrame: DataFrame combinado.
    """
    return incidencias.set_index(indice).join(dataframe)


# Implementación modular
def acotar_casos_de_problemas_de_salud_a_areas_de_influencias_hospital(
    casos_fonasa_ine, incidencias, columnas_poblacion, columna_areas, columna_estrato, columna_casos
):
    """
    Realiza todo el flujo de procesamiento para las áreas de influencia y consolidación de casos.

    Args:
        casos_fonasa_ine (pd.DataFrame): DataFrame original con los casos.
        incidencias (pd.DataFrame): DataFrame con datos de incidencia.
        columnas_poblacion (list): Lista de columnas de población.
        columna_areas (str): Nombre de la columna con las áreas de influencia propuestas.
        columna_estrato (str): Nombre de la columna con los estratos.
        columna_casos (str): Nombre de la columna con casos a hacerse cargo.

    Returns:
        tuple: Dos DataFrames combinados con las incidencias y las áreas de influencia.
    """
    # Paso 1: Procesar áreas de influencia
    df_areas = procesar_areas_de_influencia(casos_fonasa_ine, columna_areas)

    # Paso 2: Filtrar por estrato
    df_coincide_a_de_infl_desglosado = filtrar_por_estrato(df_areas, columna_areas, columna_estrato)

    # Paso 3: Calcular casos a hacerse cargo
    df_casos_a_hacerse_cargo_desglosado = calcular_casos_a_hacerse_cargo(
        df_coincide_a_de_infl_desglosado, columnas_poblacion, columna_casos
    )

    # Paso 4: Consolidar datos por diagnóstico
    df_coincide_a_de_infl_consolidado = consolidar_por_diagnostico(
        df_coincide_a_de_infl_desglosado, columnas_poblacion
    )
    df_casos_a_hacerse_cargo_consolidado = consolidar_por_diagnostico(
        df_casos_a_hacerse_cargo_desglosado, columnas_poblacion
    )

    # Paso 5: Unir con incidencias
    df_coincide_a_de_infl_final = unir_con_incidencias(
        incidencias, df_coincide_a_de_infl_consolidado, "Diagnostico"
    )
    df_casos_a_hacerse_cargo_final = unir_con_incidencias(
        incidencias, df_casos_a_hacerse_cargo_consolidado, "Diagnostico"
    )

    # Resetea indices
    df_coincide_a_de_infl_desglosado = df_coincide_a_de_infl_desglosado.reset_index()
    df_casos_a_hacerse_cargo_desglosado = df_casos_a_hacerse_cargo_desglosado.reset_index()
    df_coincide_a_de_infl_final = df_coincide_a_de_infl_final.reset_index()
    df_casos_a_hacerse_cargo_final = df_casos_a_hacerse_cargo_final.reset_index()

    return (
        df_coincide_a_de_infl_desglosado,
        df_casos_a_hacerse_cargo_desglosado,
        df_coincide_a_de_infl_final,
        df_casos_a_hacerse_cargo_final,
    )


def calcular_casos_macroproceso(
    casos_por_region,
    casos_consolidados,
    columnas_poblacion,
    columna_porcentaje,
    porcentaje_a_reemplazar=None,
):
    """
    Calcula los casos de un macroproceso multiplicando los casos por el porcentaje de pacientes
    del macroproceso.

    Args:
        casos_por_region (pd.DataFrame): DataFrame con los casos desglosados por región.
        casos_consolidados (pd.DataFrame): DataFrame con los casos consolidados.
        columnas_poblacion (list): Lista de columnas correspondientes a la población.
        columna_porcentaje (str): Nombre de la columna que contiene el porcentaje de pacientes
        del macroproceso.

    Returns:
        tuple: DataFrames actualizados con los casos del macroproceso por región y consolidados.
    """
    # Crea una copia de los DataFrames
    casos_por_region = casos_por_region.copy()
    casos_consolidados = casos_consolidados.copy()

    # Reemplaza el porcentaje de la columna si es que se especifica por el usuario
    if porcentaje_a_reemplazar:
        casos_por_region[columna_porcentaje] = porcentaje_a_reemplazar
        casos_consolidados[columna_porcentaje] = porcentaje_a_reemplazar

    # Calcula los casos de un macroproceso por región
    casos_por_region[columnas_poblacion] = casos_por_region[columnas_poblacion].mul(
        casos_por_region[columna_porcentaje], axis=0
    )

    # Elimina todas las filas donde hayan 0 casos o casos NaN
    casos_por_region = casos_por_region.dropna(subset="2017").query("`2017` > 0")

    # Calcula los casos de un macroproceso consolidados
    casos_consolidados[columnas_poblacion] = casos_consolidados[columnas_poblacion].mul(
        casos_consolidados[columna_porcentaje], axis=0
    )

    # Elimina todas las filas donde hayan 0 casos o casos NaN
    casos_consolidados = casos_consolidados.dropna(subset="2017").query("`2017` > 0")

    return casos_por_region, casos_consolidados


def consolidar_casos_macroproceso(
    casos_area_de_infl,
    casos_hosp,
    casos_uci,
    casos_uti,
    casos_medias,
    casos_quir,
    casos_cv,
    casos_ct,
    casos_hmd,
    columnas_anios,
    indice_a_asginar,
):
    # Genera copias de los DataFrames
    casos_area_de_infl = casos_area_de_infl.copy()
    casos_hosp = casos_hosp.copy()
    casos_uci = casos_uci.copy()
    casos_uti = casos_uti.copy()
    casos_medias = casos_medias.copy()
    casos_quir = casos_quir.copy()
    casos_cv = casos_cv.copy()
    casos_ct = casos_ct.copy()
    casos_hmd = casos_hmd.copy()

    # Genera una columna indicadora
    casos_area_de_infl["tipo_paciente"] = "area_de_influencia"
    casos_hosp["tipo_paciente"] = "hospitalizados"
    casos_uci["tipo_paciente"] = "UCI"
    casos_uti["tipo_paciente"] = "UTI"
    casos_medias["tipo_paciente"] = "medias"
    casos_quir["tipo_paciente"] = "quirurgicos"
    casos_cv["tipo_paciente"] = "CV"
    casos_ct["tipo_paciente"] = "CT"
    casos_hmd["tipo_paciente"] = "hmd"

    # Pone como indice el Diagnostico, tipo de paciente y estrato
    casos_area_de_infl = casos_area_de_infl[indice_a_asginar + columnas_anios]
    casos_hosp = casos_hosp[indice_a_asginar + columnas_anios]
    casos_uci = casos_uci[indice_a_asginar + columnas_anios]
    casos_uti = casos_uti[indice_a_asginar + columnas_anios]
    casos_medias = casos_medias[indice_a_asginar + columnas_anios]
    casos_quir = casos_quir[indice_a_asginar + columnas_anios]
    casos_cv = casos_cv[indice_a_asginar + columnas_anios]
    casos_ct = casos_ct[indice_a_asginar + columnas_anios]
    casos_hmd = casos_hmd[indice_a_asginar + columnas_anios]

    # Consolida los casos
    resumen_casos = pd.concat(
        [
            casos_area_de_infl,
            casos_hosp,
            casos_uci,
            casos_uti,
            casos_medias,
            casos_quir,
            casos_cv,
            casos_ct,
            casos_hmd,
        ]
    )
    resumen_casos["Diagnostico"] = resumen_casos["Diagnostico"].str.split(" - ").str[0]
    resumen_casos = resumen_casos.set_index(indice_a_asginar)

    return resumen_casos


def calcular_casos_incidencia(incidencias, poblaciones_ine, poblaciones_fonasa_extrapoladas):
    """Calcula los casos a partir de las incidencias y poblaciones."""
    # Une las incidencias con las poblaciones INE y calcula los casos
    df_casos_ine = incidencias.merge(poblaciones_ine, how="left", on="Edad Incidencia")
    df_poblacion_area_de_estudio = df_casos_ine.query("Estrato == 'Pais'")
    df_casos_ine[COLUMNAS_POBLACION_INE] = df_casos_ine[COLUMNAS_POBLACION_INE].mul(
        df_casos_ine["rate_incidencia"], axis=0
    )

    # Une las incidencias con las poblaciones FONASA extrapoladas y calcula los casos
    df_casos_fonasa = incidencias.merge(
        poblaciones_fonasa_extrapoladas, how="left", on="Edad Incidencia"
    )
    df_casos_fonasa[COLUMNAS_POBLACION_INE] = df_casos_fonasa[COLUMNAS_POBLACION_INE].mul(
        df_casos_fonasa["rate_incidencia"], axis=0
    )

    return df_casos_ine, df_poblacion_area_de_estudio, df_casos_fonasa


def ajustar_casos_por_limite_oferta(casos_df, columnas_poblacion):
    """
    Ajusta el número de casos basado en limitaciones de oferta.

    Parámetros:
    casos_df (pd.DataFrame): DataFrame con datos de casos.
    columnas_poblacion (list): Lista de columnas de población a actualizar.

    Retorna:
    pd.DataFrame: DataFrame ajustado con casos limitados por oferta.
    """
    casos_ajustados = casos_df.copy()
    for anio in columnas_poblacion:
        casos_ajustados[anio] = casos_ajustados["Casos (Cada 100.000)"]

    casos_ajustados["Estrato"] = "Acotado por oferta"
    return casos_ajustados


def unir_casos(df_original, df_nuevos_casos):
    """
    Une nuevos casos en un DataFrame existente.

    Parámetros:
    df_original (pd.DataFrame): DataFrame existente.
    df_nuevos_casos (pd.DataFrame): Nuevos casos a agregar.

    Retorna:
    pd.DataFrame: DataFrame combinado.
    """
    return pd.concat([df_original, df_nuevos_casos])


def obtener_casos_por_macroprocesos(
    casos_por_region, casos_consolidados, porcentaje_uci, porcentaje_uti, porcentaje_medias
):
    # Obtiene los casos hospitalizados por region y consolidados
    casos_hosp_por_region, casos_hosp_consolidados = calcular_casos_macroproceso(
        casos_por_region,
        casos_consolidados,
        COLUMNAS_POBLACION_INE,
        "Porcentaje Pacientes Hospitalizados",
    )

    # Obtiene los casos UCI por region y consolidados
    casos_uci_por_region, casos_uci_consolidados = calcular_casos_macroproceso(
        casos_hosp_por_region,
        casos_hosp_consolidados,
        COLUMNAS_POBLACION_INE,
        "Porcentaje Pacientes Hospitalizados",
        porcentaje_a_reemplazar=porcentaje_uci,
    )

    # Obtiene los casos UTI por region y consolidados
    casos_uti_por_region, casos_uti_consolidados = calcular_casos_macroproceso(
        casos_hosp_por_region,
        casos_hosp_consolidados,
        COLUMNAS_POBLACION_INE,
        "Porcentaje Pacientes Hospitalizados",
        porcentaje_a_reemplazar=porcentaje_uti,
    )

    # Obtiene los casos Medias por region y consolidados
    casos_medias_por_region, casos_medias_consolidados = calcular_casos_macroproceso(
        casos_hosp_por_region,
        casos_hosp_consolidados,
        COLUMNAS_POBLACION_INE,
        "Porcentaje Pacientes Hospitalizados",
        porcentaje_a_reemplazar=porcentaje_medias,
    )

    # Obtiene los casos quirurgicos por region y consolidados
    casos_quir_por_region, casos_quir_consolidados = calcular_casos_macroproceso(
        casos_por_region,
        casos_consolidados,
        COLUMNAS_POBLACION_INE,
        "Porcentaje Pacientes Quirúrgicos",
    )

    # Obtiene los casos quirurgicos CV por region y consolidados
    casos_quir_CV_por_region, casos_quir_CV_consolidados = casos_quir_por_region.query(
        "`Especialidad Quirúrgica` == 'CV'"
    ), casos_quir_consolidados.query("`Especialidad Quirúrgica` == 'CV'")

    # Obtiene los casos quirurgicos CT por region y consolidados
    casos_quir_CT_por_region, casos_quir_CT_consolidados = casos_quir_por_region.query(
        "`Especialidad Quirúrgica` == 'CT'"
    ), casos_quir_consolidados.query("`Especialidad Quirúrgica` == 'CT'")

    # Obtiene los casos de hemodinamia por region y consolidados
    casos_hmd_por_region, casos_hmd_consolidados = calcular_casos_macroproceso(
        casos_por_region,
        casos_consolidados,
        COLUMNAS_POBLACION_INE,
        "Porcentaje Pacientes Hemodinamia",
    )

    return (
        [
            casos_hosp_por_region,
            casos_uci_por_region,
            casos_uti_por_region,
            casos_medias_por_region,
            casos_quir_por_region,
            casos_quir_CV_por_region,
            casos_quir_CT_por_region,
            casos_hmd_por_region,
        ],
        [
            casos_hosp_consolidados,
            casos_uci_consolidados,
            casos_uti_consolidados,
            casos_medias_consolidados,
            casos_quir_consolidados,
            casos_quir_CV_consolidados,
            casos_quir_CT_consolidados,
            casos_hmd_consolidados,
        ],
    )


def calcular_casos_de_trazadoras(ruta_poblaciones, ruta_incidencias):
    # Lee la planilla de poblaciones INE y FONASA, junto a las poblaciones atingentes
    poblaciones_ine, poblacion_fonasa, porcentaje_fonasa, poblaciones_fonasa_extrapoladas = (
        leer_planilla_poblaciones(ruta_poblaciones)
    )

    # Lee la planilla de trazadoras del hospital
    incidencias, limitados_por_oferta = leer_planilla_incidencias(
        ruta_incidencias, COLUMNAS_INCIDENCIA
    )

    # Obtiene los casos para cada problema de salud INE y FONASA
    casos_INE, poblacion_area_de_estudio, casos_FONASA = calcular_casos_incidencia(
        incidencias, poblaciones_ine, poblaciones_fonasa_extrapoladas
    )

    # Acota los casos calculados solamente a los casos que requiere el hospital
    (
        casos_FONASA_por_region,
        casos_a_hacerse_cargo_por_region,
        casos_FONASA_consolidados,
        casos_a_hacerse_cargo_consolidados,
    ) = acotar_casos_de_problemas_de_salud_a_areas_de_influencias_hospital(
        casos_FONASA,
        incidencias,
        COLUMNAS_POBLACION_INE,
        "Área de Influencia Propuesta",
        "Estrato",
        "Casos a hacerse cargo del Área de Influencia Propuesta",
    )

    # Procesar casos limitados por oferta
    casos_por_limite_oferta = ajustar_casos_por_limite_oferta(
        limitados_por_oferta, COLUMNAS_POBLACION_INE
    )

    # Unir con casos por región y casos consolidados
    casos_a_hacerse_cargo_por_region = unir_casos(
        casos_a_hacerse_cargo_por_region, casos_por_limite_oferta
    )
    casos_a_hacerse_cargo_consolidados = unir_casos(
        casos_a_hacerse_cargo_consolidados, casos_por_limite_oferta
    )

    # Obtiene los casos de camas UCI, UTI y Medias
    PORCENTAJE_UCI = 0.21
    PORCENTAJE_UTI = 0.42
    PORCENTAJE_MEDIAS = 0.37

    # Obtiene los casos por los macroprocesos
    casos_macroprocesos_por_region, casos_macroprocesos_consolidados = (
        obtener_casos_por_macroprocesos(
            casos_a_hacerse_cargo_por_region,
            casos_a_hacerse_cargo_consolidados,
            PORCENTAJE_UCI,
            PORCENTAJE_UTI,
            PORCENTAJE_MEDIAS,
        )
    )

    # Consolida los casos por macroprocesos en una unica tabla
    columnas_indice = ["Diagnostico", "Diagnosticos Contenidos", "tipo_paciente", "Estrato"]
    casos_macroprocesos_por_region = consolidar_casos_macroproceso(
        casos_a_hacerse_cargo_por_region,
        *casos_macroprocesos_por_region,
        COLUMNAS_POBLACION_INE,
        columnas_indice,
    )

    columnas_indice = ["Diagnostico", "Diagnosticos Contenidos", "tipo_paciente"]
    casos_macroprocesos_consolidados = consolidar_casos_macroproceso(
        casos_a_hacerse_cargo_consolidados,
        *casos_macroprocesos_consolidados,
        COLUMNAS_POBLACION_INE,
        columnas_indice,
    )

    return {
        "incidencias": incidencias.reset_index(),
        "casos_area_de_estudio": poblacion_area_de_estudio.reset_index(),
        "casos_teoricos_INE": casos_INE.reset_index(),
        "casos_teoricos_FONASA": casos_FONASA.reset_index(),
        "casos_fonasa_consolidados": casos_FONASA_consolidados.reset_index(),
        "casos_macroproceso_por_region": casos_macroprocesos_por_region.reset_index(),
        "casos_macroproceso_consolidado": casos_macroprocesos_consolidados.reset_index(),
        "casos_a_hacerse_cargo_INT": casos_a_hacerse_cargo_consolidados,
    }


def generar_resumen_total_hospital(
    incidencias,
    casos_area_de_estudio,
    casos_ine,
    casos_fonasa,
    casos_fonasa_consolidados,
    casos_a_hacerse_cargo_consolidados,
    columnas_poblacion,
):
    # Obtiene datos de area de estudio
    resumen_area_de_estudio = casos_area_de_estudio.set_index(columnas_poblacion)

    # Obtiene casos INE
    resumen_casos_ine = casos_ine.query("Estrato == 'Pais'").set_index(columnas_poblacion)

    # Obtiene los casos FONASA a nivel Pais
    resumen_casos_fonasa = casos_fonasa.query("Estrato == 'Pais'").set_index(columnas_poblacion)

    # Obtiene el porcentaje a hacerse cargo del area de influencia

    # # Obtiene casos de Area de Influencia Totales
    resumen_area_de_influencia = casos_fonasa_consolidados.set_index(columnas_poblacion)

    # Obtiene casos a hacerse cargo del area de influencia
    resumen_casos_a_hacerse_cargo = casos_a_hacerse_cargo_consolidados.set_index(columnas_poblacion)

    # Obtiene el resumen total de casos teoricos
    resumen_total_casos_teoricos = pd.DataFrame(
        {
            "poblacion_ine_2035_area_de_estudio": resumen_area_de_estudio["2035"],
            "casos_teoricos_ine_2035_area_de_estudio": resumen_casos_ine["2035"],
            "casos_teoricos_fonasa_2035_area_de_estudio": resumen_casos_fonasa["2035"],
            "casos_teoricos_fonasa_2035_area_de_influencia": resumen_area_de_influencia["2035"],
            "casos_teoricos_fonasa_2035_a_hacerse_cargo": resumen_casos_a_hacerse_cargo["2035"],
        },
        index=resumen_casos_a_hacerse_cargo.index,
    ).reset_index()

    # Obtiene el porcentaje de FONASA
    resumen_total_casos_teoricos["porcentaje_fonasa_pais"] = (
        resumen_total_casos_teoricos["casos_teoricos_fonasa_2035_area_de_estudio"]
        / resumen_total_casos_teoricos["casos_teoricos_ine_2035_area_de_estudio"]
    )

    # Reordena las columnas del resumen
    orden_resumen_columnas = [
        "Diagnostico",
        "Diagnosticos Contenidos",
        "Diagnostico agregado en el macroproceso",
        "Estadística",
        "Casos (Cada 100.000)",
        "Edad Incidencia",
        "poblacion_ine_2035_area_de_estudio",
        "casos_teoricos_ine_2035_area_de_estudio",
        "porcentaje_fonasa_pais",
        "casos_teoricos_fonasa_2035_area_de_estudio",
        "Área de Influencia Formal",
        "Área de Influencia Propuesta",
        "casos_teoricos_fonasa_2035_area_de_influencia",
        "Casos a hacerse cargo del Área de Influencia Propuesta",
        "casos_teoricos_fonasa_2035_a_hacerse_cargo",
    ]

    resumen_total_casos_teoricos = resumen_total_casos_teoricos[orden_resumen_columnas]

    return resumen_total_casos_teoricos


def guardar_resultados(dict_resultados, ruta_archivo):
    with pd.ExcelWriter(ruta_archivo) as file:
        for hoja, df_a_guardar in dict_resultados.items():
            df_a_guardar.to_excel(file, sheet_name=hoja, index=False)


if __name__ == "__main__":
    resultados_poblacionales = calcular_casos_de_trazadoras(
        RUTA_PLANILLA_POBLACIONES, RUTA_PLANILLA_INCIDENCIAS
    )

    resumen_total = generar_resumen_total_hospital(
        incidencias=resultados_poblacionales["incidencias"],
        casos_area_de_estudio=resultados_poblacionales["casos_area_de_estudio"],
        casos_ine=resultados_poblacionales["casos_teoricos_INE"],
        casos_fonasa=resultados_poblacionales["casos_teoricos_FONASA"],
        casos_fonasa_consolidados=resultados_poblacionales["casos_fonasa_consolidados"],
        casos_a_hacerse_cargo_consolidados=resultados_poblacionales["casos_a_hacerse_cargo_INT"],
        columnas_poblacion=COLUMNAS_INCIDENCIA,
    )

    resultados_poblacionales["resumen_total_RDR"] = resumen_total

    guardar_resultados(resultados_poblacionales, RUTA_PLANILLA_OUTPUT)
