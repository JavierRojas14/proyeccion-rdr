import pandas as pd


def obtener_distribucion_consultas(df, agrupacion):
    # Agrupa segun lo especificado por el usuario y separa por pacientes
    agrupacion_por_paciente = agrupacion + ["id_paciente"]

    # Obtiene la cantidad de consultas por paciente
    consultas_por_paciente = (
        df.groupby(agrupacion_por_paciente).size().reset_index(name="n_consultas")
    )

    # Obtiene el desempeno de las consultas
    distribucion_consultas = consultas_por_paciente.groupby(agrupacion)["n_consultas"].describe()

    # Obtiene la cantidad de consultas totales por especialidad
    cantidad_consultas = df.groupby(agrupacion).size()
    cantidad_consultas.name = "cantidad_consultas"

    # Agrega la cantidad de consultas por especialidad
    distribucion_consultas = distribucion_consultas.join(cantidad_consultas)

    # Periodo minimo
    anio_minimo = df["fechacita"].dt.year.min()
    anio_maximo = df["fechacita"].dt.year.max()

    # Agrega indicadores de anio del resumen
    distribucion_consultas.columns = (
        distribucion_consultas.columns + f"_entre_{anio_minimo}_{anio_maximo}"
    )

    return distribucion_consultas, consultas_por_paciente


def leer_y_filtrar_consultas_medicas_y_no_medicas(ruta_track):
    # Lee track
    df_track = pd.read_csv(ruta_track)

    # Formatea el id del paciente
    df_track["id_paciente"] = df_track["id_paciente"].astype(str).str.strip(".0")

    # Formatea la fecha de cita a datetime
    df_track["fechacita"] = pd.to_datetime(df_track["fechacita"], format="%Y-%m-%d")

    # Define las glosas solamente de consultas (filtra procedimientos y miscaleneos) y medicos
    GLOSAS_CONSULTAS = ["Consulta Repetida", "Consulta Nueva"]
    PROFESIONALES_MEDICOS = ["Médico Cirujano", "Médico", "Psiquiatra"]

    # Llena las especialidades sin imputar
    df_track["especialidad_agrupada"] = df_track["especialidad_agrupada"].fillna(
        "Especialidad no ingresada"
    )

    # Convierte las prestaciones a mayuscula
    df_track["prestacion"] = df_track["prestacion"].str.upper().str.strip()

    # Reasigna profesionales
    df_track["tipoprofesional_reasignado"] = df_track["tipoprofesional"].replace(
        {
            "Kinesiólogo (a)": "MEDICINA FÍSICA Y REHABILITACIÓN",
            "Fonoaudiólogo (a)": "FONOAUDIOLOGO",
            "Psicólogo (a)": "PSICOLOGÍA",
            "Asistente Social     ": "TRABAJO SOCIAL",
        }
    )

    # Filtra solamente las consultas validas (Atendidas, Consulta Nueva/Repetida y que
    # tenga Consulta en el nombre)
    consultas_validas = df_track.query(
        "estadocita == 'Atendido' and tipoatencion.isin(@GLOSAS_CONSULTAS)"
    )
    # consultas_validas = consultas_validas.query("prestacion.str.contains('CONSULTA')")

    # Filtra por consultas medicas y profesionales medicas
    mask_medicas = (consultas_validas["tipoprofesional"].isin(PROFESIONALES_MEDICOS)) & (
        consultas_validas["prestacion"].str.contains("CONSULTA")
    )
    mask_no_medicas = ~consultas_validas["tipoprofesional"].isin(PROFESIONALES_MEDICOS)

    consultas_medicas = consultas_validas[mask_medicas]
    consultas_no_medicas = consultas_validas[mask_no_medicas]

    return df_track, consultas_medicas, consultas_no_medicas


def calcular_casos_por_especialidad(df, casos_totales, anios_poblacion):
    """
    Calcula la cantidad total de casos por especialidad y grupo de pacientes.

    Parameters:
    - df: DataFrame inicial con los datos de los casos.
    - casos_totales: DataFrame con el total de casos para unir.
    - anios_poblacion: Lista de columnas de años de población para sumar.

    Returns:
    - DataFrame con el total de casos por especialidad y grupo de pacientes.
    """
    # Separa cada diagnóstico contenido en cada una de las especialidades
    df_desglosado = df.explode("Diagnostico")

    # Define como índice el tipo de paciente y diagnóstico
    df_indexado = df_desglosado.set_index(["tipo_paciente", "Diagnostico"])

    # Une los casos de cada diagnóstico a cada especialidad
    df_unido = df_indexado.join(casos_totales)

    # Suma la totalidad de pacientes por cada grupo de pacientes por especialidad
    df_resultado = df_unido.groupby(["ESTAMENTO/ESPECIALIDAD", "Grupos de Pacientes"])[
        anios_poblacion
    ].sum()

    return df_resultado


def acotar_casos_especialidad_por_porcentaje(casos_completos, anios_poblacion):
    """
    Calcula la cantidad de casos a atender por especialidad después de aplicar el porcentaje de
    pacientes.

    Parameters:
    - df: DataFrame inicial con los datos de los casos.
    - casos_por_especialidad_desglosada: DataFrame desglosado previamente por especialidad y
    diagnóstico.
    - anios_poblacion: Lista de columnas de años de población para multiplicar por el porcentaje.

    Returns:
    - DataFrame con el total de casos a atender por especialidad después de aplicar el porcentaje
    de pacientes.
    """
    # Crea un nuevo DataFrame copiando los casos completos para calcular los casos a atender
    casos_a_hacerse_cargo = casos_completos.copy()

    # Multiplica la cantidad inicial de pacientes por especialidad por el porcentaje de atención
    casos_a_hacerse_cargo[anios_poblacion] = casos_completos[anios_poblacion].mul(
        casos_completos["% de los pacientes a atender"], axis=0
    )

    # Reinicia el índice del grupo de pacientes para contar consultas por especialidad
    casos_a_hacerse_cargo = casos_a_hacerse_cargo.reset_index()

    # Agrupa y suma la cantidad de pacientes por especialidad
    casos_agrupados_por_especialidad = casos_a_hacerse_cargo.groupby(["ESTAMENTO/ESPECIALIDAD"])[
        anios_poblacion
    ].sum()

    return casos_agrupados_por_especialidad


# Función para cargar los datos desde el archivo
def cargar_datos_incidencias(ruta, hoja="consultas_medicas"):
    """Carga datos de incidencias desde un archivo Excel."""
    print(f"Cargando datos desde {ruta}, hoja: {hoja}")
    return pd.read_excel(ruta, sheet_name=hoja)


# Función para separar diagnósticos dentro de una especialidad
def separar_diagnosticos(df):
    """Separa los diagnósticos contenidos en la columna 'Diagnostico'."""
    print("Separando diagnósticos por especialidad...")
    df["Diagnostico"] = df["Diagnostico"].str.split(", ")
    return df.explode("Diagnostico")


# Función para indexar los datos por tipo de paciente y diagnóstico
def indexar_por_tipo_y_diagnostico(df):
    """Establece un índice basado en el tipo de paciente y diagnóstico."""
    print("Indexando por tipo de paciente y diagnóstico...")
    return df.set_index(["tipo_paciente", "Diagnostico"])


# Función para unir casos con información de macroprocesos
def unir_casos_con_macroprocesos(df, casos_macroprocesos):
    """Une los casos de cada diagnóstico a los datos de macroprocesos."""
    print("Uniendo casos por diagnóstico con macroprocesos...")
    return df.join(casos_macroprocesos)


def identificar_duplicados(df: pd.DataFrame, claves: list) -> pd.DataFrame:
    """
    Filtra las filas que tienen valores duplicados en las columnas clave.

    :param df: DataFrame de entrada.
    :param claves: Columnas utilizadas para identificar duplicados.
    :return: DataFrame con las filas duplicadas.
    """
    print("Identificando casos duplicados...")
    casos_duplicados = df[df.duplicated(subset=claves, keep=False)]

    # Identifica los diags ingresados con duplicados
    especialidades_con_diags_duplicados = casos_duplicados.groupby(["ESTAMENTO/ESPECIALIDAD"])[
        "Diagnostico"
    ].unique()

    print("Especialidades con diagnósticos duplicados: \n")
    print(especialidades_con_diags_duplicados)
    print()

    return casos_duplicados


def eliminar_casos_duplicados(
    df: pd.DataFrame, claves: list, columnas_valores: list
) -> pd.DataFrame:
    """
    Ajusta los valores de los casos duplicados restando el último valor del grupo al resto.

    :param df: DataFrame con los casos duplicados.
    :param claves: Columnas utilizadas para agrupar.
    :param columnas_valores: Columnas numéricas a ajustar.
    :return: DataFrame con los valores ajustados.
    """
    df_ajustado = (
        df.groupby(claves)[columnas_valores]
        .apply(lambda x: x - x.iloc[-1])
        .groupby(claves)
        .head(1)
        .reset_index()
        .set_index("level_3")
    )
    return df_ajustado


def corregir_casos_duplicados_por_especialidad(
    df: pd.DataFrame, claves: list, columnas_valores: list, columna_orden: str
) -> pd.DataFrame:
    """
    Ordena, identifica y ajusta casos duplicados en un DataFrame y en un grupo.

    :param df: DataFrame de entrada.
    :param claves: Columnas utilizadas para agrupar.
    :param columnas_valores: Columnas numéricas a ajustar.
    :param columna_orden: Columna utilizada para ordenar de mayor a menor.
    :return: DataFrame procesado sin superposición de casos.
    """
    df_ordenado = df.reset_index().sort_values(
        claves + [columna_orden], ascending=[True] * len(claves) + [False]
    )
    df_duplicados = identificar_duplicados(df_ordenado, claves)
    df_sin_superposicion = eliminar_casos_duplicados(df_duplicados, claves, columnas_valores)

    df_ordenado.loc[df_sin_superposicion.index, columnas_valores] = df_sin_superposicion
    return df_ordenado


# Función para sumar pacientes por grupo y especialidad
def sumar_pacientes_por_grupo(df, agrupacion, columnas_suma):
    """Agrupa y suma los pacientes por especialidad y grupos de pacientes."""
    print("Sumando pacientes por grupo y especialidad...")
    return df.groupby(agrupacion)[columnas_suma].sum()


# Función para crear un DataFrame con los casos ajustados
def calcular_casos_a_hacerse_cargo(df, porcentaje_col, columnas_suma):
    """Calcula los casos ajustados por porcentaje."""
    print("Calculando casos a hacerse cargo...")
    df_copia = df.copy()
    df_copia[columnas_suma] = df[columnas_suma].mul(df[porcentaje_col], axis=0)
    return df_copia.reset_index()


# Flujo principal
def procesar_incidencias(
    ruta_incidencias, tipo_consulta, casos_macroproceso_por_region, anios_poblacion
):
    """Ejecuta todo el proceso de análisis de incidencias."""
    print("Iniciando proceso...")

    # Cargar datos
    df_incidencias = cargar_datos_incidencias(ruta_incidencias, tipo_consulta)

    # Separar diagnósticos
    casos_por_especialidad_long = separar_diagnosticos(df_incidencias)

    # Obtener diagnósticos únicos
    diagnosticos_ingresados = set(casos_por_especialidad_long["Diagnostico"].unique())
    print(f"Diagnósticos únicos ingresados: {len(diagnosticos_ingresados)}")

    # Indexar por tipo de paciente y diagnóstico
    casos_por_especialidad_long = indexar_por_tipo_y_diagnostico(casos_por_especialidad_long)

    # Unir con macroprocesos
    casos_por_especialidad_long = unir_casos_con_macroprocesos(
        casos_por_especialidad_long, casos_macroproceso_por_region
    )

    # Corrige (elimina) los casos duplicados de una especialidad
    agrupacion_para_sacar_duplicados = ["ESTAMENTO/ESPECIALIDAD", "Diagnostico", "Estrato"]
    casos_por_especialidad_long_sin_duplicados = corregir_casos_duplicados_por_especialidad(
        casos_por_especialidad_long, agrupacion_para_sacar_duplicados, anios_poblacion, "2035"
    )

    # Obtiene los casos totales de macroproceso previo a obtener los hacerse cargo
    agrupacion = ["ESTAMENTO/ESPECIALIDAD", "Grupos de Pacientes", "es_presencial"]
    casos_totales_por_especialidad_y_grupo = sumar_pacientes_por_grupo(
        casos_por_especialidad_long_sin_duplicados, agrupacion, anios_poblacion
    )

    # Casos a hacerse cargo long
    casos_a_hacerse_cargo_long = calcular_casos_a_hacerse_cargo(
        casos_por_especialidad_long_sin_duplicados, "% de los pacientes a atender", anios_poblacion
    )

    # Obtiene casos totales por especialidad, grupo de pacientes y por presencialidad
    agrupacion = ["ESTAMENTO/ESPECIALIDAD", "Grupos de Pacientes", "es_presencial"]
    casos_a_hacerse_cargo_por_especialidad_grupo_y_presencial = sumar_pacientes_por_grupo(
        casos_a_hacerse_cargo_long, agrupacion, anios_poblacion
    )

    # Obtiene los casos a hacerse cargo consolidados
    agrupacion = ["ESTAMENTO/ESPECIALIDAD"]
    casos_a_hacerse_cargo_consolidados = sumar_pacientes_por_grupo(
        casos_a_hacerse_cargo_long, agrupacion, anios_poblacion
    )

    print("Proceso completado.")
    return (
        df_incidencias,
        diagnosticos_ingresados,
        casos_por_especialidad_long,
        casos_totales_por_especialidad_y_grupo,
        casos_a_hacerse_cargo_long,
        casos_a_hacerse_cargo_por_especialidad_grupo_y_presencial,
        casos_a_hacerse_cargo_consolidados,
    )


def expandir_serie_rendimientos(datos_consultas, grupos_pacientes, valores_presencial):
    """
    Crea una serie de consultas médicas adaptada a diferentes grupos de pacientes y modalidad
    presencial/telemedicina.

    Args:
        datos_consultas (pd.Series): Serie con la cantidad inicial de consultas por
        estamento/especialidad.
        grupos_pacientes (list): Lista de identificadores de grupos de pacientes.
        valores_presencial (list): Lista de valores indicando si es presencial o no
        (e.g., ["Si", "No"]).

    Returns:
        pd.Series: Serie expandida con un MultiIndex que incluye estamento/especialidad, grupos
        de pacientes y modalidad.
    """
    # Crear un nuevo índice con un MultiIndex
    nuevo_indice = pd.MultiIndex.from_product(
        [datos_consultas.index, grupos_pacientes, valores_presencial],
        names=["ESTAMENTO/ESPECIALIDAD", "Grupos de Pacientes", "es_presencial"],
    )

    # Expandir los valores de la serie para ajustarse al nuevo índice
    consultas_expandidas = pd.Series(
        datos_consultas.values.repeat(len(grupos_pacientes) * len(valores_presencial)),
        index=nuevo_indice,
    )

    return consultas_expandidas
