import pandas as pd
import polars as pl
import xlsxwriter.utility as xl_util


def leer_cae(ruta):
    df = pd.read_csv(ruta, dtype={"id_paciente": str})
    return df


def leer_grd(ruta):
    df = pd.read_csv(ruta, dtype={"id_paciente": str})

    # Formatea las fechas de ingreso y egreso
    df["fecha_ingreso"] = pd.to_datetime(df["fecha_ingreso"], format="%Y-%m-%d")
    df["fecha_egreso"] = pd.to_datetime(df["fecha_egreso"], format="%Y-%m-%d")

    # Obtiene la estancia
    df["estancia"] = (df["fecha_egreso"] - df["fecha_ingreso"]).dt.days

    # Corrige estancias de 0 dias
    mask_estancia_cero = df["estancia"] == 0
    df.loc[mask_estancia_cero, "estancia"] += 1

    # Renombra columnas
    df = df.rename(columns={"fecha_egreso_ano": "ano"})

    # Obtiene solamente los egresos hosp. Notar que la base ya llegaba solo con egresos hosp
    df = df.query("tipo_actividad == 1").copy()

    return df


def leer_laboratorio(ruta):
    # Lee la base de datos de laboratorio
    df = pl.read_csv(ruta, dtypes={"n": pl.Float64, "id_paciente": str}).to_pandas()

    # Cambia los examenes del tipo externos a ambulatorios
    df_hosp = df.query("tipo_examen == 'AC'")
    df_amb = df.query("tipo_examen == 'AA'")

    return df, df_hosp, df_amb


def leer_farmacia(ruta):
    # Lee la base de datos de farmacia
    df = pl.read_csv(ruta, dtypes={"id_paciente": str}).to_pandas()

    # Cambia los examenes del tipo externos a ambulatorios
    df_hosp = df.query("tipopac_2 == 'HOSP'")
    df_amb = df.query("tipopac_2 == 'AMB'")

    return df, df_hosp, df_amb


def leer_imagenologia(ruta):
    # Lee la base de datos de imagenologia
    df = pl.read_csv(ruta, dtypes={"id_paciente": str}).to_pandas()

    # Renombra las columnas
    df = df.rename(columns={"nombre_prestacion": "glosa_examen"})

    # Separa en paciente hosp, ambulatorio y urgencia
    df_hosp = df.query("tipo_paciente == 'HOSPITALIZADO'")
    df_amb = df.query("tipo_paciente == 'AMBULATORIO'")
    df_urg = df.query("tipo_paciente == 'URGENCIA'")

    return df, df_hosp, df_amb, df_urg


def unir_atenciones_con_examenes(atenciones, examenes):
    """
    Une los datos de exámenes con los datos de días de estancia por paciente. Deja las atenciones
    como el total que se debe dejar
    """
    return atenciones.to_frame().join(examenes, how="left")


def calcular_ratio_examenes(df, columna_examenes, columna_atenciones):
    """
    Calcula la proporción de exámenes por día de estancia.
    """
    df["ratio_examenes"] = df[columna_examenes] / df[columna_atenciones]
    return df


def describir_por_ano(df):
    """
    Agrupa los datos por año y genera estadísticas descriptivas del ratio de exámenes.
    """
    return df.groupby("ano")["ratio_examenes"].describe()


def calcular_examenes_por_atencion(
    atenciones_totales_por_paciente,
    examenes_por_paciente,
    columna_atenciones,
    columna_examenes,
):
    """
    Calcula el desempeño de algun servicio, incluyendo estadísticas descriptivas
    del ratio de exámenes (incluso prescripciones) por algun tipo de atencion (DCO o consultas).
    """
    # Paso 1: Unir las atenciones totales con los pacientes con examenes
    datos_unidos = unir_atenciones_con_examenes(
        atenciones_totales_por_paciente, examenes_por_paciente
    )

    # Paso 2: Rellena pacientes que nunca se han visto en el servicio
    datos_unidos = datos_unidos.fillna(0)

    # Paso 3: Calcular ratio de exámenes por día de estancia
    datos_ratio = calcular_ratio_examenes(datos_unidos, columna_examenes, columna_atenciones)

    # Paso 4: Generar estadísticas descriptivas por año
    resumen = describir_por_ano(datos_ratio)

    # Paso 5: Obtiene la cantidad de pacientes distintos en la base de examenes
    pacientes_unicos_examenes = (
        examenes_por_paciente.reset_index().groupby("ano")["id_paciente"].nunique()
    )

    # Paso 6: Une los pacientes con examenes al resumen
    resumen = resumen.join(pacientes_unicos_examenes)
    resumen = resumen[["count", "id_paciente", "mean", "std", "min", "25%", "50%", "75%", "max"]]

    # Paso 7: Elimina los anios sin registro
    resumen = resumen.dropna()

    return datos_ratio, resumen


def guardar_dict_en_excel(
    nombre_archivo,
    datos_por_hoja,
    espacio_filas=2,
    espacio_columnas=1,
    estilo_tabla="Table Style Medium 4",
):
    """
    Guarda un diccionario de diccionarios de DataFrames en un archivo Excel como tablas formateadas
    con títulos.

    - Si hay 6 elementos en el diccionario interno:
      - Los primeros 3 elementos se guardan en formato horizontal con un espacio de columnas.
      - Los últimos 3 elementos se guardan en formato vertical con un espacio de filas.
    - Si hay solo 1 elemento, se guarda directamente en la hoja con su título y formato de tabla.
    - Se aplica formato de tabla de Excel usando XlsxWriter.
    """
    with pd.ExcelWriter(nombre_archivo, engine="xlsxwriter") as writer:
        workbook = writer.book  # Acceder al libro de Excel

        for nombre_hoja, tablas in datos_por_hoja.items():
            if not isinstance(tablas, dict) or not all(
                isinstance(df, (pd.DataFrame, pd.Series)) for df in tablas.values()
            ):
                raise ValueError(
                    "Cada entrada debe ser un diccionario con nombres y DataFrames o Series."
                )

            worksheet = workbook.add_worksheet(nombre_hoja)
            writer.sheets[nombre_hoja] = worksheet

            lista_items = list(tablas.items())

            if len(lista_items) == 1:
                # Caso de un solo DataFrame
                nombre_tabla, df = lista_items[0]
                formato_cursiva = workbook.add_format({"italic": True})
                worksheet.write(0, 0, nombre_tabla, formato_cursiva)  # Insertar título en cursiva
                df.to_excel(writer, sheet_name=nombre_hoja, startrow=1, startcol=0, index=False)
                filas, columnas = df.shape
                if filas > 0:
                    rango = xl_util.xl_range(
                        1, 0, 1 + filas, columnas - 1
                    )  # Ajuste para incluir encabezado y datos
                    worksheet.add_table(
                        rango,
                        {
                            "columns": [{"header": col} for col in df.columns],
                            "style": estilo_tabla,
                        },
                    )

            elif len(lista_items) == 6:
                # Caso de 6 elementos
                fila_inicio, columna_inicio = 0, 0

                formato_cursiva = workbook.add_format({"italic": True})

                # Guardar los primeros 3 elementos en formato horizontal
                for i, (nombre_tabla, df) in enumerate(lista_items[:3]):
                    worksheet.write(
                        fila_inicio, columna_inicio, nombre_tabla, formato_cursiva
                    )  # Insertar título en cursiva
                    df.to_excel(
                        writer,
                        sheet_name=nombre_hoja,
                        startrow=fila_inicio + 1,
                        startcol=columna_inicio,
                        index=False,
                    )
                    filas, columnas = df.shape
                    if filas > 0:
                        rango = xl_util.xl_range(
                            fila_inicio + 1,
                            columna_inicio,
                            fila_inicio + 1 + filas,
                            columna_inicio + columnas - 1,
                        )
                        worksheet.add_table(
                            rango,
                            {
                                "columns": [{"header": col} for col in df.columns],
                                "style": estilo_tabla,
                            },
                        )
                    columna_inicio += columnas + espacio_columnas

                # Ajustar para los elementos verticales
                fila_inicio = (
                    max(
                        len(lista_items[i][1]) if isinstance(lista_items[i][1], pd.DataFrame) else 1
                        for i in range(3)
                    )
                    + espacio_filas
                )
                columna_inicio = 0

                # Guardar los últimos 3 elementos en formato vertical
                for nombre_tabla, df in lista_items[3:]:
                    worksheet.write(
                        fila_inicio, columna_inicio, nombre_tabla, formato_cursiva
                    )  # Insertar título en cursiva
                    df.to_excel(
                        writer,
                        sheet_name=nombre_hoja,
                        startrow=fila_inicio + 1,
                        startcol=columna_inicio,
                        index=False,
                    )
                    filas, columnas = df.shape
                    if filas > 0:
                        rango = xl_util.xl_range(
                            fila_inicio + 1,
                            columna_inicio,
                            fila_inicio + 1 + filas,
                            columna_inicio + columnas - 1,
                        )
                        worksheet.add_table(
                            rango,
                            {
                                "columns": [{"header": col} for col in df.columns],
                                "style": estilo_tabla,
                            },
                        )
                    fila_inicio += filas + espacio_filas

            else:
                raise ValueError("Cada diccionario debe contener exactamente 1 o 6 elementos.")


#### Calculo de Unidades de Apoyo por cada examen ####


def estimar_unidad_completa(
    df_unidad,
    df_unidad_hosp,
    df_unidad_amb,
    df_grd,
    df_consultas,
    casos_hospitalizados_proyectados,
    casos_atencion_abierta_proyectados,
    anios_poblacion,
    estimar_examenes_por_paciente_utilizando_glosa,
    modificaciones_a_metricas_para_proyectar=None,
    valor_estadistico_a_ocupar="75%",
):
    # 1. Obtiene todas las metricas para estimar la unidad
    (
        porcentajes_examenes,
        examenes_por_paciente_hosp,
        examenes_por_paciente_amb,
        metricas_para_proyectar,
    ) = obtener_metricas_para_proyectar_unidad(
        df_unidad,
        df_unidad_hosp,
        df_unidad_amb,
        df_grd,
        df_consultas,
        estimar_examenes_por_paciente_utilizando_glosa,
        modificaciones_a_metricas_para_proyectar,
        valor_estadistico_a_ocupar,
    )

    # 2. Proyecta la cantidad de examenes con los pacientes hosp y amb
    proyeccion_por_examen = proyectar_examenes_unidad(
        metricas_para_proyectar,
        casos_hospitalizados_proyectados,
        casos_atencion_abierta_proyectados,
    )

    # 3. Obtiene la cantidad total de examenes proyectados
    proyeccion_total_unidad = pd.DataFrame(
        [proyeccion_por_examen[anios_poblacion].sum()], index=["TOTAL"]
    )
    proyeccion_total_por_procedencia = proyeccion_por_examen.groupby("procedencia")[
        anios_poblacion
    ].sum()

    proyeccion_total_unidad = pd.concat([proyeccion_total_por_procedencia, proyeccion_total_unidad])

    # 4. Obtiene la produccion actual de la unidad
    produccion_actual_unidad = calcular_produccion_actual_unidad(
        df_unidad, estimar_examenes_por_paciente_utilizando_glosa
    )

    # 5. Obtiene el crecimiento de la unidad respecto a la produccion actual
    crecimiento_unidad = (
        proyeccion_total_unidad.loc["TOTAL", "2035"] / produccion_actual_unidad.mean()
    )

    # 6. Imprime los resultados
    imprimir_resultados_estimacion_unidad(
        porcentajes_examenes,
        examenes_por_paciente_hosp,
        examenes_por_paciente_amb,
        metricas_para_proyectar,
        proyeccion_total_unidad,
        produccion_actual_unidad,
        crecimiento_unidad,
    )

    return {
        "Porcentaje de Realización de Exámenes Históricos": porcentajes_examenes.reset_index(),
        "Exámenes por Paciente - AC": examenes_por_paciente_hosp.reset_index(),
        "Exámenes por Paciente - AA": examenes_por_paciente_amb.reset_index(),
        "Métricas seleccionadas para proyectar": metricas_para_proyectar.reset_index(),
        "Proyección de Exámenes": proyeccion_por_examen.reset_index(),
        "Proyección de Exámenes Totales": proyeccion_total_unidad.reset_index(),
        "Producción Total de Unidad": produccion_actual_unidad.reset_index(),
        "Crecimiento de la Unidad": crecimiento_unidad,
    }


def obtener_metricas_para_proyectar_unidad(
    df_unidad,
    df_unidad_hosp,
    df_unidad_amb,
    df_grd,
    df_consultas,
    estimar_examenes_por_paciente_utilizando_glosa=True,
    modificaciones_a_metricas_para_proyectar=None,
    valor_estadistico_a_ocupar="75%",
):
    # 1. Obtiene el porcentaje de realizacion de cada uno de los examenes de la unidad
    porcentajes_examenes = calcular_porcentaje_examenes(
        df_unidad, df_unidad_hosp, df_unidad_amb, df_grd, df_consultas
    )

    # 2. Obtiene cuantos examenes se realiza cada paciente por examen de la unidad
    if estimar_examenes_por_paciente_utilizando_glosa:
        examenes_por_paciente_hosp = calcular_examenes_por_pacientes_por_glosa(df_unidad_hosp)
        examenes_por_paciente_amb = calcular_examenes_por_pacientes_por_glosa(df_unidad_amb)

    else:
        examenes_por_paciente_hosp = calcular_examenes_por_pacientes_por_cantidad(df_unidad_hosp)
        examenes_por_paciente_amb = calcular_examenes_por_pacientes_por_cantidad(df_unidad_amb)

    # 3. Selecciona las metricas para proyectar cada uno de los examenes
    metricas_para_proyectar_unidad = seleccionar_metricas_unidad(
        porcentajes_examenes,
        examenes_por_paciente_hosp,
        examenes_por_paciente_amb,
        valor_estadistico_a_ocupar,
    )

    # 4. Modifica los examenes que se indiquen
    if modificaciones_a_metricas_para_proyectar is not None:
        metricas_para_proyectar_unidad = modificar_metricas_para_proyectar(
            metricas_para_proyectar_unidad, modificaciones_a_metricas_para_proyectar
        )

    return (
        porcentajes_examenes,
        examenes_por_paciente_hosp,
        examenes_por_paciente_amb,
        metricas_para_proyectar_unidad,
    )


def calcular_porcentaje_examenes(
    df_unidad,
    df_unidad_hosp,
    df_unidad_amb,
    df_grd,
    consultas_medicas,
):
    """
    Calcula el porcentaje de pacientes que se realizan cada examen, separando por hospitalizados y
    ambulatorios.

    Parámetros:
    - df_unidad: DataFrame con los exámenes.
    - df_unidad_hosp: DataFrame con exámenes en hospitalizados.
    - df_unidad_amb: DataFrame con exámenes en ambulatorios.
    - df_grd: DataFrame de pacientes hospitalizados.
    - consultas_medicas: DataFrame de consultas médicas ambulatorias.

    Retorna:
    - Una lista con los DataFrames de porcentajes de exámenes.
    """
    resultados_examenes = []

    # Itera en todos los tipos de examenes de la unidad
    for tipo_examen in df_unidad["glosa_examen"].unique():
        porcentaje_examen_hosp, porcentaje_examen_amb = obtener_porcentajes_de_un_examen(
            tipo_examen,
            df_unidad_hosp,
            df_unidad_amb,
            df_grd,
            consultas_medicas,
        )
        resultados_examenes.append(porcentaje_examen_hosp)
        resultados_examenes.append(porcentaje_examen_amb)

    # Convierte el resumen de examenes a formato necesario
    resultados_examenes = pd.concat(resultados_examenes)
    resultados_examenes = pd.pivot_table(
        resultados_examenes,
        values="porcentaje_df2_en_df1",
        columns="procedencia",
        index=["glosa_examen", "ano"],
    )

    return resultados_examenes


def obtener_porcentajes_de_un_examen(
    tipo_examen, df_unidad_hosp, df_unidad_amb, df_grd, consultas_medicas
):
    """
    Procesa un tipo de examen específico, calculando el porcentaje de pacientes en hospitalizados y
    ambulatorios.

    Parámetros:
    - tipo_examen: Nombre del examen.
    - df_unidad_hosp: DataFrame con exámenes en hospitalizados.
    - df_unidad_amb: DataFrame con exámenes en ambulatorios.
    - df_grd: DataFrame de pacientes hospitalizados.
    - consultas_medicas: DataFrame de consultas médicas ambulatorias.
    - ua: Módulo con funciones de análisis.

    Retorna:
    - Lista con los DataFrames de porcentajes de exámenes en hospitalizados y ambulatorios.
    """
    df_examen_hosp = df_unidad_hosp.query("glosa_examen == @tipo_examen")
    df_examen_amb = df_unidad_amb.query("glosa_examen == @tipo_examen")

    porcentaje_hosp = comparar_pacientes(df_grd, df_examen_hosp)
    porcentaje_amb = comparar_pacientes(consultas_medicas, df_examen_amb)

    porcentaje_hosp["glosa_examen"] = tipo_examen
    porcentaje_hosp["procedencia"] = "HOSPITALIZADO"

    porcentaje_amb["glosa_examen"] = tipo_examen
    porcentaje_amb["procedencia"] = "AMBULATORIO"

    return [porcentaje_hosp, porcentaje_amb]


def calcular_examenes_por_pacientes_por_glosa(df):
    """Calcula los examenes por paciente para las prestaciones de una unidad. En este caso,
    se cuentan las glosas de examenes como 1 cantidad de examen"""
    desempeno_examenes_por_paciente = (
        df.groupby(["ano", "glosa_examen", "id_paciente"])["glosa_examen"]
        .count()
        .reset_index(name="conteo")
        .groupby(["glosa_examen", "ano"])["conteo"]
        .describe()
    )

    return desempeno_examenes_por_paciente


def calcular_examenes_por_pacientes_por_cantidad(df):
    """Calcula los examenes por paciente para las prestaciones de una unidad"""
    desempeno_examenes_por_paciente = (
        df.groupby(["ano", "glosa_examen", "id_paciente"])["cantidad"]
        .sum()
        .reset_index(name="conteo")
        .groupby(["glosa_examen", "ano"])["conteo"]
        .describe()
    )

    return desempeno_examenes_por_paciente


def seleccionar_metricas_unidad(
    porcentajes_unidad,
    desempeno_unidad_hosp,
    desempeno_unidad_amb,
    valor_estadistico_a_ocupar="75%",
    seleccion_valor_en_anios="max",
):
    """
    Calcula las métricas clave para proyectar la prestación de exámenes de una unidad con examenes.

    Parámetros:
    - porcentajes_unidad: DataFrame con los porcentajes de exámenes por tipo.
    - desempeno_unidad_hosp: DataFrame con el rendimiento de exámenes hospitalarios.
    - desempeno_unidad_amb: DataFrame con el rendimiento de exámenes ambulatorios.

    Retorna:
    - DataFrame consolidado con métricas de porcentaje y cantidad de exámenes por paciente.
    """
    # Selecciona que porcentajes ocupar por examen
    porcentajes_a_utilizar = porcentajes_unidad.groupby(["glosa_examen"]).agg(
        porcentaje_a_realizar_hosp=("HOSPITALIZADO", seleccion_valor_en_anios),
        porcentaje_a_realizar_amb=("AMBULATORIO", seleccion_valor_en_anios),
    )

    # Selecciona cuantos examenes a realizar por paciente hospitalizado
    cantidad_examenes_hosp = desempeno_unidad_hosp.groupby(["glosa_examen"]).agg(
        examenes_por_paciente_hosp=(valor_estadistico_a_ocupar, seleccion_valor_en_anios)
    )

    # Selecciona cuantos examenes a realizar por paciente ambulatorio
    cantidad_examenes_amb = desempeno_unidad_amb.groupby(["glosa_examen"]).agg(
        examenes_por_paciente_amb=(valor_estadistico_a_ocupar, seleccion_valor_en_anios)
    )

    # Consolida que porcentaje de pacientes necesitara el examen y cuantos examenes a realizar
    metricas_unidad = pd.concat(
        [
            porcentajes_a_utilizar,
            cantidad_examenes_hosp,
            cantidad_examenes_amb,
        ],
        axis=1,
    )

    # Agrega la columna de observaciones
    metricas_unidad["modificaciones_a_metricas"] = ""

    return metricas_unidad


def modificar_metricas_para_proyectar(
    metricas_para_proyectar_unidad, modificaciones_a_metricas_para_proyectar
):
    for parametros_a_cambiar in modificaciones_a_metricas_para_proyectar:
        # Extrae cada nueva metrica a asignar
        examen_a_cambiar = parametros_a_cambiar["glosa_examen"]
        porcentaje_hospitalizado_nuevo = parametros_a_cambiar["porcentaje_a_realizar_hosp"]
        porcentaje_amb_nuevo = parametros_a_cambiar["porcentaje_a_realizar_amb"]
        examenes_por_paciente_hosp_nuevo = parametros_a_cambiar["examenes_por_paciente_hosp"]
        examenes_por_paciente_amb_nuevo = parametros_a_cambiar["examenes_por_paciente_amb"]
        modificaciones_a_metricas = parametros_a_cambiar["modificaciones_a_metricas"]

        # Modifica las metricas
        metricas_para_proyectar_unidad.loc[examen_a_cambiar, "porcentaje_a_realizar_hosp"] = (
            porcentaje_hospitalizado_nuevo
        )
        metricas_para_proyectar_unidad.loc[examen_a_cambiar, "porcentaje_a_realizar_amb"] = (
            porcentaje_amb_nuevo
        )
        metricas_para_proyectar_unidad.loc[examen_a_cambiar, "examenes_por_paciente_hosp"] = (
            examenes_por_paciente_hosp_nuevo
        )
        metricas_para_proyectar_unidad.loc[examen_a_cambiar, "examenes_por_paciente_amb"] = (
            examenes_por_paciente_amb_nuevo
        )

        # Agrega la observacion de la modificacion
        metricas_para_proyectar_unidad.loc[examen_a_cambiar, "modificaciones_a_metricas"] = (
            modificaciones_a_metricas
        )

        print(f"Modificando metricas para {examen_a_cambiar}")

    return metricas_para_proyectar_unidad


def proyectar_examenes_unidad(
    metricas_para_proyectar,
    casos_hospitalizados_proyectados,
    casos_ambulatorios_proyectados,
):
    """
    Proyecta la cantidad de exámenes de una unidad según los casos y métricas proporcionadas.
    """
    proyeccion_unidad = []

    for (
        nombre_examen,
        porcentaje_hosp,
        porcentaje_amb,
        examenes_por_paciente_hosp,
        examenes_por_paciente_amb,
        _,
    ) in metricas_para_proyectar.itertuples():
        # Calcula los examenes a realizar en hospitalizados
        examenes_proyectados_hosp = calcular_examenes_proyectados(
            casos_hospitalizados_proyectados,
            porcentaje_hosp,
            examenes_por_paciente_hosp,
            nombre_examen,
            "HOSPITALIZADO",
        )

        # Calcula los examenes a realizar en ambulatorio
        examenes_proyectados_amb = calcular_examenes_proyectados(
            casos_ambulatorios_proyectados,
            porcentaje_amb,
            examenes_por_paciente_amb,
            nombre_examen,
            "AMBULATORIO",
        )

        # Agrega los resultados
        proyeccion_unidad.append(examenes_proyectados_hosp)
        proyeccion_unidad.append(examenes_proyectados_amb)

    # Consolida los resultados de todos los examenes
    proyeccion_unidad = pd.concat(proyeccion_unidad)
    proyeccion_unidad = pd.pivot_table(
        proyeccion_unidad, columns="index", values=0, index=["procedencia", "tipo_examen"]
    )

    return proyeccion_unidad


def calcular_examenes_proyectados(
    casos_proyectados, porcentaje, examenes_por_paciente, nombre_examen, procedencia
):
    """
    Calcula la cantidad de exámenes proyectados y agrega información relevante.
    """
    examenes_proyectados = (
        (casos_proyectados * porcentaje * examenes_por_paciente).to_frame().reset_index()
    )
    examenes_proyectados["tipo_examen"] = nombre_examen
    examenes_proyectados["procedencia"] = procedencia
    return examenes_proyectados


def calcular_produccion_actual_unidad(df, estimar_examenes_por_paciente_utilizando_glosa=True):
    """Calcula la produccion total de una unidad"""
    if estimar_examenes_por_paciente_utilizando_glosa:
        return df.groupby("ano")["glosa_examen"].count()

    else:
        return df.groupby("ano")["cantidad"].sum()


def imprimir_resultados_estimacion_unidad(
    porcentajes_examenes,
    examenes_por_paciente_hosp,
    examenes_por_paciente_amb,
    metricas_para_proyectar,
    proyeccion_total_unidad,
    produccion_actual_unidad,
    crecimiento_unidad,
):
    # # # Muestra los porcentajes de realizacion de examenes
    # display(porcentajes_examenes.head(10).style.format(lambda x: "-" if pd.isna(x) else f"{x:.2%}"))

    # # Muestra cuantos examenes se realiza cada paciente
    # display(examenes_por_paciente_hosp)
    # display(examenes_por_paciente_amb)

    # Muestra el consolidado de porcentajes y examenes por paciente
    print("Metricas utilizadas para proyectar")
    display(
        metricas_para_proyectar.style.format(
            {
                "porcentaje_a_realizar_hosp": lambda x: "-" if pd.isna(x) else f"{x:.1%}",
                "porcentaje_a_realizar_amb": lambda x: "-" if pd.isna(x) else f"{x:.1%}",
                "examenes_por_paciente_hosp": lambda x: "-" if pd.isna(x) else f"{x:.2f}",
                "examenes_por_paciente_amb": lambda x: "-" if pd.isna(x) else f"{x:.2f}",
            }
        )
    )

    # Muestra la proyeccion
    print("Proyeccion Unidad")
    display(proyeccion_total_unidad)

    # Muestra la produccion actual
    print("Producción Actual Unidad")
    display(produccion_actual_unidad)

    # Muestra el crecimiento de la unidad
    print(f"Aumento de Unidad: {crecimiento_unidad:.2f} veces")
