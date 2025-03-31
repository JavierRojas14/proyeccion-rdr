import pandas as pd
from tabulate import tabulate


def calcular_tiempo_utilizado_pabellon(casos_quirurgicos, duraciones_int_q):
    """
    Calcula el tiempo utilizado en pabellón multiplicando casos quirúrgicos por duraciones.

    Args:
    casos_quirurgicos (DataFrame): Casos quirúrgicos calculados.
    duraciones_int_q (Series): Duraciones promedio de intervenciones quirúrgicas.

    Returns:
    DataFrame: Tiempo utilizado en pabellón en horas.
    """
    tiempo_utilizado_pabellon = casos_quirurgicos.mul(duraciones_int_q, axis=0)
    tiempo_utilizado_pabellon_horas = tiempo_utilizado_pabellon.apply(
        lambda x: x.dt.total_seconds() / 3600
    )
    print("Tiempo utilizado en pabellón calculado (en horas):")
    print(tabulate(tiempo_utilizado_pabellon_horas.head(), headers="keys", tablefmt="pretty"))
    print()
    return tiempo_utilizado_pabellon_horas


def calcular_cantidad_de_pabellones_necesarios(tiempo_utilizado_pabellon_horas, horas_laborales):
    """
    Calcula la cantidad de pabellones necesarios.

    Args:
    tiempo_utilizado_pabellon_horas (DataFrame): Tiempo utilizado en pabellón en horas.
    horas_laborales (Series): Horas laborales por año.

    Returns:
    DataFrame: Cantidad de pabellones necesarios por año.
    """
    cantidad_de_pabellones_necesarios = tiempo_utilizado_pabellon_horas.div(horas_laborales, axis=1)
    print("Cantidad de pabellones necesarios calculada:")
    print(tabulate(cantidad_de_pabellones_necesarios.head(), headers="keys", tablefmt="pretty"))
    print()
    return cantidad_de_pabellones_necesarios


def obtener_resumen_ocurrencia_complicacion(df, df_filtrada):
    # Obtiene el resumen de la cantidad de ocurrencias del DataFrame total y el filtrado
    resumen = pd.DataFrame(
        {
            "totales": df.groupby(["ano_de_intervencion", "especialidad"]).size(),
            "ocurrencia_filtrado": df_filtrada.groupby(
                ["ano_de_intervencion", "especialidad"]
            ).size(),
        }
    )

    # Obtiene el resumen acumulado en el periodo
    resumen_acumulado = resumen.sum()

    # Obtiene los porcentajes de ocurrencia desglosados y acumulados
    resumen["fraccion"] = resumen["ocurrencia_filtrado"] / resumen["totales"]
    porcentaje_acumulado = resumen_acumulado["ocurrencia_filtrado"] / resumen_acumulado["totales"]

    # Obtiene el resumen por especialidad acumulado
    resumen_acumulado_por_especialidad = (
        resumen.reset_index().groupby("especialidad")[["totales", "ocurrencia_filtrado"]].sum()
    )
    resumen_acumulado_por_especialidad["fraccion"] = (
        resumen_acumulado_por_especialidad["ocurrencia_filtrado"]
        / resumen_acumulado_por_especialidad["totales"]
    )

    return resumen, resumen_acumulado, resumen_acumulado_por_especialidad, porcentaje_acumulado


def buscar_nombre_operacion_pabellon(df, operaciones):
    # Filtra la base de datos segun el nombre de la operacion
    return df[df["nombre_de_la_operacion"].fillna("").str.contains(operaciones, regex=True)]


def buscar_nombre_diagnosticos_pabellon(df, diagnosticos):
    # Filtra la base de datos segun el nombre del diagnostico 1 y 2
    return df[
        (df["primer_diagnostico"].fillna("").str.contains(diagnosticos, regex=True))
        | (df["segundo_diagnostico"].fillna("").str.contains(diagnosticos, regex=True))
    ]


def iterar_en_complicaciones_a_buscar(df, dict_textos_a_buscar, tipo_complicacion):
    # Decide que parametro a buscar en la base de datos
    busqueda_a_realizar = {
        "intervencion_quirurgica": buscar_nombre_operacion_pabellon,
        "diagnostico": buscar_nombre_diagnosticos_pabellon,
    }
    funcion_a_ocupar_para_buscar = busqueda_a_realizar[tipo_complicacion]

    # Itera por el diccionario de busqueda y guarda los resultados
    df_resultado = pd.DataFrame()
    for nombre_complicacion, textos_a_buscar in dict_textos_a_buscar.items():
        df_filtrada = funcion_a_ocupar_para_buscar(df, textos_a_buscar)
        resumen_filtrado = obtener_resumen_ocurrencia_complicacion(df, df_filtrada)
        tiempo_pabellon_75 = df_filtrada["duracion"].describe()["75%"]

        # Concatena resultados acumulados en el periodo por complicacion
        resultado_acumulado = resumen_filtrado[2]
        resultado_acumulado["complicacion"] = nombre_complicacion
        resultado_acumulado["texto_a_buscar"] = textos_a_buscar
        resultado_acumulado["tiempo_operacion_75%"] = tiempo_pabellon_75
        df_resultado = pd.concat([df_resultado, resultado_acumulado])

    return df_resultado
