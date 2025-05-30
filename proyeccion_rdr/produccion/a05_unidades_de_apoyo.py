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
