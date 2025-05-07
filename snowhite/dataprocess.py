import sqlite3
from snowhite.dbdump import FileDataBase
from snowhite.utils.timer import RepeatTimer
import time
import pandas as pd
from snowhite.dbdump import FileDataBase
from snowhite.utils.timer import RepeatTimer


class SnowWhiteDataProcess():

    def __init__(self, filedb, interval):

        self.interval = interval
        self.filedb = filedb

    def update_hourly_data(self):
        db = FileDataBase(self.filedb)

        # Consulta ultimos valores para detreminar que valores
        # calcular los volumenes horarios
        ultimos_valores = db.get_dataframe('SELECT * FROM Ultimos_Valores')
        ultimos_valores['FECHA'] = pd.to_datetime(ultimos_valores['FECHA'])
        ultimos_valores.set_index('TIPO', inplace=True)

        datehourTR = ultimos_valores.loc['Valores_Tiempo_Real',
                                         'FECHA'].floor('H')
        datehourH = ultimos_valores.loc['Valores_Horarios', 'FECHA']

        # CALCULO DE LOS VALORES HORARIOS
        if pd.isna(datehourH):
            values_unproccessed = db.get_dataframe(
                "SELECT * FROM Valores_Tiempo_Real " +
                "WHERE FECHA <'" +
                datehourTR.strftime('%Y-%m-%d %H:%M:%S') +
                "'")
        else:
            values_unproccessed = db.get_dataframe(
                "SELECT * FROM Valores_Tiempo_Real " +
                "WHERE FECHA >='" +
                datehourH.strftime('%Y-%m-%d %H:%M:%S') +
                "' AND FECHA < '" +
                datehourTR.strftime('%Y-%m-%d %H:%M:%S') +
                "'")
        # Convertir la columna FECHA a tipo datetime
        values_unproccessed['FECHA'] = pd.to_datetime(
            values_unproccessed['FECHA'])
        values_unproccessed.set_index('FECHA', drop=False, inplace=True)

        # Calcular el intervalo de tiempo en segundos entre cada fecha
        intervalo = values_unproccessed['FECHA'].diff()
        intervalo = intervalo.dt.total_seconds().fillna(0)

        # Calcular el volumen en cada fecha (caudal * intervalo de tiempo)
        values_unproccessed['volumen'] = ((values_unproccessed['CAUDAL']
                                           / 3600.0)
                                          * intervalo)

        # Agrupar por las fechas redondeadas y calcular el valor acumulado
        hourly_values = values_unproccessed.resample('H',
                                                     closed='left',
                                                     label='right').agg({
                                                         'FECHA': 'max',
                                                         'volumen': 'sum',
                                                         'CAUDAL': 'mean'
                                                     })

        for index, row in hourly_values.iterrows():
            db.insert_hourly_data(str(index),
                                  row['CAUDAL'],
                                  row['volumen'],
                                  str(str(index)))

        db.close()
        return hourly_values


swdproc = SnowWhiteDataProcess('snowdata.db', 0.5)
q = swdproc.update_hourly_data()
print(q)
