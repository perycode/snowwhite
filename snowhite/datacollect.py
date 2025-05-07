from snowhite.swclient import SWClient
from snowhite.dbdump import FileDataBase
from snowhite.utils.timer import RepeatTimer
import pandas as pd
import time


class SnowWhiteDataCollect():

    def __init__(self, filedb, interval):

        self.sw = SWClient()
        self.interval = interval
        self.filedb = filedb
        self.trigger = RepeatTimer(self.interval, self.collect)
        self.trigger.start()

        self.prev_values = {}

    def collect(self):

        # Lee la medida instantenea
        data = self.sw.get_fmt_data()

        fecha_medida = str(data['actualdatetime'][0])
        duracion = str(data['duration'][0])
        estado = data['status'][0]
        difpresion = data['Pdiff'][0]
        caudal = data['flowrate'][0]
        volumen_cum = data['airvolume'][0]
        fecha_inicio = str(data['startdatetime'][0])

        # Se calcula el volumen por la regal del trapecio
        if len(self.prev_values) == 0:
            Tsegs = 1.0
            Qanterior = data['flowrate'][0]
        else:
            # Calcular la diferencia
            Tdiferencia = data['actualdatetime'][0] - self.prev_values['fecha']
            Tsegs = Tdiferencia.total_seconds()
            Qanterior = self.prev_values['caudal']

        # Obtener la diferencia en segundos
        volumen_inc = Tsegs * ((data['flowrate'][0] + Qanterior)/7200.0)

        self.prev_values['fecha'] = data['actualdatetime'][0]
        self.prev_values['caudal'] = data['flowrate'][0]

        db = FileDataBase(self.filedb)
        db.insert_data(fecha_medida,
                       fecha_inicio,
                       duracion,
                       estado,
                       difpresion,
                       caudal,
                       volumen_inc,
                       volumen_cum)
        db.close()


# EJEMPLO DE USO
swdc = SnowWhiteDataCollect('registro_test.db', 0.5)
time.sleep(150)  # It gets suspended for the given number of seconds
print('Threading finishing')
swdc.trigger.cancel()
