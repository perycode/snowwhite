import pandas as pd
import sys
import pathlib
import json

import libs.serialport
import libs.parser
from libs.mathutils import timediffseconds
from libs.mathutils import RecursiveStats
from libs.timer import RepeatTimer, nextevent

import threading

PWD = pathlib.Path(sys.executable).parent
OUTPUT_FOLDER = PWD / 'Output_Snowwhite'


class SnowWhite:
    """ Clase Comunicación RS485 snowwhite

    Ejemplo de código::

        from snowwhite import snowwhite
        import time

        config = {
            "configuracion medidas": {
                "timeperiod": 20,
                "gammaperiod": 3600,
                "numbkdata": 200
            },
            "configuracion puerto serie": {
                "port": "COM3",
                "baudrate": 9600,
                "bytesize": 8,
                "parity": "N",
                "stop": 1,
                "timeout": 2,
                "charendline": ""
            },
            "configuracion parser":{
                "delimitadores": ["S", "E"],
                "parametros":{
                    "startdatetime": {
                        "position": [1,20],
                        "type": "datetime",
                        "format": "%d/%m/%Y %H:%m:%s"
                    },
                    "duration": {
                        "position": [21,29],
                        "type": "datetime",
                        "format": "%H:%m:%s"
                    },
                    "status": {
                        "position": [30,31],
                        "type": "int64"
                    },
                    "Pdiff": {
                        "position": [32,36],
                        "type": "int64"
                    },
                    "flowrate": {
                        "position": [37,43],
                        "type": "float64"
                    },
                    "airvolume": {
                        "position": [44,51],
                        "type": "float64"
                    },
                    "temperatureafilter": {
                        "position": [52,57],
                        "type": "float64"
                    },
                    "Pafilter": {
                        "position": [58,63],
                        "type": "float64"
                    },
                    "temperature": {
                        "position": [64,69],
                        "type": "float64"
                    },
                    "winddir": {
                        "position": [70,73],
                        "type": "int64"
                    },
                    "windspeed": {
                        "position": [74,78],
                        "type": "float64"
                    },
                    "pressure": {
                        "position": [79,85],
                        "type": "float64"
                    },
                    "humidity": {
                        "position": [86,91],
                        "type": "float64"
                    },
                    "rainfall": {
                        "position": [92,96],
                        "type": "float64"
                    },
                    "tamper_1": {
                        "position": [97,98],
                        "type": "int64"
                    },
                    "tamper_2": {
                        "position": [99,100],
                        "type": "int64"
                    },
                    "actualdatetime": {
                        "position": [101,120],
                        "type": "datetime",
                        "format": "%d/%m/%Y %H:%m:%s"
                    },
                    "unknown0": {
                        "position": [121,128],
                        "type": "float64"
                    },
                    "unknown1": {
                        "position": [129,133],
                        "type": "int64"
                    },
                    "unknown2": {
                        "position": [134,141],
                        "type": "float64"
                    }
                }
            }
        }

        sw = SnowWhite(config)
        time.sleep(30)
        data = sw.getdata(1)


    """

    def __init__(self, config: dict) -> None:
        """ Inicia la clase. Únicamente tiene como parámetro un diccionario con
        los valores de configuración. La configuración se divide en varias
        secciones:

        * configuracion medidas: Configuración de la toma de medidas.
        * configuracion puerto seria: Configuración de los parámetros del puerto serie
        * configuracion parser: Configuración del formato de recibidos.

        """
        # Cargamos la configuracion de puerto serie
        self.serialport = libs.serialport.SerialPort(
            config['configuracion puerto serie'])
        self.serialport.connect()

        # Cargamos la configuracion de formato de datos
        configparser = config['configuracion parser']
        self.parser = libs.parser.ParserFixedWidth(
            configparser['parametros'], configparser['delimitadores'])

        # Cargamos la configuracion de las medidas
        configmeasure = config["configuracion medidas"]
        for nameparam in configmeasure:
            setattr(self, nameparam, configmeasure[nameparam])

        # Buffer datos
        self.intervaldf = pd.DataFrame()
        self.gammadf = pd.DataFrame(columns=[
            'inidate', 'enddate', 'flow', 'errorflow', 'volume', 'errorvolume'
        ])
        self.gammabuffer = {
            'inimeasure': None,
            'endmeasure': None,
            'measure': RecursiveStats()
        }

        self.sampledf = pd.DataFrame(columns=[
            'inidate', 'enddate', 'flow', 'errorflow', 'volume', 'errorvolume'
        ])
        self.samplebuffer = {
            'inimeasure': None,
            'endmeasure': None,
            'measure': RecursiveStats()
        }

        # Iniciamos el update
        self.dflock = threading.Lock()
        self.trigger = RepeatTimer(self.timeperiod, self.update_data)
        self.trigger.start()

    def thread_status(self) -> bool:
        """
        Comprueba si el hilo que recoje datos a intervalos regulares está
        activo.

        :returns: **True** si el hilo está activo. **False** en cualquier
            otro caso.
        :rtype: bool

        """

        return self.trigger.is_alive()

    def thread_reboot(self) -> None:
        """
        Reinicia el hilo de recepción de datos.

        :rtype: None

        """

        if self.thread_status():
            self.trigger.cancel()

        self.trigger = RepeatTimer(self.timeperiod, self.update_data)
        self.trigger.start()

    def update_data(self) -> None:
        """
        Recibe los datos directamente y los almacena en los diferentes buffers.
        En caso necesario, realiza los promedios y los errores.

        :rtype: None

        """

        self.dflock.acquire()

        self.intervaldf, shift_time = self.get_rtdata()
        self.intervaldf['shiftime'] = [shift_time.total_seconds()]

        # GAMMA MEASURE
        inimeasure, endmeasure = nextevent(
            self.gammaperiod, self.intervaldf['actualdatetime'].iloc[0])

        if endmeasure != self.gammabuffer['endmeasure']:
            if self.gammabuffer['endmeasure'] is not None:
                # guardamos en el df
                n, flow, errorflow = self.gammabuffer['measure'].stats
                new_measure = [
                    self.gammabuffer['inimeasure'],
                    self.gammabuffer['endmeasure'], flow, errorflow,
                    (flow / 3600) * self.gammaperiod,
                    (errorflow / 3600) * self.gammaperiod
                ]
                self.gammadf.loc[len(self.gammadf) + 1] = new_measure
                # eliminar filas necesarias para que el df tenga el
                # tamaño asignado en el config
                self.gammadf.sort_values(by='inidate',
                                         ascending=False,
                                         inplace=True)
                self.gammadf = self.gammadf.reset_index(drop=True)
                self.gammadf = self.gammadf.iloc[:self.numbkdata, :]

            self.gammabuffer['endmeasure'] = endmeasure
            self.gammabuffer['inimeasure'] = inimeasure
            self.gammabuffer['measure'].reset()

        self.gammabuffer['measure'] += self.intervaldf['flowrate'].iloc[0]

        # SAMPLE MEASURE
        if self.intervaldf['status'].iloc[0] == 1:

            if self.intervaldf['startdatetime'].iloc[0] != self.samplebuffer[
                    'inimeasure']:
                self.samplebuffer['inimeasure'] = self.intervaldf[
                    'startdatetime'].iloc[0]
                self.samplebuffer['measure'].reset()

            self.samplebuffer['endmeasure'] = self.intervaldf[
                'actualdatetime'].iloc[0]

            self.samplebuffer['measure'] += self.intervaldf['flowrate'].iloc[0]

        elif self.intervaldf['status'].iloc[0] == 0:
            if self.samplebuffer['inimeasure'] not in self.sampledf[
                    'inidate'].values:
                # guardamos en el df
                n, flow, errorflow = self.samplebuffer['measure'].stats
                duration = timediffseconds(self.samplebuffer['inimeasure'],
                                           self.samplebuffer['endmeasure'])
                new_measure = [
                    self.samplebuffer['inimeasure'],
                    self.samplebuffer['endmeasure'], flow, errorflow,
                    (flow / 3600) * duration, (errorflow / 3600) * duration
                ]
                self.sampledf.loc[len(self.sampledf) + 1] = new_measure
                # eliminar filas necesarias para que el df tenga el
                # tamaño asignado en el config
                self.sampledf.sort_values(by='inidate',
                                          ascending=False,
                                          inplace=True)
                self.sampledf = self.sampledf.reset_index(drop=True)
                self.sampledf = self.sampledf.iloc[:self.numbkdata, :]

        self.dflock.release()

    def get_rtdata(self) -> float:
        """
        Consulta de los datos medidos actualmente en la estación snowwhite.

        :returns: Los valores medidos actualmente en la estación snowwhite
            y el desfase temporal del reloj interno de la snowhwite con el pc
            que hace la consulta.
        :rtype: float

        """

        self.serialport.write("poll\r\n".encode())
        data = self.serialport.getdata()

        utcnow = pd.Timestamp.utcnow()
        df = self.parser.lineparser(data)

        return df, utcnow.replace(tzinfo=None) - df.actualdatetime.iloc[0]

    def getdata(self, numdata: int) -> dict:
        """
        datos de volumenes en la estacion


        :param numdata:
        :type numdata: int
        :returns: los datos derivados y el ultimo dato.
        :rtype: dict

        """

        self.dflock.acquire()

        data = {'data': {}}

        data['data']['ultimo dato'] = self.intervaldf[:numdata].to_dict(
            'records')
        data['data']['medidas gamma'] = self.gammadf[:numdata].to_dict(
            'records')
        data['data']['medidas filtro'] = self.sampledf[:numdata].to_dict(
            'records')

        self.dflock.release()

        return data

    def interrupt(self) -> None:
        """
        Termina el hilo de recepción de datos y cierra la conexión con
        el puerto COM. Es recomendable antes de cerrar el programa ejectutar
        este método.

        :rtype: None

        """

        self.trigger.cancel()
        self.serialport.disconnect()


# TESTING
with open('config.json') as configfile:
    config = json.load(configfile)
