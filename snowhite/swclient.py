# import pandas as pd
from snowhite.serialport import SerialPort
from snowhite.parser import ParserFixedWidth
# import json

SERIAL_CONFIG = {'port': 'COM3',
                 'baudrate': 9600,
                 'bytesize': 8,
                 'parity': 'N',
                 'stop': 1,
                 'timeout': 2,
                 'charendline': ''}

PARSER_CONFIG = {
    "delimiters": ["S", "E"],
    "parameters": {
        "startdatetime": {"position": [1, 20], "type": "datetime"},
        "duration": {"position": [21, 29],  "type": "timedelta"},
        "status": {"position": [30, 31],  "type": "int"},
        "Pdiff": {"position": [32, 36],  "type": "float"},
        "flowrate": {"position": [37, 43],  "type": "float"},
        "airvolume": {"position": [44, 51],  "type": "float"},
        "temperatureafilter": {"position": [52, 57],  "type": "float"},
        "Pafilter": {"position": [58, 63],  "type": "float"},
        "temperature": {"position": [64, 69],  "type": "float"},
        "winddir": {"position": [70, 73],  "type": "float"},
        "windspeed": {"position": [74, 78],  "type": "float"},
        "pressure": {"position": [79, 85],  "type": "float"},
        "humidity": {"position": [86, 91],  "type": "float"},
        "rainfall": {"position": [92, 96],  "type": "float"},
        "tamper_1": {"position": [97, 98],  "type": "int"},
        "tamper_2": {"position": [99, 100], "type": "int"},
        "actualdatetime": {"position": [101, 120], "type": "datetime"},
        "unknown0": {"position": [121, 128], "type": "float"},
        "unknown1": {"position": [129, 133], "type": "int"},
        "unknown2": {"position": [134, 141], "type": "float"}
    }
}


class SWClient:

    def __init__(self):

        # with open(path_config_file) as configfile:
        #     config = json.load(configfile)

        # Cargamos la configuracion de puerto serie
        self.serialport = SerialPort(SERIAL_CONFIG)
        self.serialport.connect()

        # Cargamos la configuracion de formato de datos
        self.parser = ParserFixedWidth(PARSER_CONFIG)

    def get_fmt_data(self):
        data = self.get_raw_data()
        df = self.parse_data(data)

        return df

    def parse_data(self, data):
        return self.parser.lineparser(data)

    def get_raw_data(self):

        self.serialport.write("poll\r\n".encode())
        data = self.serialport.getdata()

        return data
