import pandas as pd
from io import StringIO


# Funciones de conversión
def convert_to_float(x):
    try:
        return float(x)
    except ValueError:
        return None


def convert_to_int(x):
    try:
        return float(x)
    except ValueError:
        return None


def convert_to_timedelta(x):
    try:
        return pd.to_timedelta(x)
    except ValueError:
        return None


# Diccionario con las funciones de conversión
CONVERTER = {
    'int': convert_to_int,
    'float': convert_to_float,
    'timedelta': convert_to_timedelta
}


class ParserFixedWidth():

    def __init__(self, config):

        delimiters = config['delimiters']
        self.inichar = delimiters[0]
        self.endchar = delimiters[1]

        self.names = []
        self.positions = []
        self.converters = {}
        self.parse_dates = []
        self.parse_timedeltas = []

        parameters = config['parameters']
        for name in parameters:
            self.names.append(name)
            self.positions.append(tuple(parameters[name]['position']))

            if parameters[name]['type'] == 'datetime':
                self.parse_dates.append(name)
                continue

            self.converters[name] = CONVERTER[parameters[name]['type']]

    def lineparser(self, linedata):
        strdata = StringIO(linedata[linedata.find(self.inichar) +
                                    1:linedata.find(self.endchar)])
        values = pd.read_fwf(strdata,
                             index_col=False,
                             names=self.names,
                             colspecs=self.positions,
                             converters=self.converters,
                             parse_dates=self.parse_dates)

        return values
