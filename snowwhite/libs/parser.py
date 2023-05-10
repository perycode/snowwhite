import pandas as pd
from io import StringIO


class ParserFixedWidth():

    def __init__(self, parameters, delimiters):

        self.inichar = delimiters[0]
        self.endchar = delimiters[1]

        self.names = []
        self.positions = []
        self.types = {}
        self.parse_dates = []

        for name in parameters:
            self.names.append(name)
            self.positions.append(tuple(parameters[name]['position']))
            self.types[name] = parameters[name]['type']
            if parameters[name]['type'] == 'datetime':
                self.parse_dates.append(name)

    def lineparser(self, linedata):
        strdata = StringIO(linedata[linedata.find(self.inichar) +
                                    1:linedata.find(self.endchar)])
        values = pd.read_fwf(strdata,
                             index_col=False,
                             names=self.names,
                             colspecs=self.positions,
                             dtypes=self.types,
                             parse_dates=self.parse_dates)
        return values
