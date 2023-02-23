import pandas as pd
import sys
import json
import pathlib

import libs.serialport
import libs.parser
from libs.mathutils import timediffseconds
from libs.mathutils import CentralStats

PWD = pathlib.Path(sys.executable).parent
OUTPUT_FOLDER = PWD / 'Output_Snowwhite'


class SnowWhiteData:

    def __init__(self, output_folder, time_step):

        with open('config.json') as configfile:
            config = json.load(configfile)

        # Cargamos la configuracion de puerto serie
        self.serialport = libs.serialport.SerialPort(
            config['configuracion puerto serie'])
        self.serialport.connect()

        # Cargamos la configuracion de formato de datos
        configparser = config['configuracion parser']
        self.parser = libs.parser.ParserFixedWidth(
            configparser['parametros'], configparser['delimitadores'])

        # Parametros configurables
        self.tstep = time_step  # intervalo en segundos
        self.output_folder = output_folder

        # Buffer datos
        self.serialdata = pd.DataFrame()
        self.swdata = pd.DataFrame()
        self.statdata = pd.DataFrame()
        self.sampledata = pd.DataFrame()

        swdatafile = output_folder / 'snowwhite_ultimahora.json'
        if swdatafile.is_file():
            self.swdata = pd.read_json(swdatafile, orient='table')

        # Releeemos los datos horarios de los pasados 10 días
        # esto es un loop sobre los ultimos 10 días
        today = pd.Timestamp.today()
        trange = 10
        for statdate in (today - pd.Timedelta(trange - (1 + n), 'd')
                         for n in range(trange)):

            statdatafile = (self.ouput_folder / statdate.strftime('%Y') /
                            ('snowwhite_datoshorarios_' +
                             statdate.strftime('%y%m%d') + '.json'))

            if statdatafile.is_file():
                self.statdata = pd.concat([
                    self.statdata,
                    pd.read_json(statdatafile, orient='table')
                ])

        # SAMPLE DATATFRAME
        sampledatafile = (self.ouput_folder / today.strftime('%Y') /
                          ('filtros_' + today.strftime('%Y') + '.json'))
        if sampledatafile.is_file():
            self.sampledata = pd.read_json(sampledatafile, orient='table')
            self.sampledstats = CentralStats(self.sampledata['n'].iloc[-1],
                                             self.sampledata['sum'].iloc[-1],
                                             self.sampledata['sumsq'].iloc[-1])

    def receive_data(self):
        # Datos recibidos
        strdata = self.get_serialdata()
        self.serialdata = serialdata = self.parser.lineparser(strdata)

        # si status = 0 (bomba apagada) no almacenamos los datos
        if serialdata.status == 0:
            return

        # DataFrame swdata:
        # Se guardan en swdata los datos snowwhite correspondientes a la última
        # hora con
        # una frecuencia de self.time_step cuando la la bomba esta en
        # funcionamiento
        self.swdata = pd.concat([self.swdata, serialdata])
        self.savejson(self.swdata, None, self.ouput_folder,
                      'snowwhite_ultimahora.json')

        # DataFrame statdata:
        # Se guardan los datos estadisticos correspondientes a los
        # últimos diez días
        # con una frecuencia de una hora cuando la bomba está en funcionamiento
        actualdatetime = self.swdata.iloc[0].actualdatetime
        actualdatehour = actualdatetime.round('H')
        timehoursecs = timediffseconds(actualdatetime, actualdatehour)

        if timehoursecs >= 0.0 and timehoursecs < self.time_step:
            measuredatehour = actualdatehour - pd.Timedelta(hours=1)
            data_lasthour = ((self.swdata.actualdatetime > measuredatehour) &
                             (self.swdata.actualdatetime <= actualdatehour))
            statrow = self.swdata[data_lasthour].copy()
            # cuidado deben el tamaño debe ser mayor que uno
            metrics = {
                'flowrate': ['count', 'mean', 'std', 'sem'],
                'Pdiff': ['count', 'mean', 'std', 'sem']
            }
            statrow.agg(metrics)
            statrow = statrow.unstack().to_frame().T
            statrow.columns = statrow.columns.map('{0[0]}_{0[1]}'.format)

            # incluimos las fechas
            statrow.insert(0, 'time', actualdatehour)
            statrow.insert(1, 'measuretime', measuredatehour)

            # Calculamos el volumen
            statrow['volume'] = statrow.flowrate_mean * (
                (statrow.flowrate_count * self.tstep) * 3600)
            statrow['volume_error'] = statrow.flowrate_sem * (
                (statrow.flowrate_count * self.tstep) * 3600)

            self.statdata = pd.concat(self.statdata,
                                      statrow,
                                      ignore_index=True)
            # Se ordenan por fecha:
            # los valores recientes al final del dataframe
            self.statdata.sort_values(by='time', inplace=True)

            # Guardamos los datos horarios del último día en un fichero
            lastday = self.statdata.iloc[-1].measuretime.floor('D')
            self.savejson(
                self.statdata, lastday,
                self.ouput_folder / lastday.strftime('%Y'),
                'snowwhite_datoshorarios_' + lastday.strftime('%y%m%d') +
                '.json')

        # DataFrame sampledata:
        # Se guardan los datos estadisticos correspondientes a la
        # medida del filtro
        startdatetime = serialdata.iloc[0].startdatetime
        if startdatetime in self.sampledata['measuretime'].values:
            locindex = self.sampledata.index[self.sampledata.measuretime ==
                                             startdatetime]
        else:
            self.samplestats.reset()
            locindex = [len(self.sampledata)]

        self.samplestats.add(serialdata.loc[0]['flowrate'])
        smeanflow, svarflow, sstdflow, serrorflow = self.samplestats.get_stats(
        )
        nflow, sumflow, sumsqflow = self.samplestats.get_sums()
        duration = timediffseconds(startdatetime, actualdatetime)

        new_sample = [
            startdatetime, duration, smeanflow, serrorflow,
            smeanflow * (duration / 3600.0), serrorflow * (duration / 3600.0),
            nflow, sumflow, sumsqflow
        ]
        self.sampledata.loc[locindex[0]] = new_sample

        # Guardamos las medidas de los filtros del último año
        yeardate = (self.sampledf.iloc[-1].measuretime.floor('d') -
                    pd.tseries.offsets.YearBegin())
        self.savejson(self.sampledf, yeardate,
                      self.ouput_folder / yeardate.strftime('%Y'),
                      'filters_' + yeardate.strftime('%Y') + '.json')

    def del_olddata(self):
        # Filtramos las medidas en  swdatadf y measuredf
        actualdatetime = self.swdata.iloc[0].actualdatetime
        moving_lasthour = (self.swdata.actualdatetime >
                           (actualdatetime - pd.Timedelta(hours=1)))
        self.swdata = self.swdata[moving_lasthour]

        moving_lastweek = (self.statdata.measuretime >
                           actualdatetime - pd.Timedelta(weeks=1))
        self.statdata = self.statdata[moving_lastweek]

    @staticmethod
    def save_json(df, inidate, folder, namefile):

        if inidate is not None:
            filtdf = df.measuretime >= inidate
            outputdf = df[filtdf]
        else:
            outputdf = df.copy()

        folder.mkdir(parents=True, exist_ok=True)
        outputdf.to_json(folder / namefile, orient='table', index=False)

    def get_serialdata(self):
        self.serialport.write("poll\r\n".encode())
        data = self.serialport.getdata()

        dt_now = pd.Timestamp.today()
        print(dt_now, data)
        df = self.parser.lineparser(data)

        return df
