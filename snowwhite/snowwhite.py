#!/usr/bin/python3
import pathlib
import tkinter.ttk as ttk
import tkinter as tk
import pygubu
import json
import sys
from libs.serialport import SerialPort
from libs.parser import ParserFixedWidth
from libs.timer import RepeatTimer
from libs.mathutils import gaussian
from libs.mathutils import CentralStats
from libs.mathutils import timediffseconds
from datetime import datetime
import pandas as pd
import numpy as np
from matplotlib import use
import math

use('TkAgg')

from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg,
                                               NavigationToolbar2Tk)
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter

PROJECT_PATH = pathlib.Path(__file__).parent
PROJECT_UI = PROJECT_PATH / 'snowwhite.ui'

TIME_STEP = 20  # en segundos
VOLUME_CONVERSION = TIME_STEP / 3600.0
PWD = pathlib.Path(sys.executable).parent
OUTPUT_FOLDER = PWD / 'Output_Snowwhite'


class SnowWhiteApp:

    def __init__(self, master=None):
        self.builder = builder = pygubu.Builder()
        builder.add_resource_path(PROJECT_PATH)
        builder.add_from_file(PROJECT_UI)

        # Main widget
        self.mainwindow = builder.get_object('toplevel', master)
        self.mainwindow.protocol("WM_DELETE_WINDOW", self.close)

        self.trv = builder.get_object('treeview1')
        self.plotcontainer = builder.get_object('plot_container')
        self.plotcontainer2 = builder.get_object('plot_container2')

        builder.connect_callbacks(self)

        # Plot 1
        self.figure = Figure(figsize=(5, 3), dpi=100)
        self.figure.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.figure, master=self.plotcontainer)
        self.canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)

        self.figure2 = Figure(figsize=(5, 3), dpi=100)
        self.figure2.add_subplot(111)
        self.canvas2 = FigureCanvasTkAgg(self.figure2,
                                         master=self.plotcontainer2)
        self.canvas2.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=1)

        # Setup matplotlib toolbar (optional)
        # self.toolbar = NavigationToolbar2Tk(self.canvas, self.plotcontainer)
        # self.toolbar.update()
        # self.canvas._tkcanvas.pack(side=tk.TOP, fill=tk.BOTH, expand=1)

        # LOAD CONFIG FILE
        with open('config.json') as configfile:
            config = json.load(configfile)

        # Cargamos la configuracion de puerto serie
        self.com = SerialPort(config['configuracion puerto serie'])
        self.com.connect()

        # Cargamos la configuracion de formato de datos
        conf_parser = config['configuracion parser']
        self.parser = ParserFixedWidth(conf_parser['parametros'],
                                       conf_parser['delimitadores'])

        # Buffers de los datos
        # TODO: Se deben actualizar con las medidas de los archivos

        file_datadf = OUTPUT_FOLDER / 'snowwhite_finedata.json'
        if file_datadf.is_file():
            self.datadf = pd.read_json(file_datadf, orient='table')
        else:
            self.datadf = pd.DataFrame()

        # Releeemos los datos horarios de los pasados 10 días
        # esto es un loop sobre los ultimos 10 días
        dtnow = pd.Timestamp.today()
        self.measuredf = pd.DataFrame(columns=[
            'measuretime', 'meanflowrate', 'sdflowrate', 'nflowrate', 'volume',
            'errorvolume'
        ])

        for sdate in (dtnow - pd.Timedelta(9 - n, 'd') for n in range(10)):

            nfile_measureddf = 'snowwhite_' + \
                               sdate.strftime('%y%m%d') + '.json'
            file_measuredf = (OUTPUT_FOLDER / sdate.strftime('%Y') /
                              nfile_measureddf)

            if file_measuredf.is_file():
                self.measuredf = pd.concat([
                    self.measuredf,
                    pd.read_json(file_measuredf, orient='table')
                ])

        moving_lastweek = (self.measuredf.measuretime >
                           dtnow - pd.Timedelta(weeks=1))
        self.measuredf = self.measuredf[moving_lastweek]

        nfile_sampledf = 'filters_' + dtnow.strftime('%Y') + '.json'
        file_sampledf = (OUTPUT_FOLDER / dtnow.strftime('%Y') / nfile_sampledf)
        if file_sampledf.is_file():
            self.sampledf = pd.read_json(file_sampledf, orient='table')
            self.samplestats = CentralStats(self.sampledf['n'].iloc[-1],
                                            self.sampledf['sum'].iloc[-1],
                                            self.sampledf['sumsq'].iloc[-1])
        else:
            self.sampledf = pd.DataFrame(columns=[
                'measuretime', 'duration', 'flowrate', 'errorflowrate',
                'volume', 'errorvolume', 'n', 'sum', 'sumsq'
            ])
            self.samplestats = CentralStats(0, 0.0, 0.0)

        # Iniciamos el update
        self.trigger = RepeatTimer(TIME_STEP, self.update)
        self.trigger.start()

    def run(self):
        self.mainwindow.mainloop()

    def update(self):

        # Descargamos los datos
        data = self.getdata()
        self.datadf = pd.concat([self.datadf, data])
        self.savejson(self.datadf, None, OUTPUT_FOLDER,
                      'snowwhite_finedata.json')
        # Para calcular el flow medido por senya:
        # if len(self.datadf) == 1:
        #     self.prevvolume = data.loc[0, 'airvolume']
        # else:
        #     data.loc[0, 'diff_volume'] = data.loc[0, 'airvolume'] - self.prevvolume
        #     self.prevvolume = data.loc[0, 'airvolume']
        #     data.loc[0, 'refflowrate'] = (data.loc[0, 'diff_volume'] / TIME_STEP) * 3600.0

        # Comprobamos si tenemos una nueva hora
        actualdatetime = data.iloc[0].actualdatetime
        actualdatehour = actualdatetime.round('H')
        timehoursecs = timediffseconds(actualdatetime, actualdatehour)

        if timehoursecs >= 0.0 and timehoursecs < TIME_STEP:
            measuredatehour = actualdatehour - pd.Timedelta(hours=1)
            data_lasthour = ((self.datadf.actualdatetime > measuredatehour) &
                             (self.datadf.actualdatetime <= actualdatehour))
            meanflow = self.datadf[data_lasthour].flowrate.mean()
            stdflow = self.datadf[data_lasthour].flowrate.std()
            numflow = len(self.datadf[data_lasthour])
            new_measure = [
                measuredatehour, meanflow, stdflow, numflow, meanflow,
                stdflow / math.sqrt(numflow)
            ]
            self.measuredf.loc[len(self.measuredf)] = new_measure

            # Guardamos los datos horarios del último día en un fichero
            daydate = self.measuredf.iloc[-1].measuretime.floor('D')
            self.savejson(self.measuredf, daydate,
                          OUTPUT_FOLDER / daydate.strftime('%Y'),
                          'snowwhite_' + daydate.strftime('%y%m%d') + '.json')

        # Guardamos la medida del filtro si está en funcionamiento
        if data.iloc[0].status == 1:
            startdatetime = data.iloc[0].startdatetime
            if startdatetime in self.sampledf['measuretime'].values:
                locindex = self.sampledf.index[self.sampledf.measuretime ==
                                               startdatetime]
            else:
                self.samplestats.reset()
                locindex = [len(self.sampledf)]

            self.samplestats.add(data.loc[0]['flowrate'])
            smeanflow, svarflow, sstdflow, serrorflow = self.samplestats.get_stats(
            )
            nflow, sumflow, sumsqflow = self.samplestats.get_sums()
            duration = timediffseconds(startdatetime, actualdatetime)

            new_sample = [
                startdatetime, duration, smeanflow, serrorflow,
                smeanflow * (duration / 3600.0),
                serrorflow * (duration / 3600.0), nflow, sumflow, sumsqflow
            ]
            self.sampledf.loc[locindex[0]] = new_sample

            # Guardamos las medidas de los filtros del último año
            yeardate = (self.sampledf.iloc[-1].measuretime.floor('d') -
                        pd.tseries.offsets.YearBegin())
            self.savejson(self.sampledf, yeardate,
                          OUTPUT_FOLDER / yeardate.strftime('%Y'),
                          'filters_' + yeardate.strftime('%Y') + '.json')

        # Filtramos las medidas en  datadf y measuredf
        moving_lasthour = (self.datadf.actualdatetime >
                           (actualdatetime - pd.Timedelta(hours=1)))
        self.datadf = self.datadf[moving_lasthour]

        moving_lastweek = (self.measuredf.measuretime >
                           actualdatetime - pd.Timedelta(weeks=1))
        self.measuredf = self.measuredf[moving_lastweek]

        # Actualizamos la tabla del GUI
        self.update_treeview(data)

        # Actualizamos los graficos
        self.update_plots()

    def update_plots(self):

        # Graficos
        self.figure.clear()
        self.figure2.clear()

        gs = self.figure.add_gridspec(1, 3, wspace=0)
        rtplot, histplot, pressplot = gs.subplots(sharey=True)

        rtplot.set_xlabel('Hora (UTC)')
        rtplot.set_ylabel('Caudal (m3/h)')
        rtplot.plot(self.datadf.actualdatetime,
                    self.datadf.flowrate,
                    label='Medida tiempo real')
        dt_end = pd.Timestamp.utcnow() + pd.Timedelta(seconds=10)
        dt_ini = dt_end - pd.Timedelta(hours=1)
        rtplot.set_xlim(dt_ini, dt_end)
        rtplot.legend()

        # Define the date format
        date_form = DateFormatter("%H:%M")
        rtplot.xaxis.set_major_formatter(date_form)

        n, bins, patches = histplot.hist(
            self.datadf.flowrate,
            # bins=30,
            density=True,
            orientation="horizontal")
        histplot.xaxis.set_ticks_position('top')
        histplot.xaxis.set_label_position('top')
        histplot.set_xlabel('Frecuencia relativa normalizada')

        if len(self.datadf) > 1:
            mean_flow = self.datadf.flowrate.mean()
            std_flow = self.datadf.flowrate.std(ddof=0)

            ymin = mean_flow - (4 * std_flow)
            ymax = mean_flow + (4 * std_flow)
            rtplot.set_ylim(ymin, ymax)
            histplot.set_ylim(ymin, ymax)

            # Grafico gaussiana
            y = np.arange(ymin, ymax, (ymax - ymin) / 100.0)
            histplot.plot(gaussian(y, mean_flow, std_flow), y)

        pressplot.scatter(self.datadf.Pdiff, self.datadf.flowrate)
        # if len(self.datadf) > 35:
        #     pressplot.acorr(self.datadf.flowrate - self.datadf.flowrate.mean(),
        #                     maxlags=34, normed=True,  usevlines=True)

        volumeplot = self.figure2.add_subplot(1, 1, 1)
        volumeplot.errorbar(self.measuredf.measuretime, self.measuredf.volume,
                            self.measuredf.errorvolume)
        volumeplot.set_xlabel('Fecha')
        volumeplot.xaxis.set_major_formatter(DateFormatter("%Hh %d/%m"))

        self.canvas.draw_idle()
        self.canvas2.draw_idle()

    def update_treeview(self, data):

        # Actualizamos la tabla de ultimos valores recibidos:
        # Borramos la tabla
        children = self.trv.get_children()
        self.trv.delete(*children)

        # recorremos las columnas
        for col in data:
            self.trv.insert('',
                            tk.END,
                            text=data[col].name,
                            values=data[col].values[0])

    @staticmethod
    def savejson(df, inidate, folder, namefile):

        if inidate is not None:
            filtdf = df.measuretime >= inidate
            outputdf = df[filtdf]
        else:
            outputdf = df.copy()

        folder.mkdir(parents=True, exist_ok=True)
        outputdf.to_json(folder / namefile, orient='table', index=False)

    def getdata(self):
        self.com.write("poll\r\n".encode())
        data = self.com.getdata()

        dt_now = datetime.now()
        print(dt_now, data)
        df = self.parser.lineparser(data)

        return df

    def close(self):
        self.mainwindow.destroy()
        self.trigger.cancel()


if __name__ == "__main__":
    app = SnowWhiteApp()
    app.run()
