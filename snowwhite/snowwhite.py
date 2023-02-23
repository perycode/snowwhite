#!/usr/bin/python3
import pathlib
import tkinter.ttk as ttk
import tkinter as tk
import pygubu
import sys
from snowwhitecore import SnowWhiteData
from libs.timer import RepeatTimer
from libs.mathutils import gaussian
import pandas as pd
import numpy as np
from matplotlib import use

use('TkAgg')

from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
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

        # LOAD CONFIG FILE
        # with open('config.json') as configfile:
        #     config = json.load(configfile)

        self.snowwhite = SnowWhiteData(OUTPUT_FOLDER, TIME_STEP)

        # Iniciamos el update
        self.trigger = RepeatTimer(TIME_STEP, self.update)
        self.trigger.start()

    def run(self):
        self.mainwindow.mainloop()

    def update(self):

        # Descargamos los datos
        self.snowwhite.receive_data()
        # Se borran datos antiguos
        self.snowwhite.del_olddata()

        # Actualizamos la tabla del GUI
        self.update_treeview(self.snowwhite.serialdata)

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
        rtplot.plot(self.snowwhite.swdata.actualdatetime,
                    self.snowwhite.swdata.flowrate,
                    label='Medida tiempo real')
        dt_end = pd.Timestamp.utcnow() + pd.Timedelta(seconds=10)
        dt_ini = dt_end - pd.Timedelta(hours=1)
        rtplot.set_xlim(dt_ini, dt_end)
        rtplot.legend()

        # Define the date format
        date_form = DateFormatter("%H:%M")
        rtplot.xaxis.set_major_formatter(date_form)

        n, bins, patches = histplot.hist(self.snowwhite.swdata.flowrate,
                                         density=True,
                                         orientation="horizontal")
        histplot.xaxis.set_ticks_position('top')
        histplot.xaxis.set_label_position('top')
        histplot.set_xlabel('Frecuencia relativa normalizada')

        if len(self.snowwhite.swdata) > 1:
            mean_flow = self.snowwhite.swdata.flowrate.mean()
            std_flow = self.snowwhite.swdata.flowrate.std(ddof=0)

            ymin = mean_flow - (4 * std_flow)
            ymax = mean_flow + (4 * std_flow)
            rtplot.set_ylim(ymin, ymax)
            histplot.set_ylim(ymin, ymax)

            # Grafico gaussiana
            y = np.arange(ymin, ymax, (ymax - ymin) / 100.0)
            histplot.plot(gaussian(y, mean_flow, std_flow), y)

            pressplot.scatter(self.snowwhite.swdata.Pdiff,
                              self.snowwhite.swdata.flowrate)

        volumeplot = self.figure2.add_subplot(1, 1, 1)
        volumeplot.errorbar(self.snowwhite.statdata.measuretime,
                            self.snowwhite.statdata.volume,
                            self.snowwhite.statdata.errorvolume)
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

    def close(self):
        self.mainwindow.destroy()
        self.trigger.cancel()


if __name__ == "__main__":
    app = SnowWhiteApp()
    app.run()
