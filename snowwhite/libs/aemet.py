import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

url = 'https://www.aemet.es/es/eltiempo/' + \
      'observacion/ultimosdatos_3469A_datos-horarios.csv?' + \
      'k=ext&l=3469A&datos=det&w=0&f=temperatura&x=h24'

aemet = pd.read_csv(url,
                    names=[
                        'measuredatetime', 'temperature', 'pressure',
                        'humidity', 'humidws'
                    ],
                    usecols=[0, 1, 7, 9],
                    parse_dates=[0],
                    header=None,
                    skiprows=4)

Pstp = 1000.0  # 1013.5 unidades hPa
Tsp = 273.15  # unidades ºK

rho_stp = Pstp / Tsp

aemet['rho'] = (aemet.pressure) / (273.15 + aemet.temperature)
aemet['cfactor'] = np.sqrt(rho_stp / aemet.rho)

plt.plot(aemet.measuredatetime, aemet.cfactor)
plt.show()
