import argparse
from snowhite.swclient import SWClient


def fetch_idata():
    """
    Descarga los datos SIG de las estaciones SAIH.

    Configura el analizador de argumentos para aceptar los
    parámetros necesarios y descarga los datos de las
    estaciones según la confederación especificada.

    :return: None
    """

    # Configura el analizador de argumentos
    parser = argparse.ArgumentParser(
        description="Descarga datos instataneos de la bomba \
        de alto flujo Snowwhite")
    parser.add_argument('--datos',
                        type=str,
                        required=True,
                        help=' = directos, formateados o ambos; \
                        para datos sin formatear, formateados \
                        o ambos, respectivamente')

    args = parser.parse_args()

    sw = SWClient()

    if args.datos == 'formateados':
        swdf = sw.get_fmt_data()
        print('Datos formateados:')
        for signal in swdf.columns:
            print(signal, '=', swdf[signal][0])
        print('')

    elif args.datos == 'directos':
        swdata = sw.get_raw_data()
        print('Cadena sin formatear recibida:')
        print(swdata)
        print('')
    elif args.datos == 'ambos':
        swdata = sw.get_raw_data()
        print('Cadena sin formatear recibida:')
        print(swdata)
        print('')

        swdf = sw.parse_data(swdata)
        print('Datos obtenidos de la cadena recibida:')
        for signal in swdf.columns:
            print(signal, '=', swdf[signal][0])
        print('')

    else:
        print('Valor del parámetros datos no válido')
