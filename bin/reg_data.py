import argparse
from snowhite.swclient import SWClient


def save_data_to_db():
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
    parser.add_argument('--db',
                        type=str,
                        required=True,
                        help=' = directos, formateados o ambos; \
                        para datos sin formatear, formateados \
                        o ambos, respectivamente')
    parser.add_argument('--interval',
                        type=float,
                        required=True,
                        help=' = directos, formateados o ambos; \
                        para datos sin formatear, formateados \
                        o ambos, respectivamente')

    args = parser.parse_args()


    sw = SWClient()
