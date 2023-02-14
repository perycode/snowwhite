from serial import Serial, SerialException
import io


class SerialPort(Serial):

    # iniciamos la clase: Esto será en un archivo aparte
    def __init__(self, params, *arg, **kwargs):
        # Permite pasar los argumentos a Serial.__init__
        super().__init__(*arg, **kwargs)

        self.charendline = None
        for nameparam in params:
            setattr(self, nameparam, params[nameparam])

    def connect(self):
        try:
            self.open()
            self._sio = io.TextIOWrapper(io.BufferedReader(self),
                                         newline=self.charendline)
            self._sio._CHUNK_SIZE = 1
            return self.is_open
        except SerialException:
            return False

    # disconnect from the serial port
    def disconnect(self):
        if self.is_open:
            self.close()

    def getdata(self):
        response = self._sio.readline()
        return response
