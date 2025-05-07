import sqlite3
from pathlib import Path
import pandas as pd


class FileDataBase():
    def __init__(self, filedb):

        # ruta al archivo de la base de datos
        filedb_path = Path(filedb)
        # Verificar si el archivo existe
        if filedb_path.exists():
            filedb_exists = True
        else:
            filedb_exists = False

        # Conectar a la base de datos SQLite
        # (creará el archivo de base de datos si no existe)
        self.connection = sqlite3.connect(filedb)

        # Crear un objeto cursor para interactuar con la base de datos
        self.cursor = self.connection.cursor()

        if filedb_exists is False:
            # Se crea la base de datos
            self.create_database()

    def create_database(self):
        # Crear una tabla con las columnas especificadas y sus tipos
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS Valores_Tiempo_Real (
        FECHA TEXT PRIMARY KEY,
        FECHA_MEDIDA TEXT,
        DURACION TEXT,
        ACTIVO INTEGER,
        DIFPRESION REAL,
        CAUDAL REAL,
        VOLUMEN_INC REAL,
        VOLUMEN_CUM REAL
        )
        ''')

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS Valores_Horarios (
        FECHA TEXT PRIMARY KEY,
        CAUDAL REAL,
        VOLUMEN REAL
        )
        ''')

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS Valores_Filtro (
        FECHA_MEDIDA TEXT PRIMARY KEY,
        DURACION TEXT,
        CAUDAL REAL,
        VOLUMEN REAL
        )
        ''')

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS Ultimos_Valores (
        TIPO TEXT PRIMARY KEY,
        FECHA TEXT
        )
        ''')

        self.cursor.execute('''
        INSERT INTO Ultimos_Valores (TIPO, FECHA)
        VALUES (?, ?)
        ''', ('Valores_Tiempo_Real', ''))

        self.cursor.execute('''
        INSERT INTO Ultimos_Valores (TIPO, FECHA)
        VALUES (?, ?)
        ''', ('Valores_Horarios', ''))

        self.cursor.execute('''
        INSERT INTO Ultimos_Valores (TIPO, FECHA)
        VALUES (?, ?)
        ''', ('Valores_Filtro', ''))

        # Confirmar los cambios
        self.connection.commit()

    def insert_data(self, fecha_medida, fecha_inicio, duracion, estado,
                    difpresion, caudal, volumen_inc, volumen_cum):
        try:
            self.cursor.execute('''
            INSERT INTO Valores_Tiempo_Real (FECHA, FECHA_MEDIDA ,DURACION,
            ACTIVO, DIFPRESION, CAUDAL, VOLUMEN_INC, VOLUMEN_CUM)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (fecha_medida, fecha_inicio, duracion, estado,
                  difpresion, caudal, volumen_inc, volumen_cum))

            self.connection.commit()

        except sqlite3.IntegrityError:
            print('Valor ya registrado')
            # self.cursor.execute('''
            # UPDATE Valores_Tiempo_Real SET FECHA_MEDIDA=? ,DURACION=?,
            # ACTIVO=?, DIFPRESION=?, CAUDAL=?, VOLUMEN=? WHERE FECHA=?
            # ''', (fecha_inicio, duracion, estado,
            #       difpresion, caudal, volumen, fecha_medida))

            # self.connection.commit()

        # Insertar ultimos datod registrado en ultimos valores
        try:
            self.cursor.execute('''
            UPDATE Ultimos_Valores SET FECHA=? WHERE TIPO=?
            ''', (fecha_medida, 'Valores_Tiempo_Real'))

            self.connection.commit()

        except sqlite3.IntegrityError:
            print('Error Ultimos_VALORES: El valor ' + fecha_medida +
                  ' ya existe en la base de datos.')

    def insert_hourly_data(self, fecha, caudal, volumen, ultima_fecha):
        try:
            self.cursor.execute('''
            INSERT INTO Valores_Horarios (FECHA, CAUDAL, VOLUMEN)
            VALUES (?, ?, ?)
            ''', (fecha, caudal, volumen))

            self.connection.commit()

        except sqlite3.IntegrityError:
            print('Valor ya registrado')
            # self.cursor.execute('''
            # UPDATE Valores_Tiempo_Real SET FECHA_MEDIDA=? ,DURACION=?,
            # ACTIVO=?, DIFPRESION=?, CAUDAL=?, VOLUMEN=? WHERE FECHA=?
            # ''', (fecha_inicio, duracion, estado,
            #       difpresion, caudal, volumen, fecha_medida))

            # self.connection.commit()

        # Insertar ultimos dato registrado en ultimos valores
        try:
            self.cursor.execute('''
            UPDATE Ultimos_Valores SET FECHA=? WHERE TIPO=?
            ''', (ultima_fecha, 'Valores_Horarios'))

            self.connection.commit()

        except sqlite3.IntegrityError:
            print('Error Ultimos_VALORES: El valor ' + fecha +
                  ' ya existe en la base de datos.')

    def get_data(self):
        # Consultar la tabla para verificar la inserción de datos
        self.cursor.execute('SELECT * FROM valores')
        rows = self.cursor.fetchall()
        for row in rows:
            print(row)

    def get_dataframe(self, query):
        # Consultar la tabla para verificar la inserción de datos
        df = pd.read_sql_query(query, self.connection)
        return df

    def close(self):
        self.connection.close()

# Ejemplo de código:
# db = FileDataBase('swdata.db')
# # Ejemplo de uso
# db.insert_data('2023-09-01 00:00:00', '1:00:00', 1, 0.5, 10.0, 100.0, '2023-09-01 00:00:00')
# db.insert_data('2023-09-02 00:00:00', '2:00:00', 0, 0.6, 12.0, 120.0, '2023-09-02 00:00:00')

# db.get_data()
# db.close()
