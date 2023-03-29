# import mysql library, you must execute 'pip install pymysql' into terminal window
import pymysql.cursors

# create class connection
class clsConexion():
    # declare access data engine variables
    servidor = 'localhost'
    basedatos = 'NorthWind'
    usuario = 'root'
    contra = 'tamara11'

    # create function to execute (insert, update or delete SQL sentences)
    def ejecutar(self, AuxSql=''):
        try:
            # create local context with database engine
            conex = pymysql.connect(host=self.servidor,
                                    user=self.usuario,
                                    password=self.contra,
                                    database=self.basedatos,
                                    cursorclass=pymysql.cursors.DictCursor)

            # create local cursor, as executor sql sentences
            cursor = conex.cursor()
            cursor.execute(AuxSql)

            # applies changes data into table, mandatory
            conex.commit()

            # close database connections
            cursor.close()
            conex.close()
        except Exception as err:
            print(err)

    # create retrieve data function (select command)
    def consultar(self, AuxSql=''):
        # create data variable
        data = ''
        try:
            # create local context with database engine
            conex = pymysql.connect(host=self.servidor,
                                    user=self.usuario,
                                    password=self.contra,
                                    database=self.basedatos,
                                    cursorclass=pymysql.cursors.DictCursor)

            # create local cursor, as executor sql sentences
            cursor = conex.cursor()
            cursor.execute(AuxSql)

            # retrieve data and store into local variable
            data = cursor.fetchall()

            # close database connections
            cursor.close()
            conex.close()
        except Exception as err:
            print(err)

        return data

