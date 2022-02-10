from MyApp import MyApp
from mysql.connector  import  Connect,errorcode
from mysql.connector  import  Error as MySqlError
import logging


class Core(MyApp):



        def __init__(self):
            self.loggingToFile('db.log')

        def connector(self,host,username,pwd):
            try:
               self.conx =  Connect(host=host,user=username,password=pwd)

            except MySqlError as e:
                if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    logging.error("Access Denied in Connect MySQL")
                else :
                    logging.error(e)

            else:
                    logging.info("Successfully connection to mysql")
                    self.set_cursor()


        def get_cursor(self):
            return self.__cursor

        def set_cursor(self):
            self.__cursor =  self.conx.cursor()


        def is_db_avail(self,db_name):
            try:
                self.__cursor.execute(f"use {db_name}")
            except Exception as e:
                logging.error(e)
                return False
            else:
                return True


        def create_db(self,db_name):
            try:
                if not self.is_db_avail(db_name):
                    self.__cursor.execute(f"create database {db_name}")
            except Exception as e:
                logging.error(e)
            else:
                logging.info(f"Successfully create db {db_name}")
                self.db_name = db_name


        def use_db(self):
            try :
                self.__cursor.execute(f"use {self.db_name}")
            except Exception as e:
                logging.error(e)
            else :
                logging.info(f"Successfully now use {self.db_name} db")


        def show_database(self):
            try :
                 self.__cursor.execute("show databases")
                 logging.info("show databases",self.__cursor.fetchall())
            except Exception as e:
                logging.error(e)

        def execute_and_common_fetch_all(self,statement):
            try:
                self.__cursor.execute(statement)

                data = self.__cursor.fetchall()
            except Exception as e:
                logging.error(e)
            else :
                return data



        def close_db_connection(self):
            self.conx.close()





        def delete_db(self):
            try:
                if  self.is_db_avail(self.db_name):
                    self.__cursor.execute(f"DROP database {self.db_name}")
                    self.conx.commit()
            except Exception as e:
                logging.error(e)
            else:
                logging.info(f"Successfully delete db {self.db_name}")
                self.db_name = ""




















