from db.Core import Core
import logging
import csv



class DbTableConnection:


    def __init__(self,host,username,pwd,db_name):
        self.__c = Core()
        self.__c.connector(host=host,username=username,pwd=pwd)
        self.__c.create_db(db_name)
        self.__c.use_db()
        self.__table_name = ''





    def create_table(self,table_name,cols):
        try :
            self.__c.get_cursor().execute(f"create table {self.__c.db_name}.{table_name}({cols})")
        except Exception as e:
            logging.error(e)

        else:
            logging.info(f"Successfully create table {table_name+cols}")

        finally:
            logging.info(f" table and cols {table_name+cols}")

            self.__table_name = table_name

    def show_tables(self):
        try :
            self.__c.get_cursor().execute("show tables")
            logging.info("show tables "+str(self.__c.get_cursor().fetchall()))
        except Exception as e:
            logging.error(e)


    def insert_data_from_file(self,file_name,mode='r'):
        try:
            with open(file_name,mode) as f:
                data = csv.reader(f,delimiter='\n')
                #logging.info("size of data of file "+str(sum(1 for row in data)))
                for i in data:
                    try:
                        pass
                        #logging.info('insert into {table_name} values({values})'.format(values=((i[0])),table_name=(self.__c.db_name+"."+self.__table_name)))
                        self.__c.get_cursor().execute('insert into {table_name} values(%s, %s, %s, %s, %s, %s, %s)'.format(table_name=(self.__c.db_name+"."+self.__table_name)),(i[0].split(",")))
                        self.__c.conx.commit()
                    except Exception as e:
                        logging.error("DbTableConnection 46"+str(e))
                else :


                    logging.info("Successfully inserted data into "+str(self.__table_name))
        except Exception as e:
            logging.error("DbTableConnection 50 "+str(e))


    def fetch_all_data(self):
        try :
            self.__c.get_cursor().execute(
                "SELECT * from {__table_name}".format(__table_name=self.__c.db_name + "." + self.__table_name))
            datas = self.__c.get_cursor().fetchall()
        except Exception as e:
            logging.error(e)
        else:

            logging.info( datas)
            return datas

    def check_data_complete(self,file_name):
        try :
            table_data = self.fetch_all_data()

        except Exception as e:
            logging.error(e)

        else:
            try :
                logging.info("start checking")
                with open(file_name) as f:
                    csv_data = csv.reader(f, delimiter='\n')
                    logging.info("is all data loaded into table "+str( len(table_data)==sum(1 for row in csv_data)))

            except Exception as e:
                logging.error(e)


    def get_count_of_group_by(self,col_name):
        try :
            self.__c.get_cursor().execute("SELECT {col_name},COUNT(*) FROM {table_name} GROUP BY {col_name} ".format(table_name=self.__c.db_name + "." + self.__table_name,col_name=col_name))
            data = self.__c.get_cursor().fetchall()
        except Exception as e:
            logging.error(e)
        else:
            logging.info('\n'.join(map(str, data)))
            return data


    def exact_filter_on_single_col(self,col_name,value):
        try :
            data = self.__c.execute_and_common_fetch_all("SELECT * FROM {table_name} WHERE {col_name}={value}".format(value=value,table_name=self.__c.db_name + "." + self.__table_name,col_name=col_name))

        except Exception as e:
            logging.error(e)
        else:
            logging.info("\n".join(map(str,data)))
            return data







    def update(self,set_col_name,set_col_value,search_col_name,search_col_value):

        try :
             sql_statement = """UPDATE  {table_name} SET {set_col_name}='{set_col_value}'
                                                    WHERE {search_col_name}='{search_col_value}'""".format(set_col_value=str(set_col_value),
                                                          table_name=self.__c.db_name + "." + self.__table_name,
                                                          search_col_value=str(search_col_value),
                                                          search_col_name=search_col_name,
                                                          set_col_name=set_col_name,
                                                          )
             logging.info(sql_statement)
             self.__c.get_cursor().execute(sql_statement)
             self.__c.conx.commit()
             data = self.fetch_all_data()
        except Exception as e:
            logging.error(e)
        else :
            logging.info("\n".join(map(str,data)))
            return data


    def delete_table(self):
        try:
            sql_statement = f"DROP TABLE {self.__table_name}"
            self.__c.get_cursor().execute(sql_statement)
            self.__c.conx.commit()
        except Exception as e:
            logging.error(e)

        else:

            logging.info(f"Delete table {self.__table_name} Successfully")


    def delete_db(self):
        self.__c.delete_db()




























