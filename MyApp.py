import logging

class MyApp():

    def __init__(self):
        pass


    def loggingToFile(self,file_name):
        logging.basicConfig(filename=file_name,force=True,level=logging.DEBUG,format="%(asctime)s %(levelname)s %(message)s")





