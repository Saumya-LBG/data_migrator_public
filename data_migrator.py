from data_sources import *
from job_interpreter import *
from data_migrator import *
import yaml
import logging

class DataMigrator:
    def __init__(self,spark,log_file=None):
        self.spark = spark
        self.log_file=log_file
        self._configure_logging()

    def _configure_logging(self):
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            handlers=[logging.StreamHandler(),
                                      logging.FileHandler(self.log_file,mode='w')])

    def read_config_file(self,file_path):
        with open(file_path, "r") as file:
            config=yaml.safe_load(file)
            log_file = config.get('log_file')
            return config,log_file
        



    def read_data_source(self,df,source_details,job_config):
        logging.info("Connecting to data source")
        data_source=datasourcefactory.read_data_source(self.spark,source_details,job_config)
        logging.info("Reading data....")

        # Stimulating data reading progress

        for i in range(1,11):
            logging.info(f"Reading {i*10}% completed...")
            #Stimulate delay in progress
            self._simulate_delay()

        logging.info("Data reading completed...")
        return data_source.read_data()

    def _simulate_delay(self):
        #simulate delay to show progress
        import time
        time.sleep(1)

    def write_to_destination(self,df,destination_details):
        file_format = destination_details['format']
        bucket = destination_details['bucket']
        path = destination_details['path']
        name = destination_details['name']

        output_path = f"{path}/{name}"

        logging.info("Writing data to destination....")

        if file_format == 'parquet':
            df.write.parquet(f"gs://{bucket}/{output_path}")

        elif file_format == 'csv':
            df.write.csv(f"gs://{bucket}/{output_path}")
        elif file_format == 'jason':
            df.write.jason(f"gs://{bucket}/{output_path}")
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        logging.info("Data writting completed")      

        
    
    def migrate_data(self,source_details,job_config,destination_details):
        # Read data from source
        df = self.read_data_source(source_details,job_config)

        # interpret job configuration and do transformation
        job_interpreter = JobInterpreter(job_config)
        interpreted_df = job_interpreter.interpreter(df)

        # Perform data migration
        self.write_to_destination(interpreted_df, destination_details)



    
