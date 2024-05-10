from data_sources import *
from job_interpreter import *
from data_migrator import *
import yaml
import logging
from kafka import *

class DataMigrator:
    def __init__(self,spark,log_file):
        self.spark = spark
        self.log_file=log_file
        self._configure_logging()

    def _configure_logging(self):
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s',
                            handlers=[logging.StreamHandler(),
                                      logging.FileHandler(self.log_file,mode='w')])

    def read_config_file(self,config_file_1,config_file_2,config_file_3,application_file):
        
        with open(config_file_1, "r") as access_details:
            access_details=yaml.safe_load(access_details)
        with open(config_file_2, "r") as job_details:    
            job_details=yaml.safe_load(job_details)
        with open(config_file_3, "r") as destination_details: 
            destination_details = yaml.safe_load(destination_details)
        with open(application_file, "r") as application_details: 
            application_details = yaml.safe_load(application_details)
        return access_details,job_details,destination_details,application_details
        



    def read_data_source(self):
        logging.info("Connecting to data source")
        data_source = datasourcefactory.read_data_source(self.spark, self.source_details, self.job_config,self.application_details)
        logging.info("Reading data....")

        try:
            for i in range(1, 11):
                logging.info(f"Reading {i * 10}% completed...")
                # Stimulate delay in progress
                self._simulate_delay()

                # Send progress update to Kafka
                progress_message = f"Reading {i * 10}% completed..."
                self.send_to_kafka(progress_message)

            logging.info("Data reading completed...")
            return data_source.read_data()
        except Exception as e:
            logging.error(f"Error occurred during data reading: {e}")
            raise e

    def _simulate_delay(self):
        #simulate delay to show progress
        import time
        time.sleep(1)

    def write_to_destination(self,interpreted_df,destination_details):
        file_format = destination_details['format']
        bucket = destination_details['bucket']
        path = destination_details['path']
        name = destination_details['name']

        output_path = f"{path}/{name}"

        logging.info("Writing data to destination....")

        if file_format == 'parquet':
            interpreted_df.write.parquet(f"gs://{bucket}/{output_path}")

        elif file_format == 'csv':
            interpreted_df.write.csv(f"gs://{bucket}/{output_path}")
        elif file_format == 'jason':
            interpreted_df.write.jason(f"gs://{bucket}/{output_path}")
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        for i in range(1,11):
            logging.info(f"writing {i*10}% completed...")
            #Stimulate delay in progress
            self._simulate_delay()
        
        logging.info("Data writting completed")        

        
    
    def migrate_data(self,source_details,job_config,destination_details,application_details):
        # Read data from source
        df = self.read_data_source(source_details,job_config)

        # interpret job configuration and do transformation
        job_interpreter = JobInterpreter(job_config)
        interpreted_df = job_interpreter.interpreter(df)

        # Perform data migration
        self.write_to_destination(interpreted_df, destination_details)






    
