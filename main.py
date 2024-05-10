import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from data_sources import *
from job_interpreter import *
from data_migrator import *


def main(config_file_1,config_file_2,config_file_3,application_file):
    #Extract job name from the configuration file name
    job_name = os.path.splitext(config_file_2)[0].split('_')[0]

    # Initialize spark Session with th job name

    spark = SparkSession.builder \
    .appNAME(job_name) \
    .getOrCreate()

    # Initialize DataMigrator
    migrator = DataMigrator(spark)

    # Read config file
    config =migrator.read_config_file(config_file_1,config_file_2,config_file_3,application_file)

    # Extract configuration file deatils
    source_details = config.get('secrets',{})
    job_config = config.get('job-configuration', {})
    destination_details = config.get('destination',{})
    application_details=config.get("log_file_path","Enviromemt_deatils",{})


    # read and transfom the data
    migrator.migrate_data(source_details,job_config,destination_details,application_details)


    # Stop Spark Session
    spark.stop()

    if __name__ == "__main__":
        #Hard coded configuration file for now
        config_file_1 = "extract2_accesss_data.yaml"
        config_file_2 = "extarct2_job_configuartions.yaml"  # Configuration file path
        config_file_3 = "extract2_destination_details.yaml"  # Configuration file path
        application_file = "application.properties"   # application file path
        main(config_file_1,config_file_3,config_file_2,application_file)
