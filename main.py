import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from data_sources import *
from job_interpreter import *
from data_migrator import *


def main(config_file):
    #Extract job name from the configuration file name
    job_name = os.path.splittext(config_file)[0]

    # Initialize spark Session with th job name

    spark = SparkSession.builder \
    .appNAME(job_name) \
    .getOrCreate()

    # Initialize DataMigrator
    migrator = DataMigrator(spark)

    # Read config file
    config =migrator.read_config_file(config_file)

    # Extract configuration file deatils
    secrets= config.get('secrets',{})
    source_details = config.get('secrets',{})
    job_config = config.get('job-configuration', {})
    destination_details = config.get('destination',{})


    # read and transfom the data
    migrator.migrate_data(source_details,job_config)


    # Stop Spark Session
    spark.stop()

    if __name__ == "__main__":
        #Hard coded configuration file for now
        config_file = "sample.yml"
        main(config_file)
