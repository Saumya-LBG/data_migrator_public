import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import *

class datasourcefactory:
    @staticmethod
    def read_data_source(spark, source_details, job_config):
        source_type = source_details['type']

        if source_type == 'database':
            return DatabaseDataSource(spark, source_details, job_config)
        elif source_type == 'filesystem':
            return FileSystemDataSource(spark, source_details, job_config)
        elif source_type in ['image', 'video']:
            return ImageOrVideoDataSource(spark, source_details, job_config)
        else:
            raise ValueError(f"Unsupported file type: {source_type}")

class DataSource:
    def __init__(self, spark, source_details):
        self.spark = spark
        self.source_details = source_details

    def read_data(self):
        raise NotImplementedError("Subclasses must implement read_data method")

class DatabaseDataSource(DataSource):
    def __init__(self, spark, source_details, job_config):
        super().__init__(spark, source_details)
        self.job_config = job_config

    def read_data(self, secrets):
        jdbc_url = self.source_details['url']
        table_name = self.job_config['name']
        schema = self.job_config.get('schema', 'public')
        predicates = self.job_config.get('predicates', [])
        username = secrets.get('username', '')
        password = secrets.get('password', '')

        df = self.spark.read.format('jdbc') \
            .option("url", jdbc_url) \
            .option("dbtable", f"{schema}.{table_name}") \
            .option("user", username) \
            .option("password", password) \
            .load()

        return df

class FileSystemDataSource(DataSource):
    def __init__(self, spark, source_details, job_config):
        super().__init__(spark, source_details)
        self.job_config = job_config

    def read_data(self):
        file_path = self.source_details['path']
        file_type = self.source_details['file_type']

        if file_type == 'csv':
            return self.spark.read.csv(file_path, header=True, inferSchema=True)
        elif file_type == 'json':
            return self.spark.read.json(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

class DataMigrator:
    def __init__(self, spark):
        self.spark = spark

    def read_config_file(self, file_path):
        with open(file_path, "r") as file:
            config = yaml.safe_load(file)
            return config

    def read_data_source(self, source_details, job_config):
        data_source = datasourcefactory.read_data_source(self.spark, source_details, job_config)
        return data_source.read_data()

    def df_transformation(self, df, source_details):
        # Placeholder for any transformations
        return df

    def write_to_destination(self, df, destination_details):
        file_format = destination_details.get('format', 'csv')
        path = destination_details.get('path', '')
        name = destination_details.get('name', '')

        if not path:
            raise ValueError("Destination path is required in the configuration.")

        output_path = os.path.join(path, name) if name else path

        if file_format == 'parquet':
            df.write.parquet(output_path)
        elif file_format == 'csv':
            df.write.csv(output_path)
        elif file_format == 'json':
            df.write.json(output_path)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

    def migrate_data(self, source_details, job_config, destination_details):
        df = self.read_data_source(source_details, job_config)
        df = self.df_transformation(df, source_details)
        self.write_to_destination(df, destination_details)

if __name__ == "__main__":
    config_file = "sample.yaml"
    job_name = os.path.splitext(os.path.basename(config_file))[0]
    spark = SparkSession.builder \
        .appName(job_name) \
        .getOrCreate()

    migrator = DataMigrator(spark)
    config = migrator.read_config_file(config_file)
    source_details = config.get('source', {})
    job_config = config.get('job', {})
    destination_details = config.get('destination', {})
    migrator.migrate_data(source_details, job_config, destination_details)
