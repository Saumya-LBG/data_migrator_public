
class datasourcefactory:
    @staticmethod
    def read_data_source(self,spark,source_details,job_config):
        source_type = source_details['type']

        if source_type =='database':
            return self.DatabaseDataSource(spark,source_details,job_config)
        elif source_type == 'filesyatem' :
            return self.FileSystemDataSource(spark,source_details,job_config)
        elif source_type in ['image','video']:
            return self.ImageOrVidoDataSource(spark,source_details,job_config)
        else:
            raise ValueError(f"Unsupported file type:{source_type}")
        

class DataSource:
    def __init__(self,spark,source_details):
        self.spark = spark
        self.source_details= source_details


    def read_data(self):
        raise NotImplementedError("Subclasses must implement read_data method")
    
class DatabaseDataSource(DataSource):
    def __init__(self,spark,source_details,job_config):
        super().__init__(spark, source_details)
        self.job_config=job_config

    def read_data(self,source_details,job_config,secrets): 
        jdbc_url=source_details['url']
        table_name = job_config['name']
        schema = job_config.get('schema','public')
        predicates = job_config.get('predicates',[])
        username =secrets.get('username','')
        password=secrets.get('password', '')

        df =self.spark.read.format('jdbc') \
        .option("url", jdbc_url) \
        .option("dbtanle", f"{schema}.{table_name}") \
        .option("user", username) \
        .option("password", password) \
        .load

        return df
    
class FileSystemDataSource(DataSource):
    def read_data(self,source_details):
        file_path = source_details['path']
        file_type = source_details['file_type']

        if file_type == 'csv':
            return self.spark.read.csv(file_path,header=True,inferSchema=True)
        elif file_type == 'jason':
            return self.spark.read.jason(file_path)
        elif file_type == 'parquet':
            return self.spark.read.parquet(file_path)
        else:
            raise ValueError(f"Unsupported file type:{file_type}")
        
class ImageOrVidoDataSource(DataSource):
    def read_image_or_video(self,df,source_details):
        # Implement logic to read image file or video file
        pass