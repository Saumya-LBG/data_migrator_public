class JobInterpreter:
    def __init__(self,job_config):
        self.job_config=job_config


    def interpreter(self,df):
        #Implement logic for transformation 
        #for example apply filters selects, or joins
        # example is given below
        interpreted_df= df.filter(df['Year']>2006)# Applying a filter based on Year
        return interpreted_df
