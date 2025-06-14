import os
from df_engines.pandas import PandasDataframe
from df_engines.vaex import VaexDataframe
from utils import profile_resources


class DataPipeline:
    FILE_PATH = "student_dataset.csv"

    def __init__(self):
        self.df = self.get_df()
    
    @profile_resources
    def get_df(self):
        DF_ENGINE = os.getenv("DF_ENGINE")
        print(DF_ENGINE)
        
        if DF_ENGINE == PandasDataframe.NAME:
            return PandasDataframe(self.FILE_PATH)
        
        if DF_ENGINE == VaexDataframe.NAME:
            return VaexDataframe(self.FILE_PATH)
        
        raise ValueError("DF ENGINE NOT FOUND")
    
    @profile_resources
    def read_rows(self):
        for row in self.df.iterate_records():
            ...

    @profile_resources
    def create_data_files_per_year(self):
        self.df.create_data_files_per_year()

    @profile_resources
    def matrix_table_query(self):
        dataset = self.df.matrix_table_query()

    @profile_resources
    def filter_dataset_query(self):
        dataset = self.df.filter_dataset_query()

    @profile_resources
    def group_by_chart_query(self):
        dataset = self.df.group_by_chart_query()

    @profile_resources
    def group_by_with_where_query(self):
        dataset = self.df.group_by_with_where_query()


if __name__ == "__main__":
    pipeline = DataPipeline()

    pipeline.read_rows()

    pipeline.create_data_files_per_year()

    pipeline.matrix_table_query()
    pipeline.filter_dataset_query()
    pipeline.group_by_chart_query()
    pipeline.group_by_with_where_query()