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
            print(row)

    @profile_resources
    def create_data_files_per_year(self):
        self.df.create_data_files_per_year()

    @profile_resources
    def query(self):
        for each_func in [
            self.df.avg_marks_and_attendance_by_major_year,
            self.df.passing_percentage_by_subject_year,
            self.df.top_majors_under_18_by_avg_marks,
            self.df.average_age_and_attendance_by_gender_year,
            self.df.subject_performance_distribution_by_major
        ]:
            dataset = each_func()
            print(type(dataset))


if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.read_rows()
    pipeline.create_data_files_per_year()
    pipeline.query()