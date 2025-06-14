import pandas as pd
from .base import Dataframe

class PandasDataframe(Dataframe):
    NAME = "Pandas"

    def __init__(self, data_file: str):
        self.df = pd.read_csv(data_file)
    
    def iterate_records(self):
        for index, row in self.df.iterrows():
            yield index, row

    def create_data_files_per_year(self):
        for year, group_df in self.df.groupby("year"):
            filename = f"students_{year}.parquet"
            group_df.to_parquet(filename, index=False)
            print(f"Written: {filename}")

    def matrix_table_query(self):
        df = self.df.groupby(['major', 'subject'])['student_id'].count().reset_index()
        df = df.rename(columns={'student_id': 'count'})
        return df.head(200).to_dict(orient='records')

    def filter_dataset_query(self):
        df = self.df[
            (self.df['marks'] > 85) &
            (self.df['attendance_percentage'] > 95) &
            (self.df['age'] < 18)
        ]
        return df.head(100).to_dict(orient='records')

    def group_by_query(self):
        df = self.df.groupby('subject')['marks'].mean().reset_index()
        return df.rename(columns={'marks': 'avg_marks'}).to_dict(orient='records')

    def group_by_with_where_query(self):
        df = self.df[self.df['year'].isin([2020, 2021, 2022])]
        result = df.groupby('subject').agg(
            avg_marks=('marks', 'mean'),
            avg_attendance=('attendance_percentage', 'mean'),
            student_count=('student_id', 'count')
        ).reset_index()
        return result.to_dict(orient='records')