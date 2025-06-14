import vaex
from .base import Dataframe

class VaexDataframe(Dataframe):
    NAME = "Vaex"

    def __init__(self, data_file: str):
        self.df = vaex.open(data_file)

    def iterate_records(self):
        for index, row in self.df.iterrows():
            yield index, row

    def create_data_files_per_year(self):
        for year, group_df in self.df.groupby("year"):
            filename = f"students_{year}.parquet"
            group_df.export_parquet(filename, progress=True)
            print(f"Written: {filename}")

    def matrix_table_query(self):
        df = self.df.groupby(['major', 'subject'], agg={
            'count': vaex.agg.count('student_id')
        })
        return df.to_pandas_df().head(200).to_items()

    def filter_dataset_query(self):
        df = self.df[
            (self.df.marks > 85) &
            (self.df.attendance_percentage > 95) &
            (self.df.age < 18)
        ]
        return df.head(100).to_items()

    def group_by_query(self):
        df = self.df.groupby('subject', agg={
            'avg_marks': vaex.agg.mean('marks')
        })
        return df.to_items()

    def group_by_with_where_query(self):
        df_filtered = self.df[self.df.year.isin([2020, 2021, 2022])]
        df = df_filtered.groupby('subject', agg={
            'avg_marks': vaex.agg.mean('marks'),
            'avg_attendance': vaex.agg.mean('attendance_percentage'),
            'student_count': vaex.agg.count('student_id')
        })
        return df.to_items()