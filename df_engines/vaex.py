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

    def avg_marks_and_attendance_by_major_year(self):
        df = self.df.groupby(['major', 'year'], agg={
            'marks_mean': vaex.agg.mean('marks'),
            'attendance_mean': vaex.agg.mean('attendance_percentage')
        })
        return df.to_items()

    def passing_percentage_by_subject_year(self):
        passed_flag = self.df['passed'].str.lower() == 'yes'
        df = self.df.groupby(['subject', 'year'], agg={
            'passing_rate': vaex.agg.mean(passed_flag)
        })
        df['passing_percentage'] = df['passing_rate'] * 100
        return df.drop('passing_rate').to_items()

    def top_majors_under_18_by_avg_marks(self, top_n=3):
        filtered = self.df[self.df['age'] < 18]
        df = filtered.groupby('major', agg={
            'marks_mean': vaex.agg.mean('marks')
        })
        sorted_df = df.sort('marks_mean', ascending=False)
        return sorted_df.head(top_n).to_items()
