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

    def avg_marks_and_attendance_by_major_year(self):
        result = self.df.groupby(['major', 'year']).agg({
            'marks': 'mean',
            'attendance_percentage': 'mean'
        }).reset_index()
        return result.to_dict(orient='records')

    def passing_percentage_by_subject_year(self):
        df = self.df.copy()
        df['passed_flag'] = df['passed'].apply(lambda x: 1 if x.lower() == 'yes' else 0)
        grouped = df.groupby(['subject', 'year'])
        result = grouped['passed_flag'].mean().reset_index()
        result['passing_percentage'] = result['passed_flag'] * 100
        return result.drop(columns=['passed_flag']).to_dict(orient='records')

    def top_majors_under_18_by_avg_marks(self, top_n=3):
        filtered = self.df[self.df['age'] < 18]
        result = filtered.groupby('major')['marks'].mean().reset_index()
        return result.sort_values(by='marks', ascending=False).head(top_n).to_dict(orient='records')
    
    def average_age_and_attendance_by_gender_year(self):
        grouped = self.df.groupby(['gender', 'year']).agg(
            avg_age=('age', 'mean'),
            avg_attendance=('attendance_percentage', 'mean'),
            student_count=('student_id', 'count')
        ).reset_index()
        return grouped.to_dict(orient='records')

    def subject_performance_distribution_by_major(self):
        grouped = self.df.groupby(['major', 'subject']).agg(
            avg_marks=('marks', 'mean'),
            min_marks=('marks', 'min'),
            max_marks=('marks', 'max'),
            student_count=('student_id', 'count')
        ).reset_index()
        return grouped.to_dict(orient='records')

