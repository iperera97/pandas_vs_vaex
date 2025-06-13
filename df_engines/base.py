
class Dataframe:
    NAME = None

    def __init__(self, data_file: str):
        raise NotImplementedError("Subclasses must implement this method")
    
    def iterate_records(self):
        raise NotImplementedError
    
    def create_data_files_per_year(self):
        raise NotImplementedError
    
    def avg_marks_and_attendance_by_major_year(self):
        raise NotImplementedError

    def passing_percentage_by_subject_year(self):
        raise NotImplementedError

    def top_majors_under_18_by_avg_marks(self, top_n=3):
        raise NotImplementedError