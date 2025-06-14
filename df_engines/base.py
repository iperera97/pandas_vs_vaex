
class Dataframe:
    NAME = None

    def __init__(self, data_file: str):
        raise NotImplementedError("Subclasses must implement this method")
    
    def iterate_records(self):
        raise NotImplementedError
    
    def create_data_files_per_year(self):
        raise NotImplementedError
    
    def matrix_table_query(self):
        raise NotImplementedError

    def filter_dataset_query(self):
        raise NotImplementedError

    def group_by_chart_query(self, top_n=3):
        raise NotImplementedError
    
    def group_by_with_where_query(self, top_n=3):
        raise NotImplementedError