import yaml


class AppConfig():
    def __init__(self, file_name):
        with open(file_name) as f:
            self.config = yaml.safe_load(f)
        self.database_user = self.config["database_user"]
        self.database_password = self.config["database_password"]
        self.database_name = self.config["database_name"]
        self.database_host = self.config["database_host"]
        self.database_port = self.config["database_port"]
        self.table_index_values_name = self.config["table_index_values_name"]
        self.data_file_folder = self.config["data_file_folder"]


class DataFileContainer():
    def __init__(self, file_name):
        with open(file_name) as f:
            self.yaml_file = yaml.safe_load(f)
        self.index_value_files = self.yaml_file["index_data"]
