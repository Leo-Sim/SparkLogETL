import yaml


class Config:
    def __init__(self, config_path=None):

        if config_path is None:
            config_path = '../config.yaml'

        self.config_path = config_path
        self.config = self._load_yaml()

    def _load_yaml(self):
        with open(self.config_path, "r") as file:
            try:
                config = yaml.safe_load(file)
                return config
            except yaml.YAMLError as e:
                print(f"Error loading YAML file: {e}")
                return {}

    def _get_reader(self):
        return self.config.get("reader")

    def get_reader_dir_path(self):
        return self._get_reader().get("dir-path")


    def _get_connector(self):
        return self.config.get("connector")

    def get_server_address(self):
        return self._get_connector().get("server-address")