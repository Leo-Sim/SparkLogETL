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


    # config malicious attacks

    def _get_attack(self):
        return self.config.get("attack")

    def _get_brute_force(self):
        return self._get_attack().get("brute-force")

    def get_brute_force_login_count_threshold(self):
        return self._get_brute_force().get("login-count-threshold")

    def get_brute_force_time_gap_threshold(self):
        return self._get_brute_force().get("time-gap-threshold")