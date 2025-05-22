
import os


class LogReader:
    """
        This class is for reading raw logs.
    """
    def __init__(self, dir_path):

        self.idx = 0
        self.dir_path = dir_path


    def read_file(self, batch_size) -> list:
        """
        Read all csv and txt files in the directory and return lines of logs

        :param batch_size:
        :return: list of log strings
        """

        file_list = os.listdir(self.dir_path)



        for file_name in file_list:
            file_path = os.path.join(self.dir_path, file_name)
            with open(file_path, mode='r') as file:
                batch_list = []

                for line in file:
                    batch_list.append(line)

                    if len(batch_list) == batch_size:
                        print("batch list : ", batch_list)
                        yield batch_list
                        batch_list = []

                if batch_list:
                    yield batch_list
