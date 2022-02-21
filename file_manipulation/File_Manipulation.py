from logger.logger import Logger
import wget
import os
"""module to implement File Manipulation functions."""

class File_Manipulation:
    __log_obj = Logger(filename="Logs\\File_Manipulation.log")

    @staticmethod
    def __cleanup(filepath):
        try:
            """function that implements clean up so that duplicate files are not downloaded."""
            os.remove(filepath)
            File_Manipulation.__log_obj.add_log("Clean up done")
        except Exception as e:
            File_Manipulation.__log_obj.add_log("File doesn't exist right now")
            File_Manipulation.__log_obj.add_log(str(e))

    def __init__(self, url, file_path):
        self.url = url
        self.file_path = file_path

    def Download_file(self):
        try:
            """function responsible for downloading the files."""
            # clean up
            File_Manipulation.__cleanup(self.file_path)

            # download file

            wget.download(self.url, self.file_path)
            File_Manipulation.__log_obj.add_log("File downloaded successfully.")

        except Exception as e:
            File_Manipulation.__log_obj.add_log(str(e))

    def create_data_dictionary_list(self):
        try:

            """function that helps in creating data dictionary list."""
            file_obj = open(self.file_path, "r")
            data = file_obj.readlines()
            keys = data[0].replace('\n', "").split(";")

            data.pop(0)

            data_Dict_list = []  # list storing data dictionaries

            for row in data:
                row = row.replace("\n", "").split(";")

                data_dict = {}
                for i in range(len(keys)):
                    data_dict[keys[i]] = row[i]
                data_Dict_list.append(data_dict)

            file_obj.close()
            File_Manipulation.__log_obj.add_log("data dictionary list created")
            return data_Dict_list

        except Exception as e:
            File_Manipulation.__log_obj.add_log("Problems in creating data dictionary list")
            File_Manipulation.__log_obj.add_log(str(e))
