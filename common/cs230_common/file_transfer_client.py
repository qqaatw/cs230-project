import os
import ftplib
import socket
import zipfile
import sys
import logging

LOGGER = logging.getLogger(__name__)


class FileTransferClient:
    def __init__(self, host: str, port: int, username: str, password: str):
        self.ftp = ftplib.FTP()
        try:
            # connect and login to the FTP server
            self.ftp.connect(host, port)
            self.ftp.login(username, password)
            self.ftp.cwd(username)
        except (socket.error, ftplib.error_perm) as e:
            LOGGER.debug("Error:", e)
            self.ftp.close()
            raise e
        finally:
            LOGGER.debug("Constructor called, FTP connection established.")

    def __del__(self):
        self.ftp.quit()
        LOGGER.debug("Destructor called, FTP connection closed.")

    """
    1. Fetch the zip file [task_id]/[task_id].zip from the FTP server
    2. Create a new folder name [task_id] and unzip the zip file into that folder
    """

    def fetch_file(self, task_id: int):
        zip_filename = str(task_id) + ".zip"
        ftp_zipfile_path = os.path.join(str(task_id), zip_filename)
        if sys.platform.startswith("win"):
            ftp_zipfile_path = str(task_id) + "/" + zip_filename
        local_file = open(zip_filename, "wb")

        try:
            # retrieve the remote file and write it to the local file
            self.ftp.retrbinary(f"RETR {ftp_zipfile_path}", local_file.write)
        except ftplib.error_perm as e:
            LOGGER.debug("Error:", e)
            local_file.close()
            os.remove(zip_filename)
            raise e
        finally:
            local_file.close()
            LOGGER.debug("File fetched successfully:", ftp_zipfile_path)

        folder_name = str(task_id)
        os.mkdir(folder_name)  # Assume the folder doesn't exist

        try:
            # open the zip file and extract its contents to the folder
            with zipfile.ZipFile(zip_filename, "r") as zip_file:
                zip_file.extractall(folder_name)
        except zipfile.BadZipFile as e:
            LOGGER.debug("Error:", e)
            os.rmdir(folder_name)
            raise e
        finally:
            LOGGER.debug("File unzipped successfully:", zip_filename)

        os.remove(zip_filename)

    """
    1. Create a zip file using the file_list
    2. Create a [task_id] folder in the FTP server and send the zip file to that folder
    """

    def push_file(self, task_id: int, file_list: list):
        for file in file_list:
            if not isinstance(file, str) or not file:
                raise ValueError("Invalid file name in file_list")

        zip_filename = str(task_id) + ".zip"
        zip_file = zipfile.ZipFile(zip_filename, "w")

        try:
            # add each file from the file_list to the zip file
            for file in file_list:
                zip_file.write(file)
        except (OSError, zipfile.BadZipFile) as e:
            LOGGER.debug("Error:", e)
            zip_file.close()
            os.remove(zip_filename)
            raise e
        finally:
            zip_file.close()
            LOGGER.debug("File zipped successfully:", zip_filename)

        remote_file = open(zip_filename, "rb")
        ftp_zipfile_path = os.path.join(str(task_id), zip_filename)
        if sys.platform.startswith("win"):
            ftp_zipfile_path = str(task_id) + "/" + zip_filename

        try:
            # send the zip file to the FTP server
            self.ftp.mkd(str(task_id))
            self.ftp.storbinary(f"STOR {ftp_zipfile_path}", remote_file)
        except ftplib.error_perm as e:
            LOGGER.debug("Error:", e)
            remote_file.close()
            raise e
        finally:
            remote_file.close()
            LOGGER.debug("File pushed successfully:", ftp_zipfile_path)

        os.remove(zip_filename)
        LOGGER.debug("File removed successfully:", zip_filename)

    def list_files(self, path: str):
        files = self.ftp.nlst(path)
        for file in files:
            LOGGER.debug(file)
        return files

    def erase_files(self, path: str = "/kunwp1"):
        def is_directory(ftp, name):
            try:
                ftp.cwd(name)
                ftp.cwd("..")  # Move back to the parent directory
                return True
            except:
                return False

        def remove_directory_recursive(ftp, directory):
            # List the directory contents
            files = ftp.nlst(directory)

            # Remove files and subdirectories
            for item in files:
                LOGGER.debug(f"Deleting {item}")
                if is_directory(ftp, item):  # If item is a directory, recurse
                    remove_directory_recursive(ftp, item)
                else:
                    ftp.delete(item)  # Remove file

            # Remove the target directory
            if is_directory(ftp, directory):
                ftp.rmd(directory)

        for dir in self.list_files(path):
            remove_directory_recursive(self.ftp, dir)

    def upload_results(self, task_id: int, filenames: list):
        results_path = os.path.join(str(task_id), "results")
        if sys.platform.startswith("win"):
            results_path = str(task_id) + "/" + "results"

        if results_path not in self.ftp.nlst(str(task_id)):
            self.ftp.mkd(results_path)

        for filename in filenames:
            final_path = os.path.join(results_path, filename)
            if sys.platform.startswith("win"):
                final_path = results_path + "/" + filename
            try:
                file = open(filename, "rb")
                self.ftp.storbinary(f"STOR {final_path}", file)
            except Exception as e:
                LOGGER.debug(f"An error occurred while uploading {filename}: {e}")
            finally:
                file.close()
