from cs230_common.file_transfer_client import FileTransferClient

host_ip = "169.234.56.23"


def main():
    FTPServer = FileTransferClient(
        host=host_ip, port=21, username="kunwp1", password="test"
    )
    FTPServer.push_file(1, ["messenger_example.py"])
    FTPServer.fetch_file(1)
    FTPServer.list_files("1")


if __name__ == "__main__":
    main()
