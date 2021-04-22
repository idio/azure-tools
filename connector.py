# from typing import Container, Iterable
# from utils import get_blob_storage_url, parse_azure_path

from args_handler import arguments_decorator, multi_arguments_decorator
from azure.identity._credentials.default import DefaultAzureCredential

# from azure.storage import blob
from azure.storage.blob import BlobServiceClient, ContainerClient

# from azure.common.client_factory import get_client_from_cli_profile
# from azure.identity import DefaultAzureCredential
# from six import Iterator
import logging
import re
import os


class Connector:
    def __init__(self, path=None, storage_account=None, container=None):

        logging.basicConfig(level=logging.INFO)

        self.storage_account = storage_account
        self.container = container

        if path:
            parsed_path = self.parse_azure_path(path)
            self.storage_account = parsed_path["storage_account"]
            self.container = parsed_path["container"]

        # Gets credential from azure cli
        self.credential = DefaultAzureCredential()

        # Create class wide storage account and container clients if names are passed
        if self.storage_account:
            blob_storage_url = self.get_blob_storage_url(storage_account=self.storage_account)
            self.blob_service_client = BlobServiceClient(
                credential=self.credential, account_url=blob_storage_url
            )
            if self.container:
                container_names = [
                    container.name
                    for container in self.blob_service_client.list_containers()
                ]
                if self.container in container_names:
                    self.container_client = (
                        self.blob_service_client.get_container_client(
                            container=self.container
                        )
                    )
                else:
                    raise ValueError(
                        f"The container: {self.container} is not in the storage account: {self.storage_account}"
                    )

    @arguments_decorator()
    def get_blob_storage_url(
        self,
        path: str = None,
        storage_account: str = None,
        container: str = None,
        file_path: str = None,
    ) -> str:
        """
        Returns the storage account url for the path or storage_account name passed

        :param path: str: optional An azure path. Defaults to None.
        :param storage_account: str: optional Storage account name. Defaults to None.
        :param container: str: optional Ignored. Defaults to None.
        :param file_path: str: optional Ignored. Defaults to None.

        :return str: The storage account url in the form: https://{storage_account}.blob.core.windows.net/
        """
        return f"https://{storage_account}.blob.core.windows.net/"

    def parse_azure_path(self, path: str) -> dict:
        """
        Parse an azure url into : storage_account, container and filepath.
        If passing a url of the for azure://container/filepath the storage account is
        taken from the class instance. If there is no storage account passed for the class
        the storage account will be None.

        :param path: str: The azure blob path
        :return: dict: A dictionary containing the container name and filepath
        """
        storage_account = self.storage_account
        container = self.container

        if path.startswith("https://"):
            storage_account = re.findall(
                r"https://(.*)\.blob\.core\.windows\.net", path
            )[0]
            path = path.replace(f"https://{storage_account}.blob.core.windows.net/", "")
            split_path = path.split("/")
            container = split_path.pop(0)
            filepath = "/".join(split_path)

        elif path.startswith("azure://"):
            path = path.replace("azure://", "")
            split_path = path.split("/")
            container = split_path.pop(0)
            filepath = "/".join(split_path)

        else:
            filepath = path
        return {
            "storage_account": storage_account,
            "container": container,
            "file_path": filepath,
        }
    
    def is_azure_path(self, path: str) -> bool:
        """
        Returns true if the path is of a recognised azure path format

        :param path: str: The path to test

        :return bool: True if path is of an accepted azure path format
        """        
        patterns = [
            r"https://.*\.blob.core.windows.net",
            r"azure://"
        ]
        return any([bool(re.match(p, path)) for p in patterns])

    @arguments_decorator()
    def get_blob_service_client(
        self,
        path: str = None,
        storage_account: str = None,
        container: str = None,
        file_path: str = None,
    ) -> BlobServiceClient:
        """
        Returns a blob service client for the specified storage account. If no parameters are passed the class values are used

        :param path: str: optional An azure path, the storage account will be used to create a client. Defaults to None.
        :param storage_account: str: optional The name of the storage account to create a client for. Defaults to None.
        :param container: str: optional Ignored. Defaults to None.
        :param file_path: str: optional Ignored. Defaults to None.

        :return BlobServiceClient: An azure blobserviceclient for the specified storage account
        """
        if storage_account == self.storage_account:
            return self.blob_service_client
        else:
            blob_storage_url = self.get_blob_storage_url(
                storage_account=storage_account
            )
            return BlobServiceClient(
                credential=self.credential, account_url=blob_storage_url
            )

    @arguments_decorator()
    def get_container_client(
        self,
        path: str = None,
        storage_account: str = None,
        container: str = None,
        file_path: str = None,
    ) -> ContainerClient:
        """
        Returns a container client when a container name in the storage account is passed. If no params are passed the class values will be used

        :param path: str: optional An Azure path, the container in the path will be used. Defaults to None.
        :param storage_account: str: optional A storage account name containing the container. Defaults to None.
        :param container: str: optional The name of the container to create a client for. Defaults to None.
        :param file_path: str: optional The file path will ultimately be ignored. Defaults to None.

        :exception ValueError: Raised if the container does not exist in the storage account

        :return ContainerClient: An Azure client for the container
        """
        if storage_account == self.storage_account and container == self.container:
            return self.container_client
        else:
            client = self.get_blob_service_client(storage_account=storage_account)
            container_names = [container.name for container in client.list_containers()]
            if container in container_names:
                return client.get_container_client(container=container)
            else:
                raise ValueError(
                    f"The container: {container} is not in the storage account: {storage_account}"
                )

    @arguments_decorator()
    def list_blobs(
        self,
        path: str = None,
        storage_account: str = None,
        container: str = None,
        file_path: str = None,
    ) -> list:
        """
        Returns a list of blobs, with paths that match the path passed

        :param path: str: optional An azure path to search for blobs. Defaults to None.
        :param storage_account: str: optional storage account name. Defaults to None.
        :param container: str: optional container name. Defaults to None.
        :param file_path: str: optional the prefix file path. Defaults to None.

        :return list: Blobs in the path passed
        """
        container_client = self.get_container_client(
            storage_account=storage_account, container=container
        )
        if file_path:
            blob_iter = container_client.list_blobs(name_starts_with=file_path)
            return [blob.name.replace(file_path, "") for blob in blob_iter]
        else:
            blob_iter = container_client.list_blobs()
            return [blob.name for blob in blob_iter]
    
    @multi_arguments_decorator(local_support=True)
    def download_folder(
        self,
        source_path: str = None,
        source_storage_account: str = None,
        source_container: str = None,
        source_file_path: str = None,
        dest_path: str = None,
        dest_storage_account: str = None,
        dest_container: str = None,
        dest_file_path: str = None,
    ):
        """
        Copy a folder from azure to a local path

        :param source_path: str: optional An Azure path to the folder to download. Defaults to None.
        :param source_storage_account: str: optional The storage account name. Defaults to None.
        :param source_container: str: optional The container name. Defaults to None.
        :param source_file_path: str: optional The path to the folder to download. Defaults to None.
        :param dest_path: str: optional The local path to download the folder to. Defaults to None.
        :param dest_storage_account: str: optional Ignored. Defaults to None.
        :param dest_container: str: optional Ignored. Defaults to None.
        :param dest_file_path: str: optional Ignored. Defaults to None.

        :exception ValueError: Raised when destination path is an azure path
        """    
        container_client = self.get_container_client(
            storage_account=source_storage_account, container=source_container
        )

        if self.is_azure_path(dest_path): raise ValueError(f"Expected destination to be local path got azure path: {dest_path}")
        os.makedirs(dest_path, exist_ok=True)

        for blob in container_client.list_blobs(source_file_path):
            file_name = os.path.basename(blob.name)
            local_path = os.path.join(dest_path, file_name)
            with open(local_path, "wb") as f:
                logging.info(f"Downloading {blob.name} to {local_path}")
                blob_data = container_client.download_blob(blob.name)
                blob_data.readinto(f)
            logging.info("Completed Download")
    
    @arguments_decorator()
    def blob_exists(
        self,
        path: str = None,
        storage_account: str = None,
        container: str = None,
        file_path: str = None,
    ):
        """
        Checks if a file exists in azure, return bool

        :param path: str: optional Azure path to file to check. Defaults to None.
        :param storage_account: str: optional Storage account. Defaults to None.
        :param container: str: optional Container. Defaults to None.
        :param file_path: str: optional path to file. Defaults to None.

        :return [bool]: True if file exists
        """        
        
        client = self.get_blob_service_client(storage_account=storage_account)
        blob_client = client.get_blob_client(container, file_path)
        return blob_client.exists()
    
    @multi_arguments_decorator(local_support=True)
    def upload_folder(
        self,
        source_path: str = None,
        source_storage_account: str = None,
        source_container: str = None,
        source_file_path: str = None,
        dest_path: str = None,
        dest_storage_account: str = None,
        dest_container: str = None,
        dest_file_path: str = None,
    ):
        """
        Upload a directory to an azure location. Subdirectories are not currently supported

        :param source_path: str: optional Local path to folder to upload. Defaults to None.
        :param source_storage_account: str: optional Ignored. Defaults to None.
        :param source_container: str: optional Ignored. Defaults to None.
        :param source_file_path: str: optional Ignored. Defaults to None.
        :param dest_path: str: optional Azure path to upload to. Defaults to None.
        :param dest_storage_account: str: optional Storage account. Defaults to None.
        :param dest_container: str: optional Container name. Defaults to None.
        :param dest_file_path: str: optional Path to folder. Defaults to None.

        :exception ValueError: Raised if source is an Azure path
        """    
        if self.is_azure_path(source_path): raise ValueError(f"Expected destination to be local path got azure path: {source_path}")

        container_client = self.get_container_client(storage_account=dest_storage_account, container=dest_container)

        for root, dirs, files in os.walk(source_path):
            logging.warning("upload folder does not support sub-directories only files will be uploaded")
            for file in files:
                file_path = os.path.join(root, file)
                blob_path = dest_file_path + file

                logging.info(f"Uploading {file_path} to {blob_path}")
                with open(file_path, "rb") as data:
                    container_client.upload_blob(name=blob_path, data=data)
    
    # @arguments_decorator(local_support=True)
    # def open(
    #     self,
    #     path: str = None,
    #     storage_account: str = None,
    #     container: str = None,
    #     file_path: str = None,
    #     mode="r", 
    #     *args, **kwargs
    #     ):
    #     """
    #     wrapper around smart_open so we dont have to pass a blob client everywhere.
    #     """
    #     if not path.startswith("azure://") and "w" in mode:
    #         # if it is local write mode, check the path and create folder if needed
    #         subdir = os.path.dirname(path)
    #         if subdir:
    #             os.makedirs(subdir, exist_ok=True)
    #     transport_params = {"client": get_or_create_blob_client()}
    #     if "transport_params" not in kwargs:
    #         kwargs["transport_params"] = transport_params
    #     return smart_open.open(path, mode, *args, **kwargs)
