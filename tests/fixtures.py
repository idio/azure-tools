import pytest
from unittest.mock import patch
from aztools.args_handler import arguments_decorator, multi_arguments_decorator
from aztools.storage import *


@pytest.fixture
def patched_creds_connector():
    """
    For this connector just the DefaultAzureCredential class is patched, allowing for mocking
    of the other azure classes specifically for the test
    The connector class returned can be intialised with path, storage_account, container just like normal.

    :param path: str: optional An azure path. Defaults to None.
    :param storage_account: str: optional Storage account name. Defaults to None.
    :param container: str: optional A container int he storage account. Defaults to None.
    :param file_path: str: optional Ignored. Defaults to None.

    :return str: A Connector class intialised with the parameters above and with credential azure library patched
    """

    def connector_factory(*args, **kwargs):
        with patch("aztools.storage.DefaultAzureCredential", return_value="mock-cred"):
            return Connector(*args, **kwargs)

    return connector_factory


@pytest.fixture
def patched_connector():
    """
    This connector patches the crednetials class and the blobservice client. The connector class returned can
    be intialised with path, storage_account, container just like normal.

    :param path: str: optional An azure path. Defaults to None.
    :param storage_account: str: optional Storage account name. Defaults to None.
    :param container: str: optional A container int he storage account. Defaults to None.
    :param file_path: str: optional Ignored. Defaults to None.

    :return str: A Connector class intialised with the parameters above and with mocked azure libraries patched
    """

    def connector_factory(*args, **kwargs):
        with patch("aztools.storage.DefaultAzureCredential", return_value="mock-cred"):
            with patch("aztools.storage.BlobServiceClient") as mock_client:
                # A fixture that has a name attribute like a container is expected to
                def mock_container():
                    pass

                mock_container.name = kwargs.get("container", None)

                mock_client.return_value.list_containers.return_value = [mock_container]
                return Connector(*args, **kwargs)

    return connector_factory


# A class that is initialised the same as the connector is used for testing as it saves some very complex/not working mocking
class MockConnector:
    """
    An essentially empty class that can be used to test the arguments decorator without complexities
    of mocking the connector class
    """

    def __init__(self, storage_account=None, container=None):
        self.storage_account = storage_account
        self.container = container

    @arguments_decorator()
    def func(
        self,
        path: str = None,
        storage_account: str = None,
        container: str = None,
        file_path: str = None,
    ):
        return path, storage_account, container, file_path

    @arguments_decorator(local_support=True)
    def func_local(
        self,
        path: str = None,
        storage_account: str = None,
        container: str = None,
        file_path: str = None,
    ):
        return path, storage_account, container, file_path

    @arguments_decorator()
    def func_extra_args(
        self,
        path: str = None,
        storage_account: str = None,
        container: str = None,
        file_path: str = None,
        extra: str = None,
    ):
        return path, storage_account, container, file_path, extra

    @multi_arguments_decorator()
    def multi_func(
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
        return (
            source_path,
            source_storage_account,
            source_container,
            source_file_path,
            dest_path,
            dest_storage_account,
            dest_container,
            dest_file_path,
        )

    @multi_arguments_decorator()
    def multi_func_extra_args(
        self,
        source_path: str = None,
        source_storage_account: str = None,
        source_container: str = None,
        source_file_path: str = None,
        dest_path: str = None,
        dest_storage_account: str = None,
        dest_container: str = None,
        dest_file_path: str = None,
        extra: str = None,
    ):
        return (
            source_path,
            source_storage_account,
            source_container,
            source_file_path,
            dest_path,
            dest_storage_account,
            dest_container,
            dest_file_path,
            extra,
        )

    @multi_arguments_decorator(local_support=True)
    def multi_func_local(
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
        return (
            source_path,
            source_storage_account,
            source_container,
            source_file_path,
            dest_path,
            dest_storage_account,
            dest_container,
            dest_file_path,
        )

    def parse_azure_path(self, path: str) -> dict:
        with patch("aztools.storage.DefaultAzureCredential", return_value="mock-cred"):
            with patch("aztools.storage.BlobServiceClient") as mock_client:
                # A fixture that has a name attribute like a container is expected to
                def mock_container():
                    pass

                mock_container.name = self.container

                mock_client.return_value.list_containers.return_value = [mock_container]
                con = Connector(
                    storage_account=self.storage_account, container=self.container
                )
                return con.parse_azure_path(path)

    def is_azure_path(self, path: str) -> bool:
        with patch("aztools.storage.DefaultAzureCredential", return_value="mock-cred"):
            with patch("aztools.storage.BlobServiceClient"):
                con = Connector()
                return con.is_azure_path(path)
