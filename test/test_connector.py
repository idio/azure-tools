import pytest
from unittest.mock import patch
from connector import Connector
from test.fixtures import *


@pytest.mark.parametrize(
    "path, storage_account, expected",
    [
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            None,
            {
                "storage_account": "test-account",
                "container": "test-container",
                "file_path": "test-directory/test-sub-dir/test.txt",
            },
        ),
        (
            "azure://test-container/test-folder/test-subfolder/file.txt",
            None,
            {
                "storage_account": None,
                "container": "test-container",
                "file_path": "test-folder/test-subfolder/file.txt",
            },
        ),
        (
            "azure://test-container/test-folder/test-subfolder/file.txt",
            "test-storage-account",
            {
                "storage_account": "test-storage-account",
                "container": "test-container",
                "file_path": "test-folder/test-subfolder/file.txt",
            },
        ),
    ],
)
def test_parse_azure_path(path, storage_account, expected):
    """
    Tests the parsing of azure paths to a dictionary of:
        storage_account
        Container
        path
    """
    con = Connector(storage_account=storage_account)
    parsed_path = con.parse_azure_path(path)
    assert parsed_path == expected


@pytest.mark.parametrize(
    "path, storage_account, container, file_path, expected_path, expected_storage_account, expected_container, expected_file_path",
    [
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            None,
            None,
            None,
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
        ),
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            "test-account-2",
            None,
            None,
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
        ),
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            "test-account-2",
            "test-container-2",
            None,
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
        ),
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            "test-account-2",
            "test-container-2",
            "test-path",
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
        ),
        (
            None,
            "test-account",
            "test-container",
            "test-path",
            None,
            "test-account",
            "test-container",
            "test-path",
        ),
        (
            "azure://test-container/test-directory/test-sub-dir/test.txt",
            "test-account-2",
            "test-container-2",
            "test-path",
            None,
            "test-account-2",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
        ),
    ],
)
def test_arguments_decorator(
    path,
    storage_account,
    container,
    file_path,
    expected_path,
    expected_storage_account,
    expected_container,
    expected_file_path,
):
    """
    Tests the handling of arguments for a function in connector class
    """
    # Assert passes right params if connector not init with params
    con = MockConnector()

    r_path, r_storage_account, r_container, r_file_path = con.func(
        path=path,
        storage_account=storage_account,
        container=container,
        file_path=file_path,
    )

    assert r_path == expected_path
    assert r_storage_account == expected_storage_account
    assert r_container == expected_container
    assert r_file_path == expected_file_path

    # Assert passes right params if connector IS init with params
    con_init = MockConnector(storage_account=storage_account, container=container)

    r_path, r_storage_account, r_container, r_file_path = con_init.func(
        path=path, file_path=file_path
    )

    assert r_path == expected_path
    assert r_storage_account == expected_storage_account
    assert r_container == expected_container
    assert r_file_path == expected_file_path


@pytest.mark.parametrize(
    "path, storage_account, container, file_path, exception",
    [
        (
            "azure://test-container/test-directory/test-sub-dir/test.txt",
            None,
            None,
            None,
            ValueError,
        )
    ],
)
def test_arguments_decorator_errors(
    path, storage_account, container, file_path, exception
):
    """
    Tests the handling of arguments for a function in connector class
    """

    con = MockConnector(storage_account=storage_account, container=container)
    with pytest.raises(exception):
        con.func(path=path, file_path=file_path)


def test_connector_init(patched_creds_connector):
    with patch("connector.BlobServiceClient") as mock_client:
        # Asserting that no clients are intialised if the connector is not initialised with params
        con = patched_creds_connector()
        mock_client.assert_not_called()
        mock_client.return_value.get_container_client.assert_not_called()
        assert con.storage_account == None
        assert con.container == None

        # Assert builds boblserviceclient when init with storage_account name
        con = patched_creds_connector(storage_account="test-account")
        mock_client.assert_called_with(
            credential="mock-cred",
            account_url="https://test-account.blob.core.windows.net/",
        )
        mock_client.return_value.get_container_client.assert_not_called()
        assert con.storage_account == "test-account"
        assert con.container == None

        # A fixture that has a name attribute like a container is expected to
        def mock_container():
            pass

        mock_container.name = "test-container"

        # Assert builds container client if init with storage_account AND container
        mock_client.return_value.list_containers.return_value = [mock_container]
        con = patched_creds_connector(
            storage_account="test-account", container="test-container"
        )
        mock_client.assert_called_with(
            credential="mock-cred",
            account_url="https://test-account.blob.core.windows.net/",
        )
        mock_client.return_value.get_container_client.assert_called_with(
            container="test-container"
        )
        assert con.storage_account == "test-account"
        assert con.container == "test-container"

        # Assert builds container client if init with full path
        con = patched_creds_connector(
            path="https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt"
        )
        mock_client.assert_called_with(
            credential="mock-cred",
            account_url="https://test-account.blob.core.windows.net/",
        )
        mock_client.return_value.get_container_client.assert_called_with(
            container="test-container"
        )
        assert con.storage_account == "test-account"
        assert con.container == "test-container"


@pytest.mark.parametrize(
    "path, storage_account, container, file_path, expected_url",
    [
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            None,
            None,
            None,
            "https://test-account.blob.core.windows.net/",
        ),
        (
            "azure://test-container/test-directory/test-sub-dir/test.txt",
            "test-account",
            "crazy",
            "horse",
            "https://test-account.blob.core.windows.net/",
        ),
        (
            None,
            "test-account",
            None,
            None,
            "https://test-account.blob.core.windows.net/",
        ),
    ],
)
def test_blob_storage_url(
    path, storage_account, container, file_path, expected_url, patched_connector
):
    con = patched_connector()
    result = con.get_blob_storage_url(
        path=path,
        storage_account=storage_account,
        container=container,
        file_path=file_path,
    )
    assert expected_url == result

    con_init = patched_connector(storage_account=storage_account)
    result_init = con_init.get_blob_storage_url(path=path)
    assert result_init == expected_url


@pytest.mark.parametrize(
    "path, storage_account, container, file_path",
    [
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            "test-account",
            "test-container",
            "test-file-path",
        )
    ],
)
def test_get_container_client(
    path, storage_account, container, file_path, patched_connector
):
    with patch("connector.Connector.get_blob_service_client") as mock_bs_client:
        # Assert tests with init connector
        con = patched_connector(storage_account=storage_account, container=container)

        con.get_container_client(
            path=path,
            storage_account=storage_account,
            container=container,
            file_path=file_path,
        )
        mock_bs_client.assert_not_called()
        mock_bs_client.return_value.get_container_client.assert_not_called()

        # Assert test with NON init connector
        con = patched_connector()

        def mock_container():
            pass

        mock_container.name = container
        mock_bs_client.return_value.list_containers.return_value = [mock_container]

        con.get_container_client(
            path=path,
            storage_account=storage_account,
            container=container,
            file_path=file_path,
        )
        mock_bs_client.assert_called_with(storage_account=storage_account)
        mock_bs_client.return_value.get_container_client.assert_called_with(
            container=container
        )

@pytest.mark.parametrize(
    "path, storage_account, container, file_path, expected_blob_url",
    [
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            "test-account",
            None,
            None,
            "https://test-account.blob.core.windows.net/"
        )
    ],
)
def test_get_blob_service_client(
    path, storage_account, container, file_path, expected_blob_url, patched_connector
):
    with patch("connector.BlobServiceClient") as mock_bs_client:
        # Assert tests with init connector
        con = patched_connector(storage_account=storage_account, container=container)

        con.get_blob_service_client(
            path=path,
            storage_account=storage_account,
            container=container,
            file_path=file_path,
        )
        mock_bs_client.assert_not_called()

        # Assert test with NON init connector
        con = patched_connector()

        def mock_container():
            pass

        mock_container.name = container
        mock_bs_client.return_value.list_containers.return_value = [mock_container]

        con.get_blob_service_client(
            path=path,
            storage_account=storage_account,
            container=container,
            file_path=file_path,
        )
        mock_bs_client.assert_called_with(credential="mock-cred", account_url=expected_blob_url)
