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
    "path, expected",
    [
        ("azure://test-container/test-folder/test-subfolder/file.txt", True),
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            True,
        ),
        ("/home/user/docs/files", False),
        ("./this/and/that", False),
        ("~/that/and/this", False),
    ],
)
def test_is_azure_path(path, expected, patched_connector):
    con = patched_connector()
    result = con.is_azure_path(path)
    assert expected == result


@pytest.mark.parametrize(
    "path, storage_account, container, file_path, extra, expected_path, expected_storage_account, expected_container, expected_file_path, expected_extra",
    [
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            None,
            None,
            None,
            None,
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
            None,
        ),
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            "test-account-2",
            None,
            None,
            None,
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
            None,
        ),
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            "test-account-2",
            "test-container-2",
            None,
            None,
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
            None,
        ),
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            "test-account-2",
            "test-container-2",
            "test-path",
            None,
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
            None,
        ),
        (
            None,
            "test-account",
            "test-container",
            "test-path",
            None,
            None,
            "test-account",
            "test-container",
            "test-path",
            None,
        ),
        (
            "azure://test-container/test-directory/test-sub-dir/test.txt",
            "test-account-2",
            "test-container-2",
            "test-path",
            None,
            None,
            "test-account-2",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
            None,
        ),
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            None,
            None,
            None,
            "test-arg",
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
            "test-arg",
        ),
    ],
)
def test_arguments_decorator(
    path,
    storage_account,
    container,
    file_path,
    extra,
    expected_path,
    expected_storage_account,
    expected_container,
    expected_file_path,
    expected_extra,
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

    # Assert passes when decorator is set to support local_paths
    r_path, r_storage_account, r_container, r_file_path = con_init.func_local(
        path=path, file_path=file_path
    )

    assert r_path == expected_path
    assert r_storage_account == expected_storage_account
    assert r_container == expected_container
    assert r_file_path == expected_file_path

    # Assert passes right params if function has extra parameters
    con = MockConnector()

    r_path, r_storage_account, r_container, r_file_path, r_extra = con.func_extra_args(
        path=path,
        storage_account=storage_account,
        container=container,
        file_path=file_path,
        extra=extra,
    )

    assert r_path == expected_path
    assert r_storage_account == expected_storage_account
    assert r_container == expected_container
    assert r_file_path == expected_file_path
    assert r_extra == expected_extra


def test_arguments_decorator_with_local_path():
    con = MockConnector()
    r_path, r_storage_account, r_container, r_file_path = con.func_local(
        path="./local-dir/local_file.txt",
        storage_account=None,
        container=None,
        file_path=None,
    )

    assert r_path == "./local-dir/local_file.txt"
    assert r_storage_account == None
    assert r_container == None
    assert r_file_path == None

    with pytest.raises(ValueError):
        con.func(
            path="./local-dir/local_file.txt",
            storage_account=None,
            container=None,
            file_path=None,
        )


@pytest.mark.parametrize(
    """
    source_path, source_storage_account, source_container, source_file_path, dest_path, dest_storage_account, dest_container, dest_file_path, extra,
    ex_source_path, ex_source_storage_account, ex_source_container, ex_source_file_path, ex_dest_path, ex_dest_storage_account, ex_dest_container, ex_dest_file_path, ex_extra
    """,
    [
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            None,
            None,
            None,
            "https://test-account2.blob.core.windows.net/test-container2/test-directory2/test-sub-dir2/test2.txt",
            None,
            None,
            None,
            None,
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
            None,
            "test-account2",
            "test-container2",
            "test-directory2/test-sub-dir2/test2.txt",
            None,
        ),
        (
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
            "https://test-account2.blob.core.windows.net/test-container2/test-directory2/test-sub-dir2/test2.txt",
            None,
            None,
            None,
            None,
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
            None,
            "test-account2",
            "test-container2",
            "test-directory2/test-sub-dir2/test2.txt",
            None,
        ),
        (
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
            None,
            "test-account2",
            "test-container2",
            "test-directory2/test-sub-dir2/test2.txt",
            None,
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
            None,
            "test-account2",
            "test-container2",
            "test-directory2/test-sub-dir2/test2.txt",
            None,
        ),
        (
            "https://test-account.blob.core.windows.net/test-container/test-directory/test-sub-dir/test.txt",
            "test-account3",
            "test-container3",
            "test-directory/test-sub-dir/test3.txt",
            "https://test-account2.blob.core.windows.net/test-container2/test-directory2/test-sub-dir2/test2.txt",
            "test-account4",
            "test-container4",
            "test-directory/test-sub-dir/test4.txt",
            None,
            None,
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
            None,
            "test-account2",
            "test-container2",
            "test-directory2/test-sub-dir2/test2.txt",
            None,
        ),
        (
            "azure://test-container4/dir/test.txt",
            "test-account",
            "test-container",
            "test-directory/test-sub-dir/test.txt",
            "https://test-account2.blob.core.windows.net/test-container2/test-directory2/test-sub-dir2/test2.txt",
            None,
            None,
            None,
            None,
            None,
            "test-account",
            "test-container4",
            "dir/test.txt",
            None,
            "test-account2",
            "test-container2",
            "test-directory2/test-sub-dir2/test2.txt",
            None,
        ),
    ],
)
def test_multi_arguments_decorator(
    source_path,
    source_storage_account,
    source_container,
    source_file_path,
    dest_path,
    dest_storage_account,
    dest_container,
    dest_file_path,
    extra,
    ex_source_path,
    ex_source_storage_account,
    ex_source_container,
    ex_source_file_path,
    ex_dest_path,
    ex_dest_storage_account,
    ex_dest_container,
    ex_dest_file_path,
    ex_extra,
):
    """
    Tests the handling of arguments for a function in connector class
    """
    # Assert passes right params if connector not init with params
    con = MockConnector()

    (
        r_source_path,
        r_source_storage_account,
        r_source_container,
        r_source_file_path,
        r_dest_path,
        r_dest_storage_account,
        r_dest_container,
        r_dest_file_path,
    ) = con.multi_func(
        source_path=source_path,
        source_storage_account=source_storage_account,
        source_container=source_container,
        source_file_path=source_file_path,
        dest_path=dest_path,
        dest_storage_account=dest_storage_account,
        dest_container=dest_container,
        dest_file_path=dest_file_path,
    )

    assert r_source_path == ex_source_path
    assert r_source_storage_account == ex_source_storage_account
    assert r_source_container == ex_source_container
    assert r_source_file_path == ex_source_file_path
    assert r_dest_path == ex_dest_path
    assert r_dest_storage_account == ex_dest_storage_account
    assert r_dest_container == ex_dest_container
    assert r_dest_file_path == ex_dest_file_path

    # Assert passes right params if connector IS init with params
    con = MockConnector(
        storage_account=source_storage_account, container=source_container
    )

    (
        r_source_path,
        r_source_storage_account,
        r_source_container,
        r_source_file_path,
        r_dest_path,
        r_dest_storage_account,
        r_dest_container,
        r_dest_file_path,
    ) = con.multi_func(
        source_path=source_path,
        source_file_path=source_file_path,
        dest_path=dest_path,
        dest_storage_account=dest_storage_account,
        dest_container=dest_container,
        dest_file_path=dest_file_path,
    )

    assert r_source_path == ex_source_path
    assert r_source_storage_account == ex_source_storage_account
    assert r_source_container == ex_source_container
    assert r_source_file_path == ex_source_file_path
    assert r_dest_path == ex_dest_path
    assert r_dest_storage_account == ex_dest_storage_account
    assert r_dest_container == ex_dest_container
    assert r_dest_file_path == ex_dest_file_path

    # Assert passes right params if local paths supported
    con = MockConnector(
        storage_account=source_storage_account, container=source_container
    )

    (
        r_source_path,
        r_source_storage_account,
        r_source_container,
        r_source_file_path,
        r_dest_path,
        r_dest_storage_account,
        r_dest_container,
        r_dest_file_path,
    ) = con.multi_func_local(
        source_path=source_path,
        source_file_path=source_file_path,
        dest_path=dest_path,
        dest_storage_account=dest_storage_account,
        dest_container=dest_container,
        dest_file_path=dest_file_path,
    )

    assert r_source_path == ex_source_path
    assert r_source_storage_account == ex_source_storage_account
    assert r_source_container == ex_source_container
    assert r_source_file_path == ex_source_file_path
    assert r_dest_path == ex_dest_path
    assert r_dest_storage_account == ex_dest_storage_account
    assert r_dest_container == ex_dest_container
    assert r_dest_file_path == ex_dest_file_path

    # Assert passes right params if func has extra parameters
    con = MockConnector(
        storage_account=source_storage_account, container=source_container
    )

    (
        r_source_path,
        r_source_storage_account,
        r_source_container,
        r_source_file_path,
        r_dest_path,
        r_dest_storage_account,
        r_dest_container,
        r_dest_file_path,
        r_extra,
    ) = con.multi_func_extra_args(
        source_path=source_path,
        source_file_path=source_file_path,
        dest_path=dest_path,
        dest_storage_account=dest_storage_account,
        dest_container=dest_container,
        dest_file_path=dest_file_path,
        extra=extra,
    )

    assert r_source_path == ex_source_path
    assert r_source_storage_account == ex_source_storage_account
    assert r_source_container == ex_source_container
    assert r_source_file_path == ex_source_file_path
    assert r_dest_path == ex_dest_path
    assert r_dest_storage_account == ex_dest_storage_account
    assert r_dest_container == ex_dest_container
    assert r_dest_file_path == ex_dest_file_path
    assert r_extra == ex_extra


@pytest.mark.parametrize(
    """
    source_path, source_storage_account, source_container, source_file_path, dest_path, dest_storage_account, dest_container, dest_file_path,
    ex_source_path, ex_source_storage_account, ex_source_container, ex_source_file_path, ex_dest_path, ex_dest_storage_account, ex_dest_container, ex_dest_file_path,
    """,
    [
        (
            "./local/local-test/test.txt",
            None,
            None,
            None,
            "https://test-account2.blob.core.windows.net/test-container2/test-directory2/test-sub-dir2/test2.txt",
            None,
            None,
            None,
            "./local/local-test/test.txt",
            None,
            None,
            None,
            None,
            "test-account2",
            "test-container2",
            "test-directory2/test-sub-dir2/test2.txt",
        ),
        (
            "https://test-account2.blob.core.windows.net/test-container2/test-directory2/test-sub-dir2/test2.txt",
            None,
            None,
            None,
            "./local/local-test/test.txt",
            None,
            None,
            None,
            None,
            "test-account2",
            "test-container2",
            "test-directory2/test-sub-dir2/test2.txt",
            "./local/local-test/test.txt",
            None,
            None,
            None,
        ),
    ],
)
def test_multi_arguments_local_decorator(
    source_path,
    source_storage_account,
    source_container,
    source_file_path,
    dest_path,
    dest_storage_account,
    dest_container,
    dest_file_path,
    ex_source_path,
    ex_source_storage_account,
    ex_source_container,
    ex_source_file_path,
    ex_dest_path,
    ex_dest_storage_account,
    ex_dest_container,
    ex_dest_file_path,
):
    """
    Tests the handling of arguments for a function in connector class with mutliple
    """
    # Assert passes right params if connector not init with params
    con = MockConnector()

    (
        r_source_path,
        r_source_storage_account,
        r_source_container,
        r_source_file_path,
        r_dest_path,
        r_dest_storage_account,
        r_dest_container,
        r_dest_file_path,
    ) = con.multi_func_local(
        source_path=source_path,
        source_storage_account=source_storage_account,
        source_container=source_container,
        source_file_path=source_file_path,
        dest_path=dest_path,
        dest_storage_account=dest_storage_account,
        dest_container=dest_container,
        dest_file_path=dest_file_path,
    )

    assert r_source_path == ex_source_path
    assert r_source_storage_account == ex_source_storage_account
    assert r_source_container == ex_source_container
    assert r_source_file_path == ex_source_file_path
    assert r_dest_path == ex_dest_path
    assert r_dest_storage_account == ex_dest_storage_account
    assert r_dest_container == ex_dest_container
    assert r_dest_file_path == ex_dest_file_path


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
            "https://test-account.blob.core.windows.net/",
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
        mock_bs_client.assert_called_with(
            credential="mock-cred", account_url=expected_blob_url
        )
