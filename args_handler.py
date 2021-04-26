def arguments_decorator(local_support=False):
    """
    This decorator handles the passing of arguments to azure tools methods. It expects a location to be defined by:

    :param path: str: optional An azure path of the format azure://<container>/path or https://<storage-account>.blob.core.windows.net/<container>/<path>
    :param storage-account: str: The name of the azure storage account
    :param container: str: The name of a container in the account
    :param file_path: str: path from container to file or directory

    The decorator will parse the path if it is passed and ensure the function is always called with the storage-account, container and file_path
    parameters.

    :param local_support: bool: optional If True local paths can be passed to the path parameter, function will be called with other params as None. Defaults to False.
    """    
    def decorator(func):
        def wrapper(*args, **kwargs):
            inst = args[0]
            path = kwargs.get("path", None)
            storage_account = kwargs.get("storage_account", None)
            container = kwargs.get("container", None)
            file_path = kwargs.get("file_path", None)

            if path:
                if inst.is_azure_path(path):
                    paths = inst.parse_azure_path(path)
                    storage_account = (
                        paths["storage_account"]
                        if paths["storage_account"]
                        else storage_account
                    )

                    if storage_account is None:
                        raise ValueError(
                            "To use a path of the form azure://container/path you must initialise the connector with the storage account or pass the account name to the function"
                        )
                    kwargs["path"] = None
                    kwargs["storage_account"] = storage_account
                    kwargs["container"] = paths["container"]
                    kwargs["file_path"] = paths["file_path"]
                elif local_support:
                    kwargs["path"] = path
                    kwargs["storage_account"] = None
                    kwargs["container"] = None
                    kwargs["file_path"] = None
                else:
                    raise ValueError(
                        f"Path: {path} is not an azure path and local_support is not enabled. Try enabling for this function in the args handler decorator"
                    )
            else:
                kwargs["path"] = None
                kwargs["storage_account"] = (
                    storage_account if storage_account else inst.storage_account
                )
                kwargs["container"] = container if container else inst.container
                kwargs["file_path"] = file_path

                if kwargs["storage_account"] is None:
                    raise ValueError("Storage account is None, pass to the function or initialise the connector")
                if kwargs["storage_account"] and kwargs["file_path"] and not kwargs["container"]:
                    raise ValueError("Container is None, pass to function or initialise Connector")

            return func(*args, **kwargs)

        return wrapper

    return decorator


def multi_arguments_decorator(local_support=False):
    """
    This decorator handles the passing of arguments to azure tools methods that require two paths, such as copy/download/upload. It expects a location to be defined by source and destination parameters:

    :param source/dest_path: str: optional An azure path of the format azure://<container>/path or https://<storage-account>.blob.core.windows.net/<container>/<path>
    :param source/dest_storage-account: str: The name of the azure storage account
    :param source/dest_container: str: The name of a container in the account
    :param source/dest_file_path: str: path from container to file or directory

    The decorator will parse the path if it is passed and ensure the function is always called with the storage-account, container and file_path
    parameters.

    :param local_support: bool: optional If True local paths can be passed to the path parameter, function will be called with other params as None. Defaults to False.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            inst = args[0]

            source_path = kwargs.get("source_path", None)
            source_storage_account = kwargs.get("source_storage_account", None)
            source_container = kwargs.get("source_container", None)
            source_file_path = kwargs.get("source_file_path", None)

            dest_path = kwargs.get("dest_path", None)
            dest_storage_account = kwargs.get("dest_storage_account", None)
            dest_container = kwargs.get("dest_container", None)
            dest_file_path = kwargs.get("dest_file_path", None)

            if source_path:
                if inst.is_azure_path(source_path):
                    paths = inst.parse_azure_path(source_path)
                    storage_account = (
                        paths["storage_account"]
                        if paths["storage_account"]
                        else source_storage_account
                    )

                    if storage_account is None:
                        raise ValueError(
                            "To use a path of the form azure://container/path you must initialise the connector with the storage account or pass the account name to the function"
                        )
                    kwargs["source_path"] = None
                    kwargs["source_storage_account"] = storage_account
                    kwargs["source_container"] = paths["container"]
                    kwargs["source_file_path"] = paths["file_path"]

                elif local_support:
                    kwargs["source_path"] = source_path
                    kwargs["source_storage_account"] = None
                    kwargs["source_container"] = None
                    kwargs["source_file_path"] = None
                else:
                    raise ValueError(
                        f"Path: {source_path} is not an azure path and local_support is not enabled. Try enabling for this function in the args handler decorator"
                    )
            else:
                kwargs["source_storage_account"] = (
                    source_storage_account
                    if source_storage_account
                    else inst.storage_account
                )
                kwargs["source_container"] = (
                    source_container if source_container else inst.container
                )
                kwargs["source_file_path"] = source_file_path
                kwargs["source_path"] = None
                if kwargs["source_storage_account"] is None:
                    raise ValueError("Storage account is None, pass to the function or initialise the connector")
                if kwargs["source_storage_account"] and kwargs["file_path"] and not kwargs["container"]:
                    raise ValueError("Container is None, pass to function or initialise Connector")

            if dest_path:
                if inst.is_azure_path(dest_path):
                    paths = inst.parse_azure_path(dest_path)
                    storage_account = (
                        paths["storage_account"]
                        if paths["storage_account"]
                        else dest_storage_account
                    )

                    if storage_account is None:
                        raise ValueError(
                            "To use a path of the form azure://container/path you must initialise the connector with the storage account or pass the account name to the function"
                        )
                    kwargs["dest_path"] = None
                    kwargs["dest_storage_account"] = storage_account
                    kwargs["dest_container"] = paths["container"]
                    kwargs["dest_file_path"] = paths["file_path"]

                elif local_support:
                    kwargs["dest_path"] = dest_path
                    kwargs["dest_storage_account"] = None
                    kwargs["dest_container"] = None
                    kwargs["dest_file_path"] = None
                else:
                    raise ValueError(
                        f"Path: {dest_path} is not an azure path and local_support is not enabled. Try enabling for this function in the args handler decorator"
                    )
            else:
                kwargs["dest_path"] = None
                kwargs["dest_storage_account"] = dest_storage_account
                kwargs["dest_container"] = dest_container
                kwargs["dest_file_path"] = dest_file_path
                if kwargs["dest_storage_account"] is None:
                    raise ValueError("Storage account is None, pass to the function or initialise the connector")
                if kwargs["dest_storage_account"] and kwargs["file_path"] and not kwargs["container"]:
                    raise ValueError("Container is None, pass to function or initialise Connector")

            return func(*args, **kwargs)

        return wrapper

    return decorator
