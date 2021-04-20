def arguments_decorator(func):
    #         @wraps(func)
    def wrapper(*args, **kwargs):
        inst = args[0]
        if kwargs.get("path", None):

            paths = inst.parse_azure_path(kwargs["path"])
            storage_account = (
                paths["storage_account"]
                if paths["storage_account"]
                else kwargs.get("storage_account", None)
            )

            if storage_account is None:
                raise ValueError(
                    "To use a path of the form azure://container/path you must initialise the connector with the storage account or pass the account name to the function"
                )
            return func(
                *args,
                storage_account=storage_account,
                container=paths["container"],
                file_path=paths["file_path"],
            )
        else:
            storage_account = (
                kwargs.get("storage_account", None)
                if kwargs.get("storage_account", None)
                else inst.storage_account
            )
            container = (
                kwargs.get("container", None)
                if kwargs.get("container", None)
                else inst.container
            )
            file_path = kwargs.get("file_path", None)
            return func(
                *args,
                storage_account=storage_account,
                container=container,
                file_path=file_path,
            )

    return wrapper

def multi_arguments_decorator(func):
    def wrapper(*args, **kwargs):
        inst = args[0]
        if kwargs.get("source_path", None):

            paths = inst.parse_azure_path(kwargs["source_path"])
            storage_account = (
                paths["storage_account"]
                if paths["storage_account"]
                else kwargs.get("source_storage_account", None)
            )

            if storage_account is None:
                raise ValueError(
                    "To use a path of the form azure://container/path you must initialise the connector with the storage account or pass the account name to the function"
                )
            source_params = {
                "source_storage_account":storage_account,
                "source_container":paths["container"],
                "source_file_path":paths["file_path"],
            }
        else:
            storage_account = (
                kwargs.get("source_storage_account", None)
                if kwargs.get("source_storage_account", None)
                else inst.storage_account
            )
            container = (
                kwargs.get("source_container", None)
                if kwargs.get("source_container", None)
                else inst.container
            )
            file_path = kwargs.get("source_file_path", None)
            source_params = {
                "source_storage_account":storage_account,
                "source_container":container,
                "source_file_path":file_path,
            }

        if kwargs.get("dest_path", None):

            paths = inst.parse_azure_path(kwargs["dest_path"])
            storage_account = (
                paths["storage_account"]
                if paths["storage_account"]
                else kwargs.get("dest_storage_account", None)
            )

            if storage_account is None:
                raise ValueError(
                    "To use a path of the form azure://container/path you must initialise the connector with the storage account or pass the account name to the function"
                )
            dest_params = {
                "dest_storage_account":storage_account,
                "dest_container":paths["container"],
                "dest_file_path":paths["file_path"],
            }
        else:
            storage_account = kwargs.get("dest_storage_account", None)
            container = kwargs.get("dest_container", None)
            file_path = kwargs.get("dest_file_path", None)
            dest_params = {
                "dest_storage_account":storage_account,
                "dest_container":container,
                "dest_file_path":file_path,
            }
        params = {**source_params, **dest_params}
        return func(*args, **params)

    return wrapper