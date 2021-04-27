from shell import cmd_output
from azure.core.exceptions import ResourceNotFoundError
from connector import Connector


class AzureACRImageTarget():
    """
    Luigi target task for docker images in Azure ACR repositories.
    """
    def __init__(self, repository, tag):
        self.path = f"{repository}:{tag}"
        self.tag = tag
        self.repository = repository

    def acr_image_exists(self, repository, tag, account="fandango"):
        """
        checks whether a docker image exists in an azure ACR repository
        """
        cmd = f"az acr repository show-tags  --name {account} --resource-group fandango --repository {repository}"
        output = cmd_output(cmd)
        return f'"{tag}"' in output

    def exists(self):
        return self.acr_image_exists(self.repository, self.tag)


class AzureBlobTarget():
    """
    Luigi Target class supporting smart_open like uris:
    azure://container/some/path/file
    """

    def __init__(self, 
        path: str = None,
        storage_account: str = None,
        container: str = None,
        file_path: str = None
    ):
        self.path = str(path)
        self.client = Connector(path=path, storage_account=storage_account)

    def exists(self):
        try:
            with self.client.open(path=self.path):
                file_exists = True
        except ValueError:
            file_exists = False
        except ResourceNotFoundError:
            file_exists = False
        print("The file: {} exists = {}".format(self.path, file_exists))
        return file_exists
