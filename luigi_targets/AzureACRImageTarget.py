import luigi
from shell import cmd_output


class AzureACRImageTarget(luigi.Target):
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
