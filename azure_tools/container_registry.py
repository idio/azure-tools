from aztools.shell import cmd_output

def acr_image_exists(repository, tag, account="fandango"):
    """
    checks whether a docker image exists in an azure ACR repository
    """
    cmd = f"az acr repository show-tags  --name {account} --resource-group fandango --repository {repository}"
    output = cmd_output(cmd)
    return f'"{tag}"' in output