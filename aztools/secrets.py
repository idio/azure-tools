from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential


class AzureSecretStore():

    def __init__(self, key_vault):
        key_vault_uri = f"https://{key_vault}.vault.azure.net"
        credential = DefaultAzureCredential()
        self.client = SecretClient(vault_url=key_vault_uri, credential=credential)

    def set_secret(self, secret_name, secret_value):
        self.client.set_secret(secret_name, secret_value)

    def get_secret(self, secret_name):
        return self.client.get_secret(secret_name).value

    def delete_secret(self, secret_name):
        poller = client.begin_delete_secret(secretName)
        deleted_secret = poller.result()
        return deleted_secret
