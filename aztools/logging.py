import logging 

    
class QuietAzureTokenLogs(logging.Filter):
    def is_failed_creds_warning(self, record):
        return (
            record.levelname == 'WARNING' and (
                record.getMessage().startswith("EnvironmentCredential.get_token failed") or
                record.getMessage().startswith("ManagedIdentityCredential.get_token failed") or
                record.getMessage().startswith("SharedTokenCacheCredential.get_token failed") or
                record.getMessage().startswith("VisualStudioCodeCredential.get_token failed")
            )
        )

    def filter(self, record):
        if record.getMessage().startswith("AzureCliCredential.get_token succeeded"):
            return True
        return not (record.levelname == "INFO" or self.is_failed_creds_warning(record))


def init_loggers():
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("azure.identity._internal.decorators").setLevel(logging.INFO)
    logging.getLogger("azure.identity._internal.decorators").addFilter(QuietAzureTokenLogs())