import logging

from azure_tools.logging import QuietAzureTokenLogs


def test_quiet_azure_logs():
    test_cases = [
        # Check azure info logs are filtered
        {
            'record': logging.LogRecord(
                msg="message", level=logging.INFO, name="azure", pathname=None, 
                lineno=None, args=None, exc_info=None
            ),
            'expected': False
        },
        # Check azure warnings are not filtered
        {
            'record': logging.LogRecord(
                msg="message", level=logging.WARNING, name="azure", pathname=None, 
                lineno=None, args=None, exc_info=None
            ),
            'expected': True
        },
        # Check azure creds warnings are not logged
        {
            'record': logging.LogRecord(
                msg="EnvironmentCredential.get_token failed", level=logging.WARNING, name="azure", pathname=None, 
                lineno=None, args=None, exc_info=None
            ),
            'expected': False
        },
        # Check passes success
        {
            'record': logging.LogRecord(
                msg="AzureCliCredential.get_token succeeded", level=logging.INFO, name="azure", pathname=None, 
                lineno=None, args=None, exc_info=None
            ),
            'expected': True
        }
    ]

    for test in test_cases:
        assert QuietAzureTokenLogs().filter(record=test['record']) == test['expected']
