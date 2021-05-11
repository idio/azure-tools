## Azure Tools
This is a library that contains basic azure utilities and tools used by many of the projects in Idio/Optimizely.

### Installing with pip
To install azure-tools with pip:
```pip install git+https://github.com/idio/azure-tools.git```
If you wish to install from a specific branch use:
```pip install git+https://github.com/idio/azure-tools.git@<branch-name>```

#### Environment setup
For this library to work as expected you should ensure you have the [azure cli tool](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-macos) installed.

Once installed add the storage-preview extension, this enables smart-open:
```az extension add -n storage-preview```