from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='azure_tools',
    version='0.0.1',    
    description='Python tools for working with azure',
    url='https://github.com/idio/azure_tools',
    author="Fandango",
    author_email="george.phillips@episerver.com",
    packages=['aztools'], #find_packages(where="aztools"),
    install_requires=[
        "azure-common==1.1.26",
        "azure-identity==1.5.0",
        "azure-kusto-data==1.0.3",
        "azure-storage-blob==12.6.0",
        "smart_open==3.0.0",
        "smart_open[azure]==3.0.0",
    ],

    classifiers=[      
        'Programming Language :: Python :: 3.8',
    ],
    python_requires=">=3.8"
)