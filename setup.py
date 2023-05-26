from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='azure_tools',
    version='0.0.1',    
    description='Python tools for working with azure',
    url='https://github.com/idio/azure_tools@azure-identity',
    author="Fandango",
    author_email="george.phillips@episerver.com",
    packages=['aztools'], #find_packages(where="aztools"),
    install_requires=requirements,
    classifiers=[      
        'Programming Language :: Python :: 3.8',
    ],
    python_requires=">=3.8"
)