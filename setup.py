from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open('requirements.txt') as f:
    requirements = [r for r in f.read().splitlines() if r not in ["pytest", "setuptools"]]

setup(
    name='azure_tools',
    version='0.0.1',    
    description='Python tools for working with azure',
    url='https://github.com/idio/azure_tools',
    author="Fandango",
    author_email="george.phillips@episerver.com",
    packages=['aztools'],
    install_requires=requirements,
    classifiers=[      
        'Programming Language :: Python :: 3.7',
    ],
    python_requires=">=3.7"
)
