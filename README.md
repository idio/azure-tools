## Azure Tools
This is a library that contains basic azure utilities and tools used by many of the projects in Idio/Optimizely.

### Deploying
To build a distrobution run:
`python setup.py sdist`

To upload:
`twine upload <url> dist/*`
`twine upload --repository-url https://test.pypi.org/legacy/ dist/pyexample-0.1.0.tar.gz`