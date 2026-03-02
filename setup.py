import os
from setuptools import setup

with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), "README.md")) as f:
    README = f.read()

setup(
    name="impectPyRSCA",
    url="https://github.com/rsca-intelligence/impectPyRSCA",
    author="RSCA Intelligence",
    packages=["impectPyRSCA"],
    install_requires=["requests>=2.24.0",
                      "pandas>=2.2.0",
                      "numpy>=1.24.2"],
    version="2.5.7",
    license="MIT",
    description="RSCA fork of impectPy — a Python package to facilitate interaction with the Impect customer API",
    long_description=README,
    long_description_content_type="text/markdown",
)
