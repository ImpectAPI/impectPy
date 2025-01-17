import os
from setuptools import setup

with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), "README.md")) as f:
    README = f.read()

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name="impectPy",
    url="tbd",
    author="Impect",
    author_email="info@impect.com",
    # Needed to actually package something
    packages=["impectPy"],
    # Needed for dependencies
    install_requires=["requests>=2.24.0",
                      "pandas>=2.0.0",
                      "numpy>=1.24.2,<2.0"],
    # *strongly* suggested for sharing
    version="2.3.0",
    # The license can be anything you like
    license="MIT",
    description="A Python package to facilitate interaction with the Impect customer API",
    long_description=README,
)