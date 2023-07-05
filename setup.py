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
    install_requires=["requests",
                      "pandas",
                      "numpy"],
    # *strongly* suggested for sharing
    version="2.0.0",
    # The license can be anything you like
    license="MIT",
    description="A Python package to facilitate interaction with the Impect customer API",
    long_description=README,
)