import os
import re
from setuptools import setup

with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), "README.md")) as f:
    README = f.read()

with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), "impectPy", "__init__.py")) as f:
    version = re.search(r'^__version__\s*=\s*["\'](.+)["\']', f.read(), re.M).group(1)

setup(
    # Needed to silence warnings (and to be a worthwhile package)
    name="impectPy",
    url="https://github.com/ImpectAPI/impectPy",
    author="Impect",
    author_email="info@impect.com",
    # Needed to actually package something
    packages=["impectPy"],
    # Needed for dependencies
    install_requires=["requests>=2.24.0",
                      "pandas>=2.2.0",
                      "numpy>=1.24.2"],
    # *strongly* suggested for sharing
    version=version,
    # The license can be anything you like
    license="MIT",
    description="A Python package to facilitate interaction with the Impect customer API",
    long_description=README,
    long_description_content_type="text/markdown",
)