from setuptools import setup

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
    version="0.1",
    # The license can be anything you like
    license="MIT",
    description="A Python package to facilitate interaction with the Impect customer API",
    # We will also need a readme eventually (there will be a warning)
    # long_description=open('README.txt').read(),
)