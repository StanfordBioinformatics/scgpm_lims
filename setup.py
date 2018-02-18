
# For some usefule documentation, see
# https://docs.python.org/2/distutils/setupscript.html.
# This page is useful for dependencies: 
# http://python-packaging.readthedocs.io/en/latest/dependencies.html.

#from distutils.core import setup
from setuptools import setup
from setuptools import find_packages
import glob

setup(
  name = "scgpm_lims",
  version = "0.1.0",
  description = "Client API for UHTS LIMS of the Sequencing Center",
  author = "Nathaniel Watson, Nathan Hammond",
  author_email = "nathankw@stanford.edu",
  url = "https://github.com/StanfordBioinformatics/scgpm_lims",
  packages = find_packages(),
  install_requires = [
    "requests",
    "urllib3"
  ],
  scripts = glob.glob("scgpm_lims/scripts/*.py")
)
