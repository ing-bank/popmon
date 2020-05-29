from setuptools import setup, find_packages

NAME = 'popmon'

MAJOR = 0
REVISION = 3
PATCH = 3
DEV = False
# NOTE: also update version at: README.rst

with open('requirements.txt') as f:
    REQUIREMENTS = f.read().splitlines()

# read the contents of abstract file
with open("README.rst", encoding="utf-8") as f:
    long_description = f.read()

VERSION = '{major}.{revision}.{patch}'.format(major=MAJOR, revision=REVISION, patch=PATCH)
FULL_VERSION = VERSION
if DEV:
    FULL_VERSION += '.dev'
    with open('requirements-test.txt') as f:
        REQUIREMENTS += f.read().splitlines()


def write_version_py(filename: str = 'popmon/version.py') -> None:
    """Write package version to version.py.

    This will ensure that the version in version.py is in sync with us.

    :param filename: The version.py to write too.
    :type filename: str
    """
    # Do not modify the indentation of version_str!
    version_str = """\"\"\"THIS FILE IS AUTO-GENERATED BY SETUP.PY.\"\"\"

name = '{name!s}'
version = '{version!s}'
full_version = '{full_version!s}'
release = {is_release!s}
"""

    with open(filename, 'w') as version_file:
        version_file.write(version_str.format(name=NAME.lower(),
                                              version=VERSION,
                                              full_version=FULL_VERSION,
                                              is_release=not DEV))


def setup_package() -> None:
    """The main setup method.

    It is responsible for setting up and installing the package.
    """
    write_version_py()

    setup(name=NAME,
          version=VERSION,
          url='https://github.com/ing-bank/popmon',
          license='MIT',
          author='ING Wholesale Banking Advanced Analytics',
          description='Monitor the stability of a pandas or spark dataset',
          keywords="pandas spark data-science data-analysis monitoring statistics python jupyter ipython",
          long_description=long_description,
          long_description_content_type="text/x-rst",
          python_requires='>=3.6',
          packages=find_packages(),
          install_requires=REQUIREMENTS,
          classifiers=[
              "Programming Language :: Python :: 3",
              "License :: OSI Approved :: MIT License",
              "Operating System :: OS Independent",
          ],
          # files to be shipped with the installation, under: popmon/popmon/
          # after installation, these can be found with the functions in resources.py
          package_data=dict(popmon=['visualization/templates/*.html', 'visualization/templates/assets/css/*.css',
                                    'visualization/templates/assets/js/*.js', 'test_data/*.csv.gz',
                                    'test_data/*.json*', 'notebooks/popmon*tutorial*.ipynb']),
          entry_points={
              'console_scripts': [
                  'popmon_run = popmon.pipeline.amazing_pipeline:run',
              ],
          })


if __name__ == '__main__':
    setup_package()
