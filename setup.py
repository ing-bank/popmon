from setuptools import find_packages, setup

with open("requirements.txt") as f:
    REQUIREMENTS = f.read().splitlines()

# read the contents of abstract file
with open("README.rst", encoding="utf-8") as f:
    long_description = f.read()


def setup_package() -> None:
    """The main setup method.

    It is responsible for setting up and installing the package.
    """
    setup(
        name="popmon",
        version="0.4.3",
        url="https://github.com/ing-bank/popmon",
        license="MIT",
        author="ING Wholesale Banking Advanced Analytics",
        description="Monitor the stability of a pandas or spark dataset",
        keywords="pandas spark data-science data-analysis monitoring statistics python jupyter ipython",
        long_description=long_description,
        long_description_content_type="text/x-rst",
        python_requires=">=3.6",
        packages=find_packages(),
        install_requires=REQUIREMENTS,
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
        ],
        # files to be shipped with the installation, under: popmon/popmon/
        # after installation, these can be found with the functions in resources.py
        package_data={
            "popmon": [
                "visualization/templates/*.html",
                "visualization/templates/assets/css/*.css",
                "visualization/templates/assets/js/*.js",
                "test_data/*.csv.gz",
                "test_data/*.json*",
                "notebooks/popmon*tutorial*.ipynb",
            ]
        },
        entry_points={
            "console_scripts": ["popmon_run = popmon.pipeline.amazing_pipeline:run"]
        },
    )


if __name__ == "__main__":
    setup_package()
