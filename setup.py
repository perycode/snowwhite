# setup.py pip install .

from setuptools import setup, find_packages

setup(
    name="snowhite",
    version="0.1",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        '': ['data/*.json'],
    },
    entry_points={
        'console_scripts': [
            'datoreal=bin.instant_data:fetch_idata',
        ],
    },
)
