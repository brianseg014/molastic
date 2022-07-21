
from setuptools import setup, find_packages

from pathlib import Path
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='molastic',
    version='0.0.1',
    description='Library to easymock out elasticsearch for your tests',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Brian Estrada',
    author_email='brianseg014@gmail.com',
    packages=find_packages(),
    license='MIT',
    install_requires=[
        'requests-mock==1.9.3',
        'Shapely==1.7.1',
        'haversine==2.5.1',
        'pygeohash==1.2.0',
        'deepmerge==1.0.1',
        'furl==2.1.3',
        'ply==3.11',
        'pyjnius==1.4.1'
    ]
)