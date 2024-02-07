from setuptools import setup, find_packages
from cs230_common import __author__, __version__

setup(
    name='cs230_common',
    version=__version__,
    description='Common library',
    author=__author__,
    author_email='qqaatw.cs230@gmail.com',
    packages=find_packages(exclude=['examples']),  # Automatically find packages under the 'src' directory
    python_requires='>=3.10',  # Specify the minimum Python version required
    install_requires=[  # List dependencies required by your package
        'pika',
    ],
)