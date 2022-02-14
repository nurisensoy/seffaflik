from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Get the version from the source code
with open(path.join(here, 'seffaflik', '__init__.py'), encoding='utf-8') as f:
    lines = f.readlines()
    for l in lines:
        if l.startswith('__version__'):
            __version__ = l.split('"')[1]  # take the part after the first "

setup(
    name="seffaflik",
    version=__version__,
    author='Dr. Nuri Şensoy',
    author_email='nurisensoy87@gmail.com',
    description="EPİAŞ tarafından Şeffaflık Platformunda yayımlanmakta olan verileri çekmek için tasarlanmış Python "
                "kütüphanesi",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords=['seffaflik', 'transparency', 'Turkish Energy Markets'],
    url='https://github.com/nurisensoy/seffaflik',
    packages=find_packages(),

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed.
    install_requires=['requests', 'pandas', 'python-dateutil', 'beautifulsoup4', 'xlrd'],
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    package_data={
        'seffaflik': ['LICENSE', 'README.md'],
    },
)
