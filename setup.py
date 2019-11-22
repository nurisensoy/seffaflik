from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="seffaflik",
    version="v0.0.1",
    author='Dr. Nuri Şensoy',
    author_email='nurisensoy87@gmail.com',
    description="seffaflik.epias.com.tr",
    keywords=['seffaflik', 'transparency', 'Turkish Electricity Market', "TURKEY"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/nurisensoy/seffaflik',
    packages=find_packages(),
    install_requires=[
        'requests',
        'pandas',
        'python-dateutil',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
