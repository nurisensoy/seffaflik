from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="seffaflik",
    version="v0.0.4.dev1",
    author='Dr. Nuri Şensoy',
    author_email='nuri.sensoy@epias.com.tr',
    description="EPİAŞ tarafından Şeffaflık Platformunda yayımlanmakta olan verileri çekmek için tasarlanmış Python "
                "kütüphanesi",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords=['seffaflik', 'transparency', 'Turkish Electricity Market'],
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
