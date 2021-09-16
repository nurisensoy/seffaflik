from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="seffaflik",
    version="0.0.15",
    author='Dr. Nuri Şensoy',
    author_email='nuri.sensoy@enerjisauretim.com.tr',
    description="EPİAŞ tarafından Şeffaflık Platformunda yayımlanmakta olan verileri çekmek için tasarlanmış Python "
                "kütüphanesi",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords=['seffaflik', 'transparency', 'Turkish Energy Markets'],
    url='https://github.com/nurisensoy/seffaflik',
    packages=find_packages(),
    install_requires=[
        'requests',
        'pandas',
        'python-dateutil',
    ],
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
