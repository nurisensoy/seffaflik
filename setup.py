from distutils.core import setup

setup(
    name='seffaflik',
    packages=['seffaflik'],
    version='0.0.0',
    license='MIT',
    description='seffaflik, seffaflik.epias.com.tr adresinde yayımlanan verilerin kolaylıkla okunmasını sağlamaktadır.',
    author='Dr. Nuri Şensoy',
    author_email='nurisensoy87@gmail.com',
    url='https://github.com/nurisensoy/seffaflik',
    download_url='https://github.com/nurisensoy/seffaflik/archive/v0.0-alpha.tar.gz',
    keywords=['seffaflik', 'transparency', 'Turkish Electricity Market', "TURKEY"],
    install_requires=[
        'requests',
        'pandas',
        'python-dateutil',
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)