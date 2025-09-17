from setuptools import find_packages, setup

with open('README.rst') as f:
    long_description = f.read()

packages = ['aqueduct']

required_setup = [
    'cffi>=1.13.0,<2.0.0',
    'setuptools>=42.0.0,<81.0.0',
]

required = [
    'psutil==5.9.4',
]

extras = {
    'aiohttp': [
        'aiohttp',
    ],
    'numpy': [
        'numpy'
    ]
}

setup(
    name='aqueduct',
    packages=find_packages(),
    version='1.12.0',
    license='MIT',
    license_files='LICENSE.txt',
    author='Data Science SWAT',
    author_email='UnitDataScienceSwat@avito.ru',
    description='Builder for performance-efficient prediction.',
    url='https://github.com/avito-tech/aqueduct',
    download_url='https://github.com/avito-tech/aqueduct/archive/refs/heads/main.zip',
    keywords=['datascience', 'learning', ],
    python_requires='>=3.8',
    include_package_data=True,
    long_description=long_description,
    long_description_content_type='text/x-rst',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development :: Build Tools',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Operating System :: OS Independent',
    ],
    cffi_modules=["atomic_build.py:ffibuilder"],
    setup_requires=required_setup,
    install_requires=required,
    extras_require=extras,
)
