from setuptools import find_packages, setup

with open('README.rst') as f:
    long_description = f.read()

packages = ['aqueduct']

setup(
    name='aqueduct',
    packages=find_packages(),
    version='1.7.1',
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
)
