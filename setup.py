
from setuptools import setup

setup(
    name='cfde_deriva',
    description='CFDE Deriva Integration Utilities',
    version='0.4',
    packages=[
        'cfde_deriva',
    ],
    package_data={},
    scripts=[],
    requires=['deriva'],
    install_requires=['deriva>=1.0'],
    maintainer_email='support@misd.isi.edu',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ])
