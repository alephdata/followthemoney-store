from setuptools import setup, find_packages

with open('README.md') as f:
    long_description = f.read()


setup(
    name='followthemoney-store',
    version='2.1.4',
    description="Store raw and structured FollowTheMoney data from "
                "different datasets in a data lake",
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    author='Organized Crime and Corruption Reporting Project',
    author_email='data@occrp.org',
    url='http://github.com/alephdata/followthemoney-store',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'tests']),
    include_package_data=True,
    zip_safe=True,
    install_requires=[
        'followthemoney>=1.31.1',
        'SQLAlchemy>=1.3.1'
    ],
    extras_require={
        'postgresql': [
            'psycopg2-binary>=2.7',
        ],
    },
    tests_require=[],
    entry_points={
        'console_scripts': [
            'ftm-store = ftmstore.cli:cli',
        ],
        'followthemoney.cli': {
            'store = ftmstore.cli:cli',
        },
        'memorious.operations': [
            'balkhash_put = ftmstore.memorious:ftm_store',
            'ftm_store = ftmstore.memorious:ftm_store',
            'ftm_load_aleph = ftmstore.memorious:ftm_load_aleph',
        ]
    }
)
