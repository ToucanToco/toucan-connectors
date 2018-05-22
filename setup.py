from setuptools import setup, find_packages

extras_require = {
    'azure_mssql': ['pymssql>=2.1.3'],
    'google_cloud_mysql': ['PyMySQL>=0.8.0'],
    'microstrategy': ['requests'],
    'mongo': ['pymongo>=3.6.1'],
    'mssql': ['pymssql>=2.1.3'],
    'mysql': ['PyMySQL>=0.8.0'],
    'oracle_sql': ['cx_Oracle>=6.2.1'],
    'postgres': ['psycopg2>=2.7.4'],
    'sap_hana': ['pyhdb>=0.3.4'],
    'snowflake': ['snowflake-connector-python'],
    'dataiku': ['dataiku-api-client'],
    'google_spreadsheet': ['gspread>=3', 'google-api-python-client>=1.6.7'],
}
extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

install_requires = [
    'toucan_data_sdk',
    'pydantic'
]

classifiers = [
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Programming Language :: Python :: 3.6'
]

setup(name='toucan_connectors',
      version='0.0.14',
      description='Toucan Toco Connectors',
      author='Toucan Toco',
      author_email='dev@toucantoco.com',
      url='https://github.com/ToucanToco/toucan-connectors',
      license='BSD',
      classifiers=classifiers,
      packages=find_packages(),
      install_requires=install_requires,
      extras_require=extras_require,
      include_package_data=True)
