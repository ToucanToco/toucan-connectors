from setuptools import setup, find_packages

extras_require = {
    'azure_mssql': ['pymssql>=2.1.3'],
    'google_cloud_mysql': ['pymssql>=2.1.3'],
    'microstrategy': ['requests'],
    'mongo': ['pymongo>=3.6.1'],
    'mssql': ['pymssql>=2.1.3'],
    'mysql': ['PyMySQL>=0.8.0'],
    'oracle_sql': ['cx_Oracle>=6.2.1'],
    'postgres': ['psycopg2>=2.7.4'],
    'sap_hana': ['pyhdb>=0.3.4'],
    'snowflake': ['snowflake-connector-python']
}
extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

install_requires = [
    'toucan_data_sdk',
    'pydantic'
]

setup(name='toucan_connectors',
      version='0.0.11',
      description='Toucan Toco Connectors',
      author='Toucan Toco',
      author_email='dev@toucantoco.com',
      url='https://github.com/ToucanToco/toucan-connectors',
      license='BSD',
      packages=find_packages(),
      install_requires=install_requires,
      extras_require=extras_require,
      include_package_data=True)
