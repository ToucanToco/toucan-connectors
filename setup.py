from setuptools import setup, find_packages

extras_require = {
    'adobe': ['adobe_analytics'],
    'azure_mssql': ['pymssql>=2.1.3'],
    'dataiku': ['dataiku-api-client'],
    'google_analytics': ['google-api-python-client'],
    'google_big_query': ['pandas_gbq'],
    'google_cloud_mysql': ['PyMySQL>=0.8.0'],
    'google_spreadsheet': ['gspread>=3', 'oauth2client'],
    'hive': ['pyhive[hive]'],
    'http_api': ['requests', 'requests_oauthlib', 'jq', 'oauthlib'],
    'magento': ['magento'],
    'micro_strategy': ['requests'],
    'mongo': ['pymongo>=3.6.1'],
    'mssql': ['pymssql>=2.1.3'],
    'mysql': ['PyMySQL>=0.8.0'],
    'odata': ['odata', 'requests_oauthlib', 'oauthlib'],
    'oracle_sql': ['cx_Oracle>=6.2.1'],
    'postgres': ['psycopg2>=2.7.4'],
    'sap_hana': ['pyhdb>=0.3.4'],
    'snowflake': ['snowflake-connector-python'],
    'toucan_toco': ['toucan_client']
}
extras_require['all'] = sorted(set(sum(extras_require.values(), [])))

install_requires = [
    'toucan_data_sdk',
    'pydantic==0.9.1'
]

dependency_links = [
    'https://github.com/ToucanToco/python-odata/tarball/master#egg=odata-0'
]

classifiers = [
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Programming Language :: Python :: 3.6'
]

setup(name='toucan_connectors',
      version='0.9.0',
      description='Toucan Toco Connectors',
      author='Toucan Toco',
      author_email='dev@toucantoco.com',
      url='https://github.com/ToucanToco/toucan-connectors',
      license='BSD',
      classifiers=classifiers,
      packages=find_packages(),
      install_requires=install_requires,
      extras_require=extras_require,
      dependency_links=dependency_links,
      include_package_data=True)
