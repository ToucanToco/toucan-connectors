from setuptools import setup, find_packages

google_credentials_deps = ['requests', 'requests_oauthlib', 'oauthlib']
extras_require = {
    'adobe': ['adobe_analytics'],
    'azure_mssql': ['pymssql>=2.1.3'],
    'dataiku': ['dataiku-api-client'],
    'google_analytics': google_credentials_deps + ['google-api-python-client', 'oauth2client'],
    'google_big_query': google_credentials_deps + ['pandas_gbq'],
    'google_cloud_mysql': google_credentials_deps + ['PyMySQL>=0.8.0'],
    'google_spreadsheet': google_credentials_deps + ['gspread>=3', 'oauth2client'],
    'hive': ['pyhive[hive]'],
    'http_api': ['jq', 'oauthlib', 'requests', 'requests_oauthlib'],
    'magento': ['magento'],
    'micro_strategy': ['requests'],
    'mongo': ['pymongo>=3.6.1'],
    'mssql': ['pymssql>=2.1.3'],
    'mysql': ['PyMySQL>=0.8.0'],
    'odata': ['jq', 'oauthlib', 'requests_oauthlib', 'tctc_odata'],
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

classifiers = [
    'Intended Audience :: Developers',
    'License :: OSI Approved :: BSD License',
    'Programming Language :: Python :: 3.6'
]

setup(name='toucan_connectors',
      version='0.10.1',
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
