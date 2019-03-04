import logging
import os
import shutil
import tempfile
import zipfile
import pandas as pd
import joblib
from toucan_client import ToucanClient
from .utils.helpers import slugify
from .utils.traceback import load_traceback


logger = logging.getLogger(__name__)


class ToucanDataSdk:
    def __init__(self, instance_url, auth, small_app=None, stage="staging"):
        instance_url = instance_url.strip().rstrip('/')
        if small_app is None:
            small_app = instance_url.split('/')[-1]
            instance_url = '/'.join(instance_url.split('/')[:-1])
        self.small_app_url = instance_url + (('/' + small_app) if small_app else '')
        self.client = ToucanClient(self.small_app_url, auth=auth, stage=stage)
        self.EXTRACTION_CACHE_PATH = os.path.join(
            'extraction_cache',
            slugify(instance_url, separator='_'),
            small_app
        )

    def get_datasources(self, domains=None):
        if domains is not None and isinstance(domains, list):
            dfs = {}
            domains_cache = [domain for domain in domains
                             if self.cache_exists(domain)]
            domains_sdk = list(set(domains) - set(domains_cache))

            if len(domains_cache) > 0:
                dfs.update(self.read_from_cache(domains_cache))
            if len(domains_sdk) > 0:
                dfs.update(self.read_datasources_from_sdk(domains_sdk))
        else:
            if self.cache_exists():
                dfs = self.read_from_cache()
            else:
                dfs = self.read_datasources_from_sdk()
        return dfs

    # alias
    get_dfs = get_datasources

    def get_domains(self, domains=None):
        if domains is None:
            domains = [meta["domain"] for meta in self.client.metadata.get().json()]
        domains_cache = [domain for domain in domains if self.cache_exists(domain)]
        domains_sdk = [domain for domain in domains if domain not in domains_cache]
        if len(domains_cache) > 0:
            dfs = self.read_from_cache(domains_cache)
        if len(domains_sdk) > 0:
            dfs = self.read_domains_from_sdk(domains_sdk)
        return dfs

    def invalidate_cache(self, domains=None):
        if domains is not None and isinstance(domains, list):
            for domain in domains:
                try:
                    os.remove(os.path.join(self.EXTRACTION_CACHE_PATH, domain))
                except OSError:
                    pass
        else:
            try:
                shutil.rmtree(self.EXTRACTION_CACHE_PATH)
            except (OSError, IOError) as e:  # For Python 2.7+ compatibility
                logger.error('failed to remove cache for : ' + str(e))

    def get_augment(self):
        return self.client.config.augment.get().text

    def get_etl(self):
        return self.client.config.etl.get().json()

    def query_basemaps(self, query):
        if isinstance(query, dict):
            return self.client.basemaps.post(json=query).json()
        else:
            raise InvalidQueryError(f'Query {query} should be a dict, {type(query)} found.')

    def read_datasources_from_sdk(self, domains=None):
        # Extract all domains if domains_sdk is null
        resp = self.client.sdk.post(json={'domains': domains})
        resp.raise_for_status()
        dfs = extract(resp.content)
        self.write(dfs)
        logger.info(f'Data {domains} fetched and cached')
        return dfs

    def read_domains_from_sdk(self, domains):
        dfs = {}
        for domain in domains:
            # first page
            data = self.client.output_domain[domain].post().json()
            rows = data['result']
            last_doc_id = data['lastDocId']

            # next pages
            while last_doc_id:
                data = self.client.output_domain[domain][last_doc_id].post().json()
                rows += data['result']
                last_doc_id = data['lastDocId']
            df = pd.DataFrame.from_dict(rows).drop(columns='_id')
            dfs[domain] = df
        self.write(dfs)
        return dfs

    def read_from_cache(self, domains=None):
        """
        Returns:
            dict: Dict[str, DataFrame]
        """
        logger.info(f'Reading data from cache ({self.EXTRACTION_CACHE_PATH})')
        if domains is not None and isinstance(domains, list):
            dfs = {domain: self.read_entry(domain) for domain in domains}
        else:
            dfs = {name: self.read_entry(name)
                   for name in os.listdir(self.EXTRACTION_CACHE_PATH)}
        return dfs

    def read_entry(self, file_name):
        """
        Args:
            file_name (str):

        Returns:
            pd.DataFrame:
        """
        file_path = os.path.join(self.EXTRACTION_CACHE_PATH, file_name)
        logger.info(f'Reading cache entry: {file_path}')
        return joblib.load(file_path)

    def write(self, dfs):
        """
        Args:
            data (str | byte):

        Returns:
            dict: Dict[str, DataFrame]
        """
        if not os.path.exists(self.EXTRACTION_CACHE_PATH):
            os.makedirs(self.EXTRACTION_CACHE_PATH)

        for name, df in dfs.items():
            file_path = os.path.join(self.EXTRACTION_CACHE_PATH, name)
            joblib.dump(df, filename=file_path)
            logger.info(f'Cache entry added: {file_path}')

    def cache_exists(self, domain=None):
        if domain is not None:
            path = os.path.join(self.EXTRACTION_CACHE_PATH, domain)
            return os.path.exists(path) and os.path.isfile(path)
        else:
            path = self.EXTRACTION_CACHE_PATH
            return os.path.exists(path) and os.path.isdir(path)

    def load_latest_traceback(self):
        tb = self.client.tracebacks.latest.get()
        if tb.ok is False:
            return tb.json()
        else:
            with open('.tb.dump', 'wb') as f:
                f.write(tb.content)
            return load_traceback('.tb.dump')


def extract_zip(zip_file_path):
    """
    Returns:
        dict: Dict[str, DataFrame]
    """
    dfs = {}
    with zipfile.ZipFile(zip_file_path, mode='r') as z_file:
        names = z_file.namelist()
        for name in names:
            content = z_file.read(name)
            _, tmp_file_path = tempfile.mkstemp()
            try:
                with open(tmp_file_path, 'wb') as tmp_file:
                    tmp_file.write(content)

                dfs[name] = joblib.load(tmp_file_path)
            finally:
                shutil.rmtree(tmp_file_path, ignore_errors=True)
    return dfs


def extract(data):
    """
    Args:
        data (str | byte):

    Returns:
        dict: Dict[str, DataFrame]

    """
    _, tmp_file_path = tempfile.mkstemp()
    try:
        with open(tmp_file_path, 'wb') as tmp_file:
            tmp_file.write(data)

        if zipfile.is_zipfile(tmp_file_path):
            return extract_zip(tmp_file_path)
        else:
            raise DataSdkError('Unsupported file type')
    finally:
        shutil.rmtree(tmp_file_path, ignore_errors=True)


class DataSdkError(Exception):
    pass


class InvalidQueryError(Exception):
    pass
