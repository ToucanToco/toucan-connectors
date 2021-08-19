from enum import Enum


class HubspotDataset(str, Enum):
    contacts = 'contacts'
    companies = 'companies'
    deals = 'deals'
    products = 'products'
    # webanalytics = 'web-analytics'
    # emails_events = 'emails-events'


class HubspotObjectType(str, Enum):
    contact = 'contact'
