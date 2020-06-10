"""
Provide a trello connector.

Check https://developers.trello.com/docs/get-started for the official API documentation.

The connector allow you to list in a dataset all the cards of a board with a chosen set of 'fields'.
The available fields are listed in the `Field ` object.

The dataset has one line by card and one columns by fields
"""

from enum import Enum
from typing import List

import pandas as pd
import requests

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


def list_function_handler(card_custom_field, custom_field):
    list_of_options = {x['id']: x for x in custom_field['options']}
    option = list_of_options[card_custom_field['idValue']]
    return option['value']['text']


CUSTOM_FIELD_GET_VALUE = {
    # How we retrieve the value given a custom field type
    'number': lambda card_custom_field, _: float(card_custom_field['value']['number']),
    'text': lambda card_custom_field, _: card_custom_field['value']['text'],
    'date': lambda card_custom_field, _: (card_custom_field['value']['date']),
    'checkbox': lambda card_custom_field, _: card_custom_field['value']['checked'] == 'true',
    'list': list_function_handler,
}

API_URL = 'https://api.trello.com/1/boards'


class Fields(str, Enum):
    name = 'name'
    url = 'url'
    lists = 'lists'
    members = 'members'
    labels = 'labels'


class CardsFilter(str, Enum):
    """
    A filter to match cards in a specific state on a board.

    https://developer.atlassian.com/cloud/trello/rest/#api-boards-id-cards-filter-get
    """

    all = 'all'
    closed = 'closed'
    open = 'open'
    visible = 'visible'
    none = 'none'


class TrelloDataSource(ToucanDataSource):
    board_id: str
    fields_list: List[Fields] = list(Fields.__members__)
    custom_fields: bool = True
    filter: CardsFilter = CardsFilter.open


class TrelloConnector(ToucanConnector):
    data_source_model: TrelloDataSource

    key_id: str = None
    token: str = None

    def get_board(self, path, **custom_params):
        return requests.get(
            f'{API_URL}/{path}', params={'key': self.key_id, 'token': self.token, **custom_params}
        ).json()

    @staticmethod
    def replace_id_by_value(
        card_with_id,
        lists_ids_mapping=None,
        labels_id_mapping=None,
        members_id_mapping=None,
        custom_fields_id_mapping=None,
    ):
        """
        `card_with_id` is a dictionary containing all data of a card,
        but with unreadlable id instead of value
        This fonction return `card_with_value` with the same dictionnary
        as `card_with_id` but with readable value

        `lists_ids_mapping`: dictionnary of correspondance between list names and ids
        `labels_id_mapping`: dictionnary of correspondance between label names and ids
        `members_id_mapping`: dictionnary of correspondance between members names and ids
        `custom_fields_id_mapping`: dictionnary of correspondance between custom field
        and there representation
        """

        # id, name and url fields do not need to translate from id to value
        card_with_value = {'id': card_with_id['id']}

        if 'name' in card_with_id:
            card_with_value['name'] = card_with_id['name']
        if 'url' in card_with_id:
            card_with_value['url'] = card_with_id['url']

        # lists, members and labels need to translate from a id to a value
        if lists_ids_mapping:
            card_with_value['lists'] = lists_ids_mapping[card_with_id['idList']]
        if members_id_mapping:
            card_with_value['members'] = [
                members_id_mapping[member] for member in card_with_id['idMembers']
            ]
        if labels_id_mapping:
            card_with_value['labels'] = [
                labels_id_mapping[label['id']] for label in card_with_id['labels']
            ]

        # custom fields
        if custom_fields_id_mapping:
            for card_custom_field in card_with_id['customFieldItems']:
                custom_field = custom_fields_id_mapping[card_custom_field['idCustomField']]
                get_value = CUSTOM_FIELD_GET_VALUE[custom_field['type']]
                card_with_value[custom_field['name']] = get_value(card_custom_field, custom_field)

        return card_with_value

    def _retrieve_data(self, data_source: TrelloDataSource) -> pd.DataFrame:
        # get board caracteristics
        # the following dictionaries are of the form:
        # - keys: id of field
        # - values: readable value of field

        fields_for_request = []
        lists_ids_mapping = labels_id_mapping = members_id_mapping = custom_fields_id_mapping = None

        if 'name' in data_source.fields_list:
            fields_for_request += ['name']
        if 'url' in data_source.fields_list:
            fields_for_request += ['url']
        if 'lists' in data_source.fields_list:
            fields_for_request += ['idList']
            lists_ids_mapping = {
                x['id']: x['name']
                for x in self.get_board(f'{data_source.board_id}/lists', fields='name')
            }
        if 'labels' in data_source.fields_list:
            fields_for_request += ['labels']
            labels_id_mapping = {
                x['id']: x['name']
                for x in self.get_board(f'{data_source.board_id}/labels', fields='name')
            }
        if 'members' in data_source.fields_list:
            fields_for_request += ['idMembers']
            members_id_mapping = {
                x['id']: x['fullName']
                for x in self.get_board(f'{data_source.board_id}/members', fields='fullName')
            }

        if data_source.custom_fields:
            custom_fields_id_mapping = {
                x['id']: x
                for x in self.get_board(f'{data_source.board_id}/customFields', fields='name')
            }

        # get cards
        cards_with_id = self.get_board(
            f'{data_source.board_id}/cards',
            fields=fields_for_request,
            customFieldItems='true' if data_source.custom_fields else 'false',
            filter=data_source.filter,
        )

        # replace all id in `cards_with_id` by the corresponding readable value
        cards_with_value = [
            self.replace_id_by_value(
                card_with_id,
                lists_ids_mapping,
                labels_id_mapping,
                members_id_mapping,
                custom_fields_id_mapping,
            )
            for card_with_id in cards_with_id
        ]

        return pd.DataFrame(cards_with_value)
