"""
Provide a trello connector.

Check https://developers.trello.com/docs/get-started for the official API documentation.

The connector allow you to list in a dataset all the cards of a board with a chosen set of "fields".
The available fields are listed in the `Field ` object.

The dataset has one line by card and one columns by fields
"""

import pandas as pd
from typing import List
from enum import Enum
import requests
import warnings

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource


class Fields(str, Enum):
    name = 'name'
    url = 'url'
    lists = 'lists'
    members = 'members'
    labels = 'labels'


class TrelloDataSource(ToucanDataSource):
    board_id: str
    fields_list: List[Fields] = [Fields.name, Fields.url,
                                 Fields.lists, Fields.members,  Fields.labels]
    custom_fields: bool = True


class TrelloConnector(ToucanConnector):
    type = "Trello"
    data_source_model: TrelloDataSource

    key_id: str = ''
    token: str = ''

    def get_board(self, path, **customParams):
        return requests.get(f'https://api.trello.com/1/boards/{path}', params={
            'key': self.key_id,
            'token': self.token, **customParams
        }).json()

    def replace_id_by_value(self, card_with_id, lists=None, labels=None,
                            members=None, custom_fields=None):
        """
        `card_with_id` is a dictionary containing all data of a card,
        but with unreadlable id instead of value
        This fonction return `card_with_value` with the same dictionnary
        as `card_with_id` but with readable value

        `lists`: dictionnary of correspondance between list names and ids
        `members`: dictionnary of correspondance between members names and ids
        `custom_fields`: dictionnary of correspondance between custom field and there representation
        """
        card_with_value = {}

        # id, name and url fields do not need to translate from id to value
        card_with_value["id"] = card_with_id["id"]

        if 'name' in card_with_id.keys():
            card_with_value['name'] = card_with_id['name']
        if 'url' in card_with_id.keys():
            card_with_value['url'] = card_with_id['url']

        # lists, members and labels need to translate from a id to a value
        if lists:
            card_with_value['lists'] = lists[card_with_id['idList']]
        if members:
            card_with_value['members'] = [members[member] for member in card_with_id['idMembers']]
        if labels:
            card_with_value['labels'] = [labels[label['id']] for label in card_with_id['labels']]

        # custom fields
        if custom_fields:
            for card_custom_field in card_with_id['customFieldItems']:
                custom_field = custom_fields[card_custom_field['idCustomField']]
                if custom_field['type'] == 'number':
                    card_with_value[custom_field['name']] = float(
                        card_custom_field['value']['number'])
                elif custom_field['type'] == 'text':
                    card_with_value[custom_field['name']] = card_custom_field['value']['text']
                elif custom_field['type'] == 'date':
                    card_with_value[custom_field['name']] = card_custom_field['value']['date']
                elif custom_field['type'] == 'checkbox':
                    if card_custom_field['value']['checked'] == 'true':
                        card_with_value[custom_field['name']] = True
                elif custom_field['type'] == 'list':
                    list_of_options = {x["id"]: x for x in custom_field['options']}
                    option = list_of_options[card_custom_field['idValue']]
                    card_with_value[custom_field['name']] = option['value']['text']
                else:
                    warnings.warn(f"""The custom field {custom_field['name']} used into \
                    your board is of type {custom_field['type']}.
                    This type is not handled by this app and is simply ignored.""")

        return card_with_value

    def get_df(self, data_source: TrelloDataSource) -> pd.DataFrame:
        # get board caracteristics
        # the following dictionaries are of the form:
        # - keys: id of field
        # - values: readable value of field

        fields_for_request = []
        lists, labels,  members, custom_fields = None, None, None, None

        if 'name' in data_source.fields_list:
            fields_for_request += ['name']
        if 'url' in data_source.fields_list:
            fields_for_request += ['url']
        if 'lists' in data_source.fields_list:
            fields_for_request += ['idList']
            lists = {x['id']: x['name']
                     for x in self.get_board(f'{data_source.board_id}/lists', fields='name')}
        if 'labels' in data_source.fields_list:
            fields_for_request += ['labels']
            labels = {x['id']: x['name']
                      for x in self.get_board(f'{data_source.board_id}/labels', fields='name')}
        if 'members' in data_source.fields_list:
            fields_for_request += ['idMembers']
            members = {x['id']: x['fullName']
                       for x in self.get_board(f'{data_source.board_id}/members', fields='fullName')}

        if data_source.custom_fields:
            custom_fields = {x['id']: x for x in self.get_board(
                f'{data_source.board_id}/customFields', fields='name')}

        # get cards
        cards_with_id = self.get_board(
            path=f'{data_source.board_id}/cards',
            fields=fields_for_request,
            customFieldItems='true' if data_source.custom_fields else 'false'
        )

        # replace all id in `cards_with_id` by the corresponding readable value
        cards_with_value = [self.replace_id_by_value(card_with_id, lists,
                                                     labels, members, custom_fields)
                            for card_with_id in cards_with_id]

        data = pd.DataFrame(cards_with_value)

        return data
