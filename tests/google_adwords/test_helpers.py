from unittest import mock

from toucan_connectors.google_adwords.helpers import apply_filter, clean_columns


def test_apply_filter(mocker):
    """Check that apply_filter is able to correctly build a query with the
    given filter_dict
    """
    list_of_filter_dict_keys = [
        "EqualTo",
        "Contains",
        "ContainsAll",
        "ContainsAny",
        "ContainsIgnoreCase",
        "DoesNotContain",
        "GreaterThan",
        "GreaterThanOrEqualTo",
        "DoesNotContainIgnoreCase",
        "In",
        "LessThan",
        "LessThanOrEqualTo",
        "ContainsNone",
        "ContainsNone",
        "NotIn",
        "NotEqualTo",
        "StartsWith",
        "StartsWithIgnoreCase",
    ]
    mocked_query_builder = mock.Mock()

    for f in list_of_filter_dict_keys:
        apply_filter(mocked_query_builder, {"foo": {"operator": f, "value": "bar"}})
    assert mocked_query_builder.Where.call_count == 18
    assert mocked_query_builder.Where().EqualTo.call_count == 1
    assert mocked_query_builder.Where().LessThan.call_count == 1
    assert mocked_query_builder.Where().StartsWithIgnoreCase.call_count == 1


def test_clean_columns():
    """
    Check that clean colum correctly returns a list of
    column names with lowered first letter
    """
    assert clean_columns("Id, AdCampaignId, CampaignId") == ["id", "adCampaignId", "campaignId"]
