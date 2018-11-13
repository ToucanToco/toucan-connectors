# TODO
# I will test this with a container, probably using this
# https://hub.docker.com/r/magento/magento2devbox-web/

from toucan_connectors.magento.magento_connector import MagentoConnector, MagentoDataSource


def test_get_df(mocker):

    mocker.patch('toucan_connectors.magento.magento_connector.API.call')

    data_source = MagentoDataSource(domain='test', name='magento', resource_path='orders_sale',
                                    arguments=[[{'status': 'pending'}]])

    provider = MagentoConnector(type='Magento', name='magento', url='https://exemple.com',
                                username='pierre', password='test')

    cmock = mocker.patch('toucan_connectors.magento.magento_connector.API')
    cmock.return_value.__enter__.return_value.call.return_value = [[1, 1], [1, 1]]
    df = provider.get_df(data_source)

    assert df.shape == (2, 2)
    cmock.assert_called_with(provider.url, provider.username, provider.password)
    cmock.return_value.__enter__.return_value.call.assert_called_with(
        'orders_sale', [{'status': 'pending'}])
