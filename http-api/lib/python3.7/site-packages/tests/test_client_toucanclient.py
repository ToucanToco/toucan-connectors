from toucan_client.client import ToucanClient


def test_get(mocker):
    mock_get = mocker.patch('requests.get')
    small_app = ToucanClient('fake.route/my-small-app')

    small_app.config.etl.get()
    mock_get.assert_called_once_with('fake.route/my-small-app/config/etl')
    mock_get.reset_mock()

    small_app.config.etl.get(stage='staging')
    mock_get.assert_called_once_with(
        'fake.route/my-small-app/config/etl',
        params={'stage': 'staging'}
    )
