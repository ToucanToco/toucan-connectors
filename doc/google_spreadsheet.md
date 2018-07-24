# GoogleSpreadsheet connector

##### Share the spreadsheet

 Unless the spreadsheet is public, you will have to manually share it.

 Open the google spreadsheet inside your web browser. Inside the File menu, there a
 Share option. Click on it and enter the email address of your service account. 
 
 If you are on Toucan Toco's cloud, it is:
 ```toucanserviceaccount@testproj-204816.iam.gserviceaccount.com
 ```


## Data provider configuration

* `type`: `"GoogleSpreadsheet"`
* `name`: str, required
* `credentials`: GoogleCredentials (see below), required
* `scope`: str, default to ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets', 'https://spreadsheets.google.com/feeds']

```coffee
DATA_PROVIDERS= [
  type:    'GoogleSpreadsheet'
  name:    '<name>'
  credentials:    '<credentials>'
  scope:    '<scope>'
,
  ...
]
```

### GoogleCredentials
For authentication, download an authentication file from console.developper.com
    and use the values here. This is an oauth2 credential file. For more information
    see this: http://gspread.readthedocs.io/en/latest/oauth2.html

* `type`: str
* `project_id`: str
* `private_key_id`: str
* `private_key`: str
* `client_email`: str
* `client_id`: str
* `auth_uri`: str
* `token_uri`: str
* `auth_provider_x509_cert_url`: str
* `client_x509_cert_url`: str


If you're on Toucan Toco's cloud and your application can be automatically deployed via package, credentials are already set up for service account `toucanserviceaccount@testproj-204816.iam.gserviceaccount.com`. In order to use these credentials in your ETL config file, you can use the syntax `secrets.google_spreadsheet_credentials.xxx`.

```eval_rst
.. note::

  Be sure to use the option `escape_newlines` for the `private_key` field.
```

Example:

    DATA_PROVIDERS: [
        name: "<gsheet_provider>"
        type: "GoogleSpreadsheet"
        credentials:
          type: '{{ secrets.google_spreadsheet_credentials.type }}'
          project_id: '{{ secrets.google_spreadsheet_credentials.project_id }}'
          private_key_id: '{{ secrets.google_spreadsheet_credentials.private_key_id }}'
          private_key: '{{ secrets.google_spreadsheet_credentials.private_key | escape_newlines }}'
          client_email: '{{ secrets.google_spreadsheet_credentials.client_email }}'
          client_id: '{{ secrets.google_spreadsheet_credentials.client_id }}'
          auth_uri: '{{ secrets.google_spreadsheet_credentials.auth_uri }}'
          token_uri: '{{ secrets.google_spreadsheet_credentials.token_uri }}'
          auth_provider_x509_cert_url: '{{ secrets.google_spreadsheet_credentials.auth_provider_x509_cert_url }}'
          client_x509_cert_url: '{{ secrets.google_spreadsheet_credentials.client_x509_cert_url }}'
    ]


## Data source configuration

* `domain`: str, required
* `name`: str, required. Should match the data provider name 
* `spreadsheet_id`: str, required. Id of the spreadsheet which can be found inside
the url: https://docs.google.com/spreadsheets/d/<spreadsheet_id_is_here>/edit?pref=2&pli=1#gid=0,
* `sheetname`: str. By default, the extractor return the first sheet.


```coffee
DATA_SOURCES= [
  domain:    '<domain>'
  name:    '<name>'
  spreadsheet_id:    '<spreadsheet_id>'
  sheetname:    '<sheetname>'
,
  ...
]
```
