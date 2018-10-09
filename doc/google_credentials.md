# GoogleCredentials
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


If you're on Toucan Toco's cloud and your application can be automatically deployed via package,
credentials are already set up for service account `toucanserviceaccount@testproj-204816.iam.gserviceaccount.com`.
In order to use these credentials in your ETL config file, you can use the syntax `secrets.google_spreadsheet_credentials.xxx`.

#### :warning: Note : Be sure to use the option `escape_newlines` for the `private_key` field.

Example:

    DATA_PROVIDERS: [
        name: "<gsheet_provider>"
        type: "GoogleSpreadsheet"
        credentials:
          type: '{{ secrets.google_credentials.type }}'
          project_id: '{{ secrets.google_credentials.project_id }}'
          private_key_id: '{{ secrets.google_credentials.private_key_id }}'
          private_key: '{{ secrets.google_credentials.private_key | escape_newlines }}'
          client_email: '{{ secrets.google_credentials.client_email }}'
          client_id: '{{ secrets.google_credentials.client_id }}'
          auth_uri: '{{ secrets.google_credentials.auth_uri }}'
          token_uri: '{{ secrets.google_credentials.token_uri }}'
          auth_provider_x509_cert_url: '{{ secrets.google_credentials.auth_provider_x509_cert_url }}'
          client_x509_cert_url: '{{ secrets.google_credentials.client_x509_cert_url }}'
    ]