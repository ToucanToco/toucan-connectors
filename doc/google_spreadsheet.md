# GoogleSpreadsheet connector

##### Share the spreadsheet

 Unless the spreadsheet is public, you will have to manually share it.

 Open the google spreadsheet inside your web browser. Inside the File menu, there a
 Share option. Click on it and enter the email address of your service account. 
 
 If you are on Toucan Toco's cloud, it is:
 ```
 toucanserviceaccount@testproj-204816.iam.gserviceaccount.com
 ```

## Data provider configuration

* `type`: `"GoogleSpreadsheet"`
* `name`: str, required
* `credentials`: [GoogleCredentials](google_credentials.md), required
* `scope`: str, default to ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets', 'https://spreadsheets.google.com/feeds']

```coffee
DATA_PROVIDERS: [
  type:    'GoogleSpreadsheet'
  name:    '<name>'
  credentials:    '<credentials>'
  scope:    '<scope>'
,
  ...
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, required. Should match the data provider name 
* `spreadsheet_id`: str, required. Id of the spreadsheet which can be found inside
the url: https://docs.google.com/spreadsheets/d/<spreadsheet_id_is_here>/edit?pref=2&pli=1#gid=0,
* `sheetname`: str. By default, the extractor return the first sheet.
* `skip_rows`: int, default to 0


```coffee
DATA_SOURCES: [
  domain:    '<domain>'
  name:    '<name>'
  spreadsheet_id:    '<spreadsheet_id>'
  sheetname:    '<sheetname>'
  skip_rows:    <skip_rows>
,
  ...
]
```
