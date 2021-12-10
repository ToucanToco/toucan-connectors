# Net Explorer connector

Import data from Excel and CSV hosted on Net Explorer.

## Data provider configuration

* `type`: `NetExplorer`
* `name`: str, required

```javascript
DATA_PROVIDERS: [
  type:         'NetExplorer'
  name:         '<name>'
]
```

## Data source configuration

* `domain`: str, required
* `name`: str, required
* `file`: str, required. Relative path file
* `sheet`: str, optional. Defautl is the first sheet

```javascript
DATA_SOURCES: [
  domain: '<domain>',
  name: '<name>',
  file: '<file>',
  sheet: '<sheet>',
]
```
