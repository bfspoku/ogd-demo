# ogd-demo
Demo of data retrieval using opendata.swiss API and a standard HTTP Server like Amazon AWS S3 as storage with HEAD request.


## Requirements

* Python 3.x
* pandas
* requests

See [requirements.txt](requirements.txt) for the virtual environment (venv). Recreate the environment with

```shell
$ python3 -m venv .venv
$ . .venv/bin/activate
$ pip install -r requirements.txt
```

## Usage

Edit harvester.py global variable BASEURL to suit your need. If you encounter issues with the proxy
settings, deactivate it.

### Local server
If you plan to use the local server to test, start a HTTP server where you have your mock API
(eidgenossische-wahlen-2023 file) and resources (*.json)
 ```shell
 $ python httpserver.py
 ```

### Harvester 
Start the harvester demo
```shell
$ python harvester.py
```

### Tests
* Try to update files and see what happens, either locally or on the remote storage.
* Try to change the resource URLs, either locally or on opendata.swiss

## Limitations

The HEAD Request works only on HTTP servers or end points where the Last-Modified header is returned. This won't work with services like the FSO DAM that is a Tomcat web app and returns data directly from a DB without Last-Modified. If you see any link looking like 
https://dam-api.bfs.admin.ch/hub/api/dam/assets/26965404/master, it won't be applicable
