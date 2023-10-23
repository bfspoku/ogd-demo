"""Start the http server in /testdata.
"""

import http.server
import os
from http.server import test, SimpleHTTPRequestHandler

os.chdir('./testdata')
test(SimpleHTTPRequestHandler)