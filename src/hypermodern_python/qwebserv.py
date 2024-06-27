import os
from functools import partial
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from polars import DataFrame, Int32, Int64
import polars as pl
import urllib.parse
import typing
import tempfile

from src.hypermodern_python.queryprocessor import QueryProcessor

def start_web(query_processor: QueryProcessor, port: int):
    print("Starting Webserver port: ", port)
    handler = partial(QWebServ, query_processor)
    httpd = HTTPServer(('localhost', port), handler)
    httpd.serve_forever()

def to_dashtype(pltype: pl.DataType) -> str:
    if pltype.is_numeric():
        return "number"
    if pltype.is_temporal():
        if isinstance(pltype, pl.datatypes.classes.Date):
            return "DateOnly"
        elif isinstance(pltype, pl.datatypes.classes.Time):
            return "Time"
        else:
            return "Date"
    elif pltype.is_nested():
        return "numarray"
    return "string"


class QWebServ(BaseHTTPRequestHandler):
    def __init__(self, query_processor: QueryProcessor, *args, **kwargs):
        self.query_processor = query_processor
        # BaseHTTPRequestHandler calls do_GET **inside** __init__ !!!
        # So we have to call super().__init__ after setting attributes.
        super().__init__(*args, **kwargs)

    extensions = {
        "html": "text/html",
        "css": "text/css",
        "js": "text/javascript",
        "plain": "text/plain",
        "xls": "application/vnd.ms-excel",
        "csv": "text/comma-separated-values",
        "json": "application/json",
    }

    def set_content_type(self):
        extension = self.path.split(".")[-1]
        if extension in self.extensions:
            return self.extensions[extension]
        return self.extensions["plain"]

    def query(self, qr: str) -> pl.DataFrame:
        qry = urllib.parse.unquote(qr)
        return self.query_processor.query(qry)

    def set_headers(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        self.send_header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
        self.send_header("Access-Control-Allow-Headers", "X-Requested-With")

    @staticmethod
    def write_json(file:typing.BinaryIO, df:DataFrame):
        file.write(b'{"tbl":{"data":')
        df.write_json(file, row_oriented=True)
        ptypes = {}
        idx = 0
        for c in df.columns:
            ptypes[c] = to_dashtype(df.dtypes[idx])
            idx = idx + 1
        file.write(b', "types":')
        file.write(bytes(json.dumps(ptypes), 'utf-8'))
        file.write(b'}}')

    def do_POST(self):
        print("POST: ", self.path)
        self.set_headers()
        self.send_header('Content-type', self.extensions['json'])
        self.end_headers()
        # r = self.query(self.path)
        data_string = self.rfile.read(int(self.headers['Content-Length']))
        if self.headers['Content-type'] == self.extensions['json']:
            jsdict = json.loads(data_string)
            qry = jsdict['query']
        else:
            qry = data_string
        r = self.query(qry)
        print(r)
        self.write_json(self.wfile, r)

    def do_OPTIONS(self):
        # issue two requests, first one OPTIONS and then the GET request.
        # 501 Unsupported method ('OPTIONS')) caused by CORS and by requesting the "Content-Type: application/json; ...
        # To solve the error, I enabled CORS in do_OPTIONS and enabled clients to request a specific content type.
        self.set_headers()
        self.end_headers()

    def do_GET(self):
        p = 'html' + self.path
        print(p)
        if self.path == '/':
            p = 'html/index.html'

        if self.path.startswith("/?]"):
            r = self.query(self.path[3:])
            self.wfile.write(bytes(r._repr_html_(), 'utf-8'))
        elif (self.path.startswith("/file.csv?") or self.path.startswith("/t.csv?")
              or self.path.startswith("/file.xls?") or self.path.startswith("/t.xls?")):
            p = self.path.index("?")
            typ = self.path[p - 3:p]
            r = self.query(self.path[p + 1:])
            self.send_response(200)
            self.send_header("Content-type", self.extensions[typ])
            self.send_header("Content-Disposition", "attachment")
            self.end_headers()
            if typ == "xls":
                with tempfile.NamedTemporaryFile(suffix="xlsx") as tmp:
                    print(tmp.name)
                    r.write_excel(tmp.name)
                    self.wfile.write(bytes(open(tmp.name).read(), 'utf-8'))
            else:
                r.write_csv(self.wfile)
        elif self.path == '/api/servertree':
            self.set_headers()
            self.send_header("Content-type", self.extensions['json'])
            self.end_headers()
            r = self.query("dk>show tables;")
            s = "["
            i = 0
            for c in r.get_column("name"):
                # server: String, namespace: String, name: String, fullName: String, type: String, query: String,
                # Partial info: String.Or(Undefined), db: String.Or(Undefined), columns: String.Or(Undefined)
                s = s + (',' if i > 0 else '') + '{"server":"pythondb", "name":"' + c + '", "namespace":"", "fullName":"' + c + '", "type":"table", "query":"dk>SELECT * FROM ' + c + ' LIMIT 1000"}'
                i = i + 1
            s = s + "]"
            self.wfile.write(bytes(s, 'utf-8'))
        elif os.path.exists(p):
            try:
                file_to_open = open(p).read()
                self.send_response(200)
                self.set_content_type()
            except:
                file_to_open = "File not found"
                self.send_response(404)
            self.end_headers()
            self.wfile.write(bytes(file_to_open, 'utf-8'))
        else:
            self.end_headers()
            self.wfile.write(b'Hello, World!')

