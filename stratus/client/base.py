from pyswagger import App, Security
from pyswagger.contrib.client.requests import Client
from pyswagger.spec.v2_0.objects import Operation
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView
from flask import Response

class ServerProxy:
    def __init__(self, openapi_spec: str ):
        self.app = App._create_( openapi_spec )
        self.client = Client()

    def request(self, method: str, **kwargs ) -> Dict:
        op: Operation = self.app.op[ method ]
        response = self.client.request( op(**kwargs) )
        return response.data