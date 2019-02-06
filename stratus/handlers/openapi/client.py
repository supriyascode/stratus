from pyswagger import App
from pyswagger.contrib.client.requests import Client
from pyswagger.spec.v2_0.objects import Operation
from typing import Dict
from stratus.handlers.client import StratusClient

class OpenApiClient(StratusClient):

    def __init__( self, **kwargs ):
        super(OpenApiClient, self).__init__( "openapi", **kwargs )

    def request(self, task: str, **kwargs) -> Dict:
        op: Operation = self.app.op[ task ]
        response = self.client.request( op(**kwargs) )
        return response.data

    def init(self):
        self.server = self["server"]
        self.port = self["port"]
        self.api = self["api"]
        openapi_spec = 'http://{}:{}/{}/swagger.json'.format( self.server, str(self.port), self.api )
        self.app = App._create_( openapi_spec )
        self.client = Client()
