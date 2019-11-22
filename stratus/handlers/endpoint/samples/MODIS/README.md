#### Stratus with MODIS

#### Installation

>> git clone https://github.com/big-data-lab-umbc/MODIS_Aggregation.git
>> cd MODIS_Aggregation
>> python setup.py install

#### Execution
* rest_test
MODIS with REST server and client
>> Run server.py
>> Run client.py

* zmq_test
MODIS with ZeroMQ server and client
>> Run server.py
>> Run client.py

* rest_zmq_test
MODIS with REST and ZeroMQ
>> Run zmq_server.py
>> Run rest_zmq_server.py
>> Run client.py