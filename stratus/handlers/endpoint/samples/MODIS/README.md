# Stratus with MODIS
# Installation
    >> git clone https://github.com/big-data-lab-umbc/MODIS_Aggregation.git
    >> cd MODIS_Aggregation
    >> python setup.py install
# Execution
- ## rest_test
    MODIS with REST server and client
    1. Run server.py
    2. Run client.py
- ## zmq_test
    MODIS with ZeroMQ server and client
    1. Run server.py
    2. Run client.py
- ## rest_zmq_test
    MODIS with REST and ZeroMQ
    1. Run zmq_server.py
    2. Run rest_zmq_server.py
    3. Run client.py