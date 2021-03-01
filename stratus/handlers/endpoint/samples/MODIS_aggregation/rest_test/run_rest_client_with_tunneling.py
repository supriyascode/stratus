import os
import os

# pid=os.fork()
command_port_fwd = "ssh -L 5000:127.0.0.1:5000 supriys1@taki.rs.umbc.edu"

os.system(command_port_fwd)
