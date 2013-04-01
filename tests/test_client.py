

import rpc.client
c = rpc.client.RPCClient('127.0.0.1', 9999)
future = c.foo(1, 2)
print future.wait()
