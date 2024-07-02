# KV Store

The KV Store is a key-value data structure for storing long-term and short-term robot data.
Powered by **DTPS**, the kvstore exposes the stored data as data queues over HTTP. Values can be 
retrieved (GET), set (POST), patched (PATCH), dropped (DELETE), and monitored for changes (WS).
