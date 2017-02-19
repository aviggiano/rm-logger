rm-logger
=========
- protocol
  - LOGGER.INIT (every client) subscribes to a HEARTBEAT channel
  - HEARTBEAT channel pubs a PING and client sends a PONG. Whenever PONG is heard, the server updates the nodes list
  - LOGGER.PUB pubs a message 
  - LOGGER.SUB listens to a message using a regex pattern
