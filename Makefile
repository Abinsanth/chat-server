CC     = gcc
CFLAGS = -Wall -Wextra -pthread

all: server client

server: src/server.c
	$(CC) $(CFLAGS) -o $@ $<

client: src/client.c
	$(CC) $(CFLAGS) -o $@ $<

clean:
	rm -f server client

.PHONY: all clean
