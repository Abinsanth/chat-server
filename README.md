# Multithreaded TCP Chat Server in C

A small chat server and terminal client written in C using POSIX sockets and pthreads. Runs on Linux and macOS with no external libraries.

## Features

- One thread per client, up to 10 clients at a time
- Unique usernames, with join/leave notifications
- Messages go to everyone else as `[HH:MM] name: message`
- Line-based message framing over TCP
- Survives clients disconnecting abruptly

## Build and run

```sh
make                # builds ./server and ./client
./server [port]     # default port 8080
./client [ip] [port]   # default 127.0.0.1 8080
```

To leave the chat, press `Ctrl+D` or `Ctrl+C`. Run `make clean` to remove the binaries.

## Example

```
$ ./client
Enter your name: alice
Welcome, alice! 2 user(s) online.
[14:05] bob: hey alice
hi bob
[14:06] *** bob left the chat ***
```

## How it works

- **Thread per client:** the main thread only calls `accept()`. Each new connection gets its own thread, which asks for a name and then relays messages. The client also uses two threads: one reads the keyboard, the other prints what the server sends.
- **Mutex:** every thread reads and writes the shared table of clients. A `pthread_mutex_t` makes sure two threads can't change it at the same time. For example, it stops two people from claiming the same name, or a message being sent to a socket that is being closed.
- **Message framing:** TCP is a byte stream, so one `recv()` can return half a message or several messages at once. Each client has a buffer that collects bytes until a `\n` arrives, then hands out one full line at a time.

**Known limitation:** messages are sent while the mutex is held, so a client that stops reading could stall the server. A per-client send queue or a send timeout would fix this.
