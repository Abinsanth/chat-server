/*
 * client.c - terminal client for the chat server
 *
 * Two jobs must happen at the same time: waiting for the user to type, and
 * waiting for messages from the server. Both calls block (fgets and recv),
 * so each gets its own thread.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define DEFAULT_IP   "127.0.0.1"
#define DEFAULT_PORT 8080
#define BUF_SIZE     1024

/*
 * The connected socket. It is set once in main() before any thread starts
 * and never changes afterwards, so both threads can read it without a lock.
 */
static int server_fd = -1;

/* Same partial-send problem as on the server: loop until all bytes are out. */
static int send_all(int fd, const char *buf, size_t len)
{
    while (len > 0) {
        ssize_t sent = send(fd, buf, len, 0);
        if (sent < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        buf += sent;
        len -= (size_t)sent;
    }
    return 0;
}

/*
 * Fill in the server address from "client [ip] [port]".
 * Returns 0 on success, -1 on a bad IP or port.
 */
static int parse_args(int argc, char *argv[], struct sockaddr_in *addr)
{
    const char *ip = (argc >= 2) ? argv[1] : DEFAULT_IP;
    long port = DEFAULT_PORT;

    if (argc >= 3) {
        char *end;
        errno = 0;
        port = strtol(argv[2], &end, 10);
        if (errno != 0 || *end != '\0' || port < 1 || port > 65535)
            return -1;
    }

    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_port = htons((uint16_t)port);
    /* inet_pton turns text like "127.0.0.1" into the 4-byte binary form,
     * already in network byte order. Returns 1 on success. */
    if (inet_pton(AF_INET, ip, &addr->sin_addr) != 1)
        return -1;
    return 0;
}

/* Create a TCP socket and connect it. Returns the fd or -1. */
static int connect_to_server(const struct sockaddr_in *addr)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    /* connect() performs the TCP three-way handshake (SYN, SYN-ACK, ACK). */
    if (connect(fd, (const struct sockaddr *)addr, sizeof(*addr)) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }
    return fd;
}

/*
 * Thread: print everything the server sends.
 * The client does not need to split messages into lines here, because it
 * only prints bytes to the screen and the terminal doesn't care where one
 * recv() ends and the next begins.
 */
static void *receive_loop(void *arg)
{
    (void)arg;   /* unused: required by the pthread signature */
    char buf[BUF_SIZE];

    for (;;) {
        ssize_t n = recv(server_fd, buf, sizeof(buf), 0);
        if (n == 0) {
            printf("Disconnected from server.\n");
            break;
        }
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("recv");
            break;
        }
        fwrite(buf, 1, (size_t)n, stdout);
        fflush(stdout);   /* show it now, even if it didn't end with '\n' */
    }
    return NULL;
}

/*
 * Thread: send every line the user types.
 * On Ctrl+D (end of input) we shutdown() only our sending direction. That
 * tells the server "no more data from me" (it sees recv() == 0) while we can
 * still receive its goodbye. The server then closes, receive_loop() sees
 * that, and the program ends. One single exit path.
 */
static void *input_loop(void *arg)
{
    (void)arg;
    char line[BUF_SIZE];

    while (fgets(line, sizeof(line), stdin) != NULL) {
        if (send_all(server_fd, line, strlen(line)) < 0) {
            perror("send");
            return NULL;   /* receive_loop will see the broken connection too */
        }
    }

    if (shutdown(server_fd, SHUT_WR) < 0)
        perror("shutdown");
    return NULL;
}

int main(int argc, char *argv[])
{
    struct sockaddr_in addr;
    if (parse_args(argc, argv, &addr) < 0) {
        fprintf(stderr, "usage: %s [server_ip] [port]   (default %s %d)\n",
                argv[0], DEFAULT_IP, DEFAULT_PORT);
        return EXIT_FAILURE;
    }

    /* If the server vanishes while we send, don't let SIGPIPE kill us
     * silently. send() returns EPIPE instead and we report it. */
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        perror("signal");
        return EXIT_FAILURE;
    }

    server_fd = connect_to_server(&addr);
    if (server_fd < 0)
        return EXIT_FAILURE;

    pthread_t recv_tid, input_tid;
    int err = pthread_create(&recv_tid, NULL, receive_loop, NULL);
    if (err != 0) {
        fprintf(stderr, "pthread_create: %s\n", strerror(err));
        close(server_fd);
        return EXIT_FAILURE;
    }
    err = pthread_create(&input_tid, NULL, input_loop, NULL);
    if (err != 0) {
        fprintf(stderr, "pthread_create: %s\n", strerror(err));
        close(server_fd);
        return EXIT_FAILURE;
    }

    /*
     * The input thread is usually stuck inside fgets(), and there is no clean
     * way to wake it up. So we only wait for the receive thread. When the
     * server closes the connection we return from main, which ends the whole
     * process, input thread included.
     */
    pthread_detach(input_tid);
    pthread_join(recv_tid, NULL);

    close(server_fd);
    return EXIT_SUCCESS;
}
