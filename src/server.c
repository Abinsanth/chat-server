/*
 * server.c - multithreaded TCP chat server
 *
 * Design: the main thread only accepts connections. Every accepted client
 * gets its own thread that asks for a username and then forwards what the
 * client says to everyone else. All threads share one table of client
 * slots, so that table is protected by a mutex.
 */

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_PORT 8080
#define MAX_CLIENTS 10
#define BACKLOG 16                  /* connections the kernel may queue before we accept() them */
#define BUF_SIZE 512                /* per-client receive buffer */
#define MAX_LINE_LEN (BUF_SIZE - 1) /* longest line: it must fit in the buffer with its '\n' */
#define MAX_NAME_LEN 20
#define MSG_SIZE (BUF_SIZE + 128) /* room for a message plus name/decoration */
#define TIME_LEN 6                /* "HH:MM" + '\0' */

/*
 * One client slot.
 *   fd == -1                      -> slot is free
 *   fd != -1, username[0] == '\0' -> pending: connected, still choosing a name
 *   fd != -1, username set        -> member: in the chat
 */
typedef struct
{
    int fd;
    char username[MAX_NAME_LEN + 1]; /* +1 for the terminating '\0' */

    /* Framing state, used only by this client's own thread (see read_message). */
    char inbuf[BUF_SIZE]; /* bytes received but not yet returned as a line */
    size_t inlen;         /* how many bytes of inbuf are in use */
    int discarding;       /* 1 while skipping the rest of an overlong line */
} client_t;

/*
 * Fixed-size table instead of a malloc'd list: memory use is known at
 * compile time and there is nothing to leak. Every thread reads it and
 * several threads write it, so every access must hold clients_lock.
 *
 * Exceptions: a client's own thread may READ its own fd/username without the
 * lock, because no other thread ever writes to a slot it doesn't own (the
 * accept loop only touches free slots). And the framing fields (inbuf,
 * inlen, discarding) belong to that thread alone, so no other thread reads
 * or writes them at all.
 */
static client_t clients[MAX_CLIENTS];
static pthread_mutex_t clients_lock = PTHREAD_MUTEX_INITIALIZER;

/* ------------------------------------------------------------------ */
/* Low-level helpers                                                   */
/* ------------------------------------------------------------------ */

/*
 * send() may write fewer bytes than asked (e.g. when the kernel's socket
 * buffer is nearly full), so keep calling it until everything is out.
 * Returns 0 on success, -1 if the connection is broken.
 */
static int send_all(int fd, const char *buf, size_t len)
{
    while (len > 0)
    {
        ssize_t sent = send(fd, buf, len, 0);
        if (sent < 0)
        {
            if (errno == EINTR) /* interrupted by a signal: just retry */
                continue;
            return -1;
        }
        buf += sent;
        len -= (size_t)sent;
    }
    return 0;
}

/*
 * printf-style formatting into out. Returns the number of bytes written.
 * If the text doesn't fit, vsnprintf cuts it off; we return the cut length
 * rather than the length it *wanted*, so callers never read past out.
 */
static size_t vformat(char *out, size_t size, const char *fmt, va_list ap)
{
    int len = vsnprintf(out, size, fmt, ap);
    if (len < 0)
        return 0;
    if ((size_t)len >= size)
        return size - 1;
    return (size_t)len;
}

/*
 * Write the server's current local time as "HH:MM" into out.
 *
 * localtime_r, not localtime: localtime() returns a pointer to ONE static
 * struct shared by the whole process, so two client threads calling it at
 * the same moment can overwrite each other's result. The _r ("reentrant")
 * version fills in a struct that we own, on this thread's stack.
 */
static void current_time(char out[TIME_LEN])
{
    time_t now = time(NULL); /* seconds since 1970-01-01 UTC */
    struct tm local;

    if (now == (time_t)-1)
    {
        perror("time");
    }
    else if (localtime_r(&now, &local) == NULL)
    {
        perror("localtime_r");
    }
    else if (strftime(out, TIME_LEN, "%H:%M", &local) > 0)
    {
        return;
    }
    /* A broken clock is no reason to stop chatting: show a placeholder. */
    snprintf(out, TIME_LEN, "--:--");
}

/* ------------------------------------------------------------------ */
/* Client table (every function here takes the lock itself)           */
/* ------------------------------------------------------------------ */

static void init_slots(void)
{
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        clients[i].fd = -1;
        clients[i].username[0] = '\0';
    }
}

/* Caller must hold clients_lock. */
static int is_member(const client_t *c)
{
    return c->fd != -1 && c->username[0] != '\0';
}

/*
 * Reserve a free slot for a freshly accepted socket. The slot starts out
 * pending (no name). Returns the slot, or NULL if the server is full.
 */
static client_t *claim_slot(int fd)
{
    client_t *slot = NULL;

    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].fd == -1)
        {
            clients[i].fd = fd;
            clients[i].username[0] = '\0';
            clients[i].inlen = 0; /* don't inherit the last owner's bytes */
            clients[i].discarding = 0;
            slot = &clients[i];
            break; /* break, not return: we must still reach the unlock */
        }
    }
    pthread_mutex_unlock(&clients_lock);

    return slot;
}

/*
 * Free a slot and close its socket. Both happen under the lock so that no
 * other thread can be in the middle of send()ing to this fd. Without that,
 * the OS could hand the same fd number to a new client, and an old
 * broadcast would leak into the wrong connection.
 */
static void release_slot(client_t *slot)
{
    pthread_mutex_lock(&clients_lock);
    close(slot->fd);
    slot->fd = -1;
    slot->username[0] = '\0';
    pthread_mutex_unlock(&clients_lock);
}

/*
 * Give slot the username `name` if no member already has it.
 * Returns 0 on success, -1 if the name is taken.
 *
 * The "is it free?" check and the "take it" write must happen inside ONE
 * critical section. If we checked, unlocked, then wrote, two clients typing
 * "bob" at the same moment could both pass the check: a classic
 * check-then-act race condition.
 */
static int claim_username(client_t *slot, const char *name)
{
    int result = 0;

    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        /* strcasecmp: "Alice" and "alice" count as the same name */
        if (is_member(&clients[i]) && strcasecmp(clients[i].username, name) == 0)
        {
            result = -1;
            break;
        }
    }
    if (result == 0)
        snprintf(slot->username, sizeof(slot->username), "%s", name);
    pthread_mutex_unlock(&clients_lock);

    return result;
}

static int count_members(void)
{
    int count = 0;

    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (is_member(&clients[i]))
            count++;
    pthread_mutex_unlock(&clients_lock);

    return count;
}

/*
 * Send a printf-style message to one client.
 *
 * Why lock just to talk to one client? Another thread may be broadcasting
 * to this same socket. If both send_all() loops needed several send() calls,
 * their bytes could interleave and garble both messages. Rule: every send
 * to a client in the table happens under clients_lock.
 */
static void send_to(client_t *slot, const char *fmt, ...)
{
    char msg[MSG_SIZE];
    va_list ap;

    va_start(ap, fmt);
    size_t len = vformat(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    pthread_mutex_lock(&clients_lock);
    send_all(slot->fd, msg, len);
    pthread_mutex_unlock(&clients_lock);
}

/*
 * Send a printf-style message to every member except the sender.
 * Pending clients (still choosing a name) are skipped: they haven't joined.
 *
 * The text is formatted BEFORE taking the lock, to keep the critical
 * section as short as possible. We then hold the lock for the whole loop so
 * the table cannot change under us.
 * Known limitation: if one client stops reading, send() can block here and
 * stall every other thread waiting for the lock (see README).
 */
static void broadcast(const client_t *sender, const char *fmt, ...)
{
    char msg[MSG_SIZE];
    va_list ap;

    va_start(ap, fmt);
    size_t len = vformat(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    pthread_mutex_lock(&clients_lock);
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        client_t *c = &clients[i];
        if (c == sender || !is_member(c))
            continue;
        /* A failed send means that client is gone; its own thread will
         * notice on its next recv() and clean up, so we just move on. */
        send_all(c->fd, msg, len);
    }
    pthread_mutex_unlock(&clients_lock);
}

/* ------------------------------------------------------------------ */
/* Message framing                                                     */
/* ------------------------------------------------------------------ */

/*
 * TCP is a byte STREAM, not a sequence of messages. The sender's
 * "hi\n" "how are you\n" may arrive as one recv() ("hi\nhow are you\n"),
 * or split anywhere ("hi\nhow a" + "re you\n"). So we keep the received
 * bytes in slot->inbuf and only hand out text once a full line ('\n') is
 * there. Whatever follows that newline stays buffered for the next call.
 */

/* Drop the first n bytes of the buffer, sliding the rest to the front.
 * memmove, not memcpy: source and destination overlap. */
static void consume(client_t *slot, size_t n)
{
    memmove(slot->inbuf, slot->inbuf + n, slot->inlen - n);
    slot->inlen -= n;
}

/*
 * Copy the first line_len bytes of the buffer into out as a C string,
 * drop that line and its '\n' from the buffer, and return the length.
 */
static int take_line(client_t *slot, size_t line_len, char *out, size_t size)
{
    size_t copy = line_len < size ? line_len : size - 1; /* never overflow out */

    memcpy(out, slot->inbuf, copy);
    out[copy] = '\0';
    if (copy > 0 && out[copy - 1] == '\r') /* tools like nc/telnet send "\r\n" */
        out[copy - 1] = '\0';

    consume(slot, line_len + 1); /* +1 drops the '\n' too */
    return (int)strlen(out);
}

/* Append whatever the socket has to the buffer. Returns -1 on disconnect. */
static int recv_more(client_t *slot)
{
    for (;;)
    {
        ssize_t n = recv(slot->fd, slot->inbuf + slot->inlen,
                         sizeof(slot->inbuf) - slot->inlen, 0);
        if (n > 0)
        {
            slot->inlen += (size_t)n;
            return 0;
        }
        if (n == 0) /* peer closed the connection */
            return -1;
        if (errno == EINTR) /* interrupted by a signal: retry */
            continue;
        perror("recv"); /* e.g. ECONNRESET when the client crashes */
        return -1;
    }
}

/*
 * Read one complete line from a client into out (without the newline).
 * Returns its length (0 for an empty line), or -1 if the client disconnected.
 *
 * A line longer than MAX_LINE_LEN can never fit in the buffer, so once the
 * buffer is full with no '\n' in sight we tell the sender, then throw bytes
 * away until the next '\n'. The connection stays open.
 */
static int read_message(client_t *slot, char *out, size_t size)
{
    for (;;)
    {
        /* 1. A full line already buffered? (left over from an earlier recv) */
        char *newline = memchr(slot->inbuf, '\n', slot->inlen);
        if (newline != NULL)
        {
            size_t line_len = (size_t)(newline - slot->inbuf);
            if (!slot->discarding)
                return take_line(slot, line_len, out, size);
            /* This newline ends an overlong line: drop its tail, resume. */
            consume(slot, line_len + 1);
            slot->discarding = 0;
            continue;
        }

        /* 2. No full line. Are we skipping, or did a line just get too long? */
        if (slot->discarding)
        {
            slot->inlen = 0;
        }
        else if (slot->inlen == sizeof(slot->inbuf))
        {
            slot->inlen = 0;
            slot->discarding = 1;
            send_to(slot, "Message too long (max %d characters), discarded.\n",
                    MAX_LINE_LEN);
        }

        /* 3. Wait for more bytes, then look again. */
        if (recv_more(slot) < 0)
            return -1;
    }
}

/* ------------------------------------------------------------------ */
/* Usernames                                                           */
/* ------------------------------------------------------------------ */

/*
 * Check the format of a proposed name (not whether it's taken).
 * Returns NULL if it's fine, otherwise a message explaining what's wrong.
 */
static const char *username_problem(const char *name)
{
    size_t len = strlen(name);

    if (len == 0)
        return "Name cannot be empty.";
    if (len > MAX_NAME_LEN)
        return "Name is too long (max 20 characters).";
    if (name[0] == '/')
        return "Name cannot start with '/'.";
    for (size_t i = 0; i < len; i++)
    {
        /* isgraph = printable and not a space. Spaces would break
         * "/msg <name> <text>" parsing. The cast avoids undefined behaviour
         * for bytes >= 128 when char is signed. */
        if (!isgraph((unsigned char)name[i]))
            return "Name cannot contain spaces or control characters.";
    }
    return NULL;
}

/*
 * Keep asking until the client picks a valid, unused name.
 * Returns 0 once the name is claimed, -1 if the client disconnects first.
 */
static int ask_for_username(client_t *slot)
{
    char name[BUF_SIZE];

    for (;;)
    {
        send_to(slot, "Enter your name: ");
        if (read_message(slot, name, sizeof(name)) < 0)
            return -1;

        const char *problem = username_problem(name);
        if (problem != NULL)
        {
            send_to(slot, "%s Try again.\n", problem);
            continue;
        }
        if (claim_username(slot, name) < 0)
        {
            send_to(slot, "Name \"%s\" is already taken. Try again.\n", name);
            continue;
        }
        return 0;
    }
}

/* ------------------------------------------------------------------ */
/* Per-client thread                                                   */
/* ------------------------------------------------------------------ */

/* Everything that happens between joining and leaving the chat. */
static void chat_session(client_t *slot)
{
    char msg[BUF_SIZE];
    char now[TIME_LEN];
    int len;

    send_to(slot, "Welcome, %s! %d user(s) online.\n", slot->username, count_members());
    current_time(now);
    broadcast(slot, "[%s] *** %s joined the chat ***\n", now, slot->username);

    while ((len = read_message(slot, msg, sizeof(msg))) >= 0)
    {
        if (len == 0) /* ignore empty lines */
            continue;
        /* Stamp the time when the SERVER receives the message, so every
         * member sees the same time no matter what their own clock says. */
        current_time(now);
        broadcast(slot, "[%s] %s: %s\n", now, slot->username, msg);
    }

    /* Announce while we still hold the name. If we released the slot
     * first, a newcomer could take the name and "bob left" would appear
     * after the new bob joined. */
    current_time(now);
    broadcast(slot, "[%s] *** %s left the chat ***\n", now, slot->username);
}

/*
 * Thread entry point: one of these runs for every connected client.
 * There is exactly one place the slot is released, at the bottom, so no
 * path (name prompt abandoned, normal leave, crash) can leak a slot.
 */
static void *handle_client(void *arg)
{
    client_t *slot = arg;
    int id = (int)(slot - clients); /* slot index, handy for server logs */

    printf("slot %d: connected (fd %d)\n", id, slot->fd);

    if (ask_for_username(slot) == 0)
    {
        printf("slot %d: joined as '%s'\n", id, slot->username);
        chat_session(slot);
        printf("slot %d: '%s' left\n", id, slot->username);
    }
    else
    {
        printf("slot %d: disconnected before choosing a name\n", id);
    }

    release_slot(slot);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Startup and accept loop                                             */
/* ------------------------------------------------------------------ */

/* Returns the port from argv[1], DEFAULT_PORT if absent, or -1 if invalid. */
static int parse_port(int argc, char *argv[])
{
    if (argc < 2)
        return DEFAULT_PORT;

    char *end;
    errno = 0;
    long port = strtol(argv[1], &end, 10);
    /* strtol (unlike atoi) lets us detect junk like "80abc" and overflow */
    if (errno != 0 || *end != '\0' || port < 1 || port > 65535)
        return -1;
    return (int)port;
}

/* Create, configure, bind and listen. Returns the listening fd or -1. */
static int create_listen_socket(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0); /* IPv4 + TCP */
    if (fd < 0)
    {
        perror("socket");
        return -1;
    }

    /*
     * After the server exits, the port stays in TIME_WAIT for a while.
     * Without SO_REUSEADDR, restarting the server immediately fails with
     * "Address already in use".
     */
    int yes = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
    {
        perror("setsockopt(SO_REUSEADDR)");
        close(fd);
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY); /* listen on every interface */
    addr.sin_port = htons((uint16_t)port);    /* network byte order = big-endian */

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, BACKLOG) < 0)
    {
        perror("listen");
        close(fd);
        return -1;
    }
    return fd;
}

/* Tell a connection we have no room for it, then hang up. */
static void reject_full(int fd)
{
    const char *msg = "Server is full, try again later.\n";
    send_all(fd, msg, strlen(msg)); /* fd isn't in the table: no lock needed */
    close(fd);
}

/* Give an accepted connection a slot and its own thread. */
static void start_client(int fd)
{
    client_t *slot = claim_slot(fd);
    if (slot == NULL)
    {
        printf("server full, rejected fd %d\n", fd);
        reject_full(fd);
        return;
    }

    pthread_t tid;
    int err = pthread_create(&tid, NULL, handle_client, slot);
    if (err != 0)
    {
        /* pthread functions return the error code instead of setting errno */
        fprintf(stderr, "pthread_create: %s\n", strerror(err));
        release_slot(slot);
        return;
    }
    /* Nobody will pthread_join() this thread, so let the system reclaim
     * its resources automatically as soon as it finishes. */
    pthread_detach(tid);
}

int main(int argc, char *argv[])
{
    int port = parse_port(argc, argv);
    if (port < 0)
    {
        fprintf(stderr, "usage: %s [port]   (1-65535, default %d)\n", argv[0], DEFAULT_PORT);
        return EXIT_FAILURE;
    }

    /*
     * Writing to a socket whose peer has gone away raises SIGPIPE, whose
     * default action kills the whole process. One client closing its
     * terminal must not take the server down, so ignore the signal; send()
     * then just returns -1 with errno EPIPE, which we handle.
     */
    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR)
    {
        perror("signal");
        return EXIT_FAILURE;
    }

    /* stdout is only line-buffered when it's a terminal. Force it so log
     * lines also appear immediately when redirected to a file. */
    setvbuf(stdout, NULL, _IOLBF, 0);

    init_slots();

    int listen_fd = create_listen_socket(port);
    if (listen_fd < 0)
        return EXIT_FAILURE;

    printf("chat server listening on port %d (max %d clients)\n", port, MAX_CLIENTS);

    for (;;)
    {
        int client_fd = accept(listen_fd, NULL, NULL);
        if (client_fd < 0)
        {
            /* One failed accept (e.g. the client gave up mid-handshake, or
             * we ran out of fds) is no reason to stop serving everyone else. */
            perror("accept");
            continue;
        }
        start_client(client_fd);
    }
}
