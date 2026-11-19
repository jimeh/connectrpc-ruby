// LD_PRELOAD getaddrinfo() shim for the connectrpc-hyper specs. Nothing
// leaves the machine:
// - names ending in ".stall" announce "dns-enter <name>" on the FIFO named by
//   STALL_EVENTS, block until a byte arrives on the FIFO named by
//   STALL_RELEASE, announce "dns-leave", then fail with EAI_NONAME;
// - names ending in ".delay" do the same, then resolve to 127.0.0.1;
// - names ending in ".test" resolve to 127.0.0.1 at once;
// - names ending in ".pair" resolve to 127.0.0.2, then 127.0.0.1 (two
//   addresses of one family, in that order).
// Everything else passes through.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef int (*gai_fn)(const char *, const char *, const struct addrinfo *, struct addrinfo **);

static int ends_with(const char *node, const char *suffix) {
  size_t n = node ? strlen(node) : 0;
  size_t s = strlen(suffix);
  return n > s && strcmp(node + n - s, suffix) == 0;
}

static void announce(const char *line, size_t length) {
  const char *events = getenv("STALL_EVENTS");
  if (!events) return;
  int fd = open(events, O_WRONLY);
  if (fd >= 0) {
    ssize_t ignored = write(fd, line, length);
    (void)ignored;
    close(fd);
  }
}

static void park(const char *node) {
  char line[256];
  int length = snprintf(line, sizeof line, "dns-enter %s\n", node);
  announce(line, (size_t)length);
  const char *release = getenv("STALL_RELEASE");
  if (release) {
    int fd = open(release, O_RDONLY);
    char byte;
    if (fd >= 0) {
      ssize_t ignored = read(fd, &byte, 1);
      (void)ignored;
      close(fd);
    }
  }
  announce("dns-leave\n", 10);
}

int getaddrinfo(const char *node, const char *service, const struct addrinfo *hints, struct addrinfo **res) {
  gai_fn real = (gai_fn)dlsym(RTLD_NEXT, "getaddrinfo");
  if (ends_with(node, ".stall")) {
    park(node);
    return EAI_NONAME;
  }
  if (ends_with(node, ".delay")) {
    park(node);
    return real("127.0.0.1", service, hints, res);
  }
  if (ends_with(node, ".test")) {
    return real("127.0.0.1", service, hints, res);
  }
  if (ends_with(node, ".pair")) {
    struct addrinfo *first = NULL;
    int status = real("127.0.0.2", service, hints, &first);
    if (status != 0) return status;
    struct addrinfo *second = NULL;
    status = real("127.0.0.1", service, hints, &second);
    if (status != 0) {
      freeaddrinfo(first);
      return status;
    }
    // glibc's freeaddrinfo frees node by node, so one chained list is safe.
    struct addrinfo *last = first;
    while (last->ai_next) last = last->ai_next;
    last->ai_next = second;
    *res = first;
    return 0;
  }
  return real(node, service, hints, res);
}
