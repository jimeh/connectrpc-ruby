// LD_PRELOAD pthread_create() shim for the connectrpc-hyper specs.
// FAIL_THREADS="allow,fail" lets the first `allow` threads whose start
// routine lives in the connectrpc_hyper extension start, refuses the next
// `fail` with EAGAIN, then lets later ones start. Other threads are
// untouched. Each extension thread's decision ("start" or "refuse") is
// appended as a line to the file named by FAIL_THREADS_LOG, if set.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef int (*create_fn)(pthread_t *, const pthread_attr_t *, void *(*)(void *), void *);

static create_fn real_create;
static pthread_once_t resolved = PTHREAD_ONCE_INIT;
static long allow = -1, fail = 0;
static atomic_long seen;

static void resolve(void) { real_create = (create_fn)dlsym(RTLD_NEXT, "pthread_create"); }

__attribute__((constructor)) static void configure(void) {
  const char *spec = getenv("FAIL_THREADS");
  if (!spec) return;
  char *end;
  allow = strtol(spec, &end, 10);
  fail = *end == ',' ? strtol(end + 1, NULL, 10) : 1;
}

static void record(const char *line) {
  const char *log = getenv("FAIL_THREADS_LOG");
  if (!log) return;
  int fd = open(log, O_WRONLY | O_CREAT | O_APPEND, 0600);
  if (fd >= 0) {
    ssize_t ignored = write(fd, line, strlen(line));
    (void)ignored;
    close(fd);
  }
}

static int extension_thread(void *(*start)(void *)) {
  Dl_info info;
  return dladdr((void *)start, &info) && info.dli_fname && strstr(info.dli_fname, "connectrpc_hyper");
}

int pthread_create(pthread_t *thread, const pthread_attr_t *attr, void *(*start)(void *), void *arg) {
  pthread_once(&resolved, resolve);
  if (allow >= 0 && extension_thread(start)) {
    long n = atomic_fetch_add(&seen, 1);
    if (n >= allow && n < allow + fail) {
      record("refuse\n");
      return EAGAIN;
    }
    record("start\n");
  }
  return real_create(thread, attr, start, arg);
}
