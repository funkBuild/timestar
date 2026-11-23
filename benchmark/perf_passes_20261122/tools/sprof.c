// Minimal LD_PRELOAD sampling profiler (perf is unavailable: perf_event_paranoid=4).
// SIGPROF via ITIMER_PROF; frames recorded with backtrace() into a MAP_SHARED file.
// Env: SPROF_OUT=<file>  SPROF_HZ=<n, default 997>
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <execinfo.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <unistd.h>

#define MAX_DEPTH 48
#define REC_WORDS (MAX_DEPTH + 2)
#define MAX_RECS (1u << 19)

static uint64_t* g_buf;
static _Atomic uint64_t* g_count;
static __thread int in_handler;

static void handler(int sig, siginfo_t* si, void* uc) {
    (void)sig; (void)si; (void)uc;
    if (!g_buf || in_handler) return;
    in_handler = 1;
    int saved = errno;
    void* frames[MAX_DEPTH];
    int n = backtrace(frames, MAX_DEPTH);
    uint64_t idx = atomic_fetch_add(g_count, 1);
    if (idx < MAX_RECS) {
        uint64_t* rec = g_buf + 1 + idx * REC_WORDS;
        rec[0] = (uint64_t)syscall(SYS_gettid);
        rec[1] = (uint64_t)n;
        for (int i = 0; i < n; i++) rec[2 + i] = (uint64_t)frames[i];
    }
    errno = saved;
    in_handler = 0;
}

static int (*real_pthread_sigmask)(int, const sigset_t*, sigset_t*);
static int (*real_sigprocmask)(int, const sigset_t*, sigset_t*);

int pthread_sigmask(int how, const sigset_t* set, sigset_t* old) {
    if (!real_pthread_sigmask) real_pthread_sigmask = dlsym(RTLD_NEXT, "pthread_sigmask");
    if (set && (how == SIG_BLOCK || how == SIG_SETMASK)) {
        sigset_t s = *set;
        sigdelset(&s, SIGPROF);
        return real_pthread_sigmask(how, &s, old);
    }
    return real_pthread_sigmask(how, set, old);
}
int sigprocmask(int how, const sigset_t* set, sigset_t* old) {
    if (!real_sigprocmask) real_sigprocmask = dlsym(RTLD_NEXT, "sigprocmask");
    if (set && (how == SIG_BLOCK || how == SIG_SETMASK)) {
        sigset_t s = *set;
        sigdelset(&s, SIGPROF);
        return real_sigprocmask(how, &s, old);
    }
    return real_sigprocmask(how, set, old);
}

__attribute__((constructor)) static void sprof_init(void) {
    const char* out = getenv("SPROF_OUT");
    if (!out) return;
    int hz = getenv("SPROF_HZ") ? atoi(getenv("SPROF_HZ")) : 997;
    size_t bytes = (1 + (size_t)MAX_RECS * REC_WORDS) * sizeof(uint64_t);
    int fd = open(out, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0 || ftruncate(fd, (off_t)bytes) != 0) return;
    void* p = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (p == MAP_FAILED) return;
    g_count = (_Atomic uint64_t*)p;
    // Save maps for symbolization.
    char mp[4096];
    snprintf(mp, sizeof mp, "%s.maps", out);
    FILE* in = fopen("/proc/self/maps", "r");
    FILE* o = fopen(mp, "w");
    if (in && o) { char line[4096]; while (fgets(line, sizeof line, in)) fputs(line, o); }
    if (in) fclose(in);
    if (o) fclose(o);
    void* warm[4];
    backtrace(warm, 4);  // force libgcc load outside signal context
    g_buf = (uint64_t*)p;
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_sigaction = handler;
    sa.sa_flags = SA_SIGINFO | SA_RESTART;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGPROF, &sa, NULL);
    struct itimerval it;
    it.it_interval.tv_sec = 0;
    it.it_interval.tv_usec = 1000000 / hz;
    it.it_value = it.it_interval;
    setitimer(ITIMER_PROF, &it, NULL);
}
