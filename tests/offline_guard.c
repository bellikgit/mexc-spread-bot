/* Test-only network denial. Never linked into the production bot. */
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>
int connect(int fd,const struct sockaddr* address,socklen_t length) {
    (void)fd;(void)address;(void)length;errno=EPERM;return -1;
}
int getaddrinfo(const char* node,const char* service,const struct addrinfo* hints,struct addrinfo** result) {
    (void)node;(void)service;(void)hints;(void)result;return EAI_FAIL;
}
