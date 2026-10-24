#include "spred/net/tls.hpp"

namespace spred {

bool g_tlsNoVerify = false;

void tlsConfigCtx(SSL_CTX* ctx) {
    if (!ctx) return;
    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
    if (g_tlsNoVerify) { SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr); return; }
    SSL_CTX_set_default_verify_paths(ctx);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
}

// имя хоста (или IP-литерал) должно совпасть с сертификатом
void tlsSetPeer(SSL* ssl, const std::string& host) {
    unsigned char buf[sizeof(struct in6_addr)];
    if (inet_pton(AF_INET, host.c_str(), buf) == 1 ||
        inet_pton(AF_INET6, host.c_str(), buf) == 1)
        X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), host.c_str());
    else
        SSL_set1_host(ssl, host.c_str());
}

// [fix F16] TCP connect с таймаутом (неблокирующий connect + poll).
// Возвращает блокирующий fd или -1.
int tcpConnectTimeout(const std::string& host, int port, int timeoutMs) {
    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    std::string portStr = std::to_string(port);
    if (getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0 || !res)
        return -1;
    int out = -1;
    for (struct addrinfo* ai = res; ai && out < 0; ai = ai->ai_next) {
        int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;
        int fl = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, fl | O_NONBLOCK);
        int rc = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (rc != 0 && errno == EINPROGRESS) {
            struct pollfd pfd{fd, POLLOUT, 0};
            int pr = ::poll(&pfd, 1, timeoutMs);
            int err = 0;
            socklen_t el = sizeof err;
            if (pr == 1 && getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &el) == 0 && err == 0)
                rc = 0;
        }
        if (rc == 0) {
            fcntl(fd, F_SETFL, fl & ~O_NONBLOCK);
            out = fd;
        } else {
            ::close(fd);
        }
    }
    freeaddrinfo(res);
    return out;
}

} // namespace spred
