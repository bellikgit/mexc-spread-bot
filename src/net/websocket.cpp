#include "spred/net/websocket.hpp"

namespace spred {

namespace ws {
Connection::Connection(const std::string& host, int port, const std::string& path)
        : host_(host), port_(port), path_(path) {
    SSL_load_error_strings();
    OpenSSL_add_ssl_algorithms();
    ctx_ = SSL_CTX_new(TLS_client_method());
    tlsConfigCtx(ctx_);   // [fix F2] VERIFY_PEER + min TLS1.2
    ssl_ = nullptr;
}

Connection::~Connection() {
    close();
    if (ctx_) SSL_CTX_free(ctx_);
}

bool Connection::connect() {
    close();
    early_.clear();
    int fd = tcpConnectTimeout(host_, port_, 5000);
    if (fd < 0) {
        std::fprintf(stderr, "[ws] TCP connect %s:%d не удался/таймаут\n",
                     host_.c_str(), port_);
        return false;
    }
    struct timeval tv{10, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    SSL* ssl = SSL_new(ctx_);
    if (!ssl) { ::close(fd); return false; }
    SSL_set_fd(ssl, fd);
    SSL_set_tlsext_host_name(ssl, host_.c_str());  // CloudFront needs SNI
    if (!g_tlsNoVerify) tlsSetPeer(ssl, host_);    // [fix F2]
    SSL_set_mode(ssl, SSL_MODE_AUTO_RETRY);
    if (SSL_connect(ssl) != 1) {
        ERR_print_errors_fp(stderr);
        SSL_free(ssl);
        ::close(fd);
        return false;
    }
    if (!g_tlsNoVerify && SSL_get_verify_result(ssl) != X509_V_OK) {   // [fix F2]
        std::fprintf(stderr, "[ws] TLS: сертификат %s не прошёл проверку (код %ld)\n",
                     host_.c_str(), SSL_get_verify_result(ssl));
        SSL_free(ssl);
        ::close(fd);
        return false;
    }
    BIO* bio = BIO_new(BIO_f_ssl());
    if (!bio) { SSL_free(ssl); ::close(fd); return false; }
    BIO_set_ssl(bio, ssl, BIO_CLOSE);
    ssl_ = ssl;
    bio_ = bio;
    fd_ = fd;
    if (!handshakeWs()) { close(); return false; }   // [fix F6] не течь BIO
    return true;
}

bool Connection::connected() const { return ssl_ != nullptr; }

void Connection::close() {
    if (bio_) { BIO_free_all(bio_); bio_ = nullptr; ssl_ = nullptr; }
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }   // [fix F16] сокет наш
}

bool Connection::sendText(const std::string& text) { return sendFrame(0x1, text); }

bool Connection::sendPong(const std::string& payload) { return sendFrame(0xA, payload); }

int Connection::waitReadable(int timeoutMs) {
    if (!ssl_) return -1;
    if (SSL_pending(ssl_) > 0) return 1;
    if (!early_.empty()) return 1;   // байты после handshake уже в буфере
    int fd = fd_;
    if (fd < 0) return -1;
    fd_set rs;
    FD_ZERO(&rs);
    FD_SET(fd, &rs);
    struct timeval tv;
    tv.tv_sec = timeoutMs / 1000;
    tv.tv_usec = (timeoutMs % 1000) * 1000;
    int r = ::select(fd + 1, &rs, nullptr, nullptr, &tv);
    return (r > 0) ? 1 : (r == 0 ? 0 : -1);
}

bool Connection::readFrame(Frame& f) {
    unsigned char h[2];
    if (!readExact(h, 2)) return false;
    bool fin = h[0] & 0x80;
    int opcode = h[0] & 0x0F;
    bool masked = h[1] & 0x80;
    uint64_t len = h[1] & 0x7F;
    if (len == 126) {
        unsigned char e[2];
        if (!readExact(e, 2)) return false;
        len = ((uint64_t)e[0] << 8) | e[1];
    } else if (len == 127) {
        unsigned char e[8];
        if (!readExact(e, 8)) return false;
        len = 0;
        for (int i = 0; i < 8; ++i) len = (len << 8) | e[i];
    }
    if (len > (64ull << 20)) return false;   // [fix F6] hostile length -> bad_alloc
    unsigned char mask[4] = {0, 0, 0, 0};
    if (masked && !readExact(mask, 4)) return false;
    std::string payload(len, '\0');
    if (len && !readExact(reinterpret_cast<unsigned char*>(&payload[0]), len))
        return false;
    if (masked)
        for (uint64_t i = 0; i < len; ++i) payload[i] ^= mask[i % 4];
    f.opcode = opcode;
    f.fin = fin;
    f.payload = std::move(payload);
    return true;
}

bool Connection::sendFrame(int opcode, const std::string& payload) {
    std::string out;
    out.push_back(char(0x80 | opcode));
    size_t len = payload.size();
    if (len < 126) {
        out.push_back(char(0x80 | len));
    } else if (len < 65536) {
        out.push_back(char(0x80 | 126));
        out.push_back(char((len >> 8) & 0xFF));
        out.push_back(char(len & 0xFF));
    } else {
        out.push_back(char(0x80 | 127));
        for (int i = 7; i >= 0; --i) out.push_back(char((len >> (8 * i)) & 0xFF));
    }
    unsigned char mask[4] = {(unsigned char)(rand() & 0xFF), (unsigned char)(rand() & 0xFF),
                             (unsigned char)(rand() & 0xFF), (unsigned char)(rand() & 0xFF)};
    for (int i = 0; i < 4; ++i) out.push_back(char(mask[i]));
    for (size_t i = 0; i < len; ++i)
        out.push_back(char(payload[i] ^ mask[i % 4]));
    return writeAll(out);
}

bool Connection::handshakeWs() {
    unsigned char randKey[16];
    for (auto& b : randKey) b = rand() & 0xFF;
    std::string key = b64(randKey, 16);
    std::string req =
        "GET " + path_ + " HTTP/1.1\r\n"
        "Host: " + host_ + "\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: " + key + "\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n";
    if (!writeAll(req)) return false;
    std::string response;
    while (response.find("\r\n\r\n") == std::string::npos) {
        char buf[2048];
        int n = BIO_read(bio_, buf, sizeof buf);
        if (n <= 0) return false;
        response.append(buf, size_t(n));
        if (response.size() > 65536) return false;
    }
    if (response.compare(0, 12, "HTTP/1.1 101") != 0) {
        std::fprintf(stderr, "[ws] handshake failed:\n%.200s\n", response.c_str());
        return false;
    }
    size_t hdr = response.find("\r\n\r\n") + 4;
    early_.assign(response.begin() + long(hdr), response.end());
    return true;
}

bool Connection::readExact(unsigned char* dst, size_t n) {
    size_t fromEarly = std::min(n, early_.size());
    for (size_t i = 0; i < fromEarly; ++i) dst[i] = (unsigned char)early_[i];
    early_.erase(0, fromEarly);
    size_t remaining = n - fromEarly;
    if (!remaining) return true;
    size_t got = 0;
    while (got < remaining) {
        int r = BIO_read(bio_, dst + got, int(remaining - got));
        if (r <= 0) return false;
        got += size_t(r);
    }
    return true;
}

bool Connection::writeAll(const std::string& s) {
    size_t sent = 0;
    while (sent < s.size()) {
        int r = BIO_write(bio_, s.data() + sent, int(s.size() - sent));
        if (r <= 0) return false;
        sent += size_t(r);
    }
    return true;
}

} // namespace ws

} // namespace spred
