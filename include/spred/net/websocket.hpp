#pragma once

#include "spred/net/tls.hpp"
#include "spred/core/encoding.hpp"
#include "spred/core/runtime.hpp"

namespace spred {

namespace ws {

struct Frame {
    int opcode = -1;
    bool fin = true;
    std::string payload;
};

class Connection {
public:
    Connection(const std::string& host, int port, const std::string& path);
    ~Connection();

    // [fix F16] свой TCP-connect с таймаутом (BIO_do_connect блокировал без
    // ограничения) + SO_RCVTIMEO/SO_SNDTIMEO: недочитанный кадр или молчащий
    // посреди кадра сервер больше не вешают цикл навсегда.
    bool connect();

    bool connected() const;
    void close();

    bool sendText(const std::string& text);
    bool sendPong(const std::string& payload);

    int waitReadable(int timeoutMs);

    bool readFrame(Frame& f);

    bool sendFrame(int opcode, const std::string& payload);

private:
    bool handshakeWs();

    bool readExact(unsigned char* dst, size_t n);

    bool writeAll(const std::string& s);

    std::string host_;
    int port_;
    std::string path_;
    SSL_CTX* ctx_ = nullptr;
    SSL* ssl_ = nullptr;
    BIO* bio_ = nullptr;
    int fd_ = -1;
    std::string early_;
};

} // namespace ws

} // namespace spred
