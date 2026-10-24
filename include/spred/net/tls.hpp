#pragma once

#include "spred/platform.hpp"

namespace spred {

extern bool g_tlsNoVerify;
void tlsConfigCtx(SSL_CTX* ctx);
void tlsSetPeer(SSL* ssl, const std::string& host);
int tcpConnectTimeout(const std::string& host, int port, int timeoutMs);

} // namespace spred
