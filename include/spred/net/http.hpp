#pragma once

#include "spred/net/tls.hpp"
#include "spred/core/runtime.hpp"

namespace spred {

std::string dechunk(const std::string& s);
bool httpsGetCurl(const std::string& host, const std::string& path,
                         std::string& out);
bool httpsGet(const std::string& host, const std::string& path, std::string& out);

struct HttpResp {
    int code = 0;
    double retryAfter = 0;   // [fix F41] заголовок Retry-After, c
    std::string body;
    bool transportOk = false;
    double rttMs = 0;
};

class HttpClient {
public:
    explicit HttpClient(const std::string& hostWithOptPort, bool noVerify);
    ~HttpClient();

    void disconnect();

    HttpResp request(const std::string& method, const std::string& path,
                     const std::string& body,
                     const std::vector<std::pair<std::string, std::string>>& hdrs,
                     int timeoutMs = 4000);

private:
    bool ensureConnected();

    bool writeAll(const std::string& s);

    bool doRequest(const std::string& method, const std::string& path,
                   const std::string& body,
                   const std::vector<std::pair<std::string, std::string>>& hdrs,
                   HttpResp& r, int timeoutMs);

    std::string host_;
    int port_ = 443;
    bool noVerify_ = false;
    SSL_CTX* ctx_ = nullptr;
    BIO* bio_ = nullptr;
    int fd_ = -1;   // [fix F11]
    bool wrote_ = false;   // [fix F19]
};

} // namespace spred
