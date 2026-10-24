#include "spred/net/http.hpp"

namespace spred {

std::string dechunk(const std::string& s) {
    std::string out;
    size_t p = 0;
    while (p < s.size()) {
        size_t e = s.find("\r\n", p);
        if (e == std::string::npos) break;
        long len = strtol(s.substr(p, e - p).c_str(), nullptr, 16);
        if (len <= 0) break;
        p = e + 2;
        if (p + (size_t)len > s.size()) break;
        out.append(s, p, (size_t)len);
        p += (size_t)len + 2;
    }
    return out;
}

// [v3.0] тестовый режим TLS (мок-стенд с self-signed): MM_TLS_NOVERIFY=1.
// В проде НЕ использовать — отключает проверку сертификата.
// [v2.5.1] REST через curl БЕЗ shell: fork + execlp.
bool httpsGetCurl(const std::string& host, const std::string& path,
                         std::string& out) {
    std::string url = "https://" + host + path;

    int pipefd[2];
    if (pipe(pipefd) != 0) return false;

    pid_t pid = fork();
    if (pid < 0) { close(pipefd[0]); close(pipefd[1]); return false; }

    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        close(pipefd[1]);
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) { dup2(devnull, STDERR_FILENO); close(devnull); }
        alarm(15);
        if (g_tlsNoVerify)
            execlp("curl", "curl",
                   "-4", "-s", "-k", "--max-time", "10", "--compressed",
                   url.c_str(),
                   (char*)nullptr);
        execlp("curl", "curl",
               "-4", "-s", "--max-time", "10", "--compressed",
               url.c_str(),
               (char*)nullptr);
        _exit(127);
    }

    close(pipefd[1]);
    out.clear();
    char buf[65536];
    ssize_t n;
    while ((n = read(pipefd[0], buf, sizeof buf)) > 0) {
        out.append(buf, size_t(n));
        if (out.size() > 16 * 1024 * 1024) break;
    }
    close(pipefd[0]);

    int status = 0;
    waitpid(pid, &status, 0);
    return (WIFEXITED(status) && WEXITSTATUS(status) == 0) && !out.empty();
}

bool httpsGet(const std::string& host, const std::string& path, std::string& out) {
    if (httpsGetCurl(host, path, out)) return true;
    std::fprintf(stderr, "[rest] curl failed for https://%s%s, trying native...\n",
                 host.c_str(), path.c_str());

    // [v3.0 fix] host может быть "127.0.0.1:PORT" (мок-стенд) — выделяем порт,
    // в DNS/SNI отдаём чистый хост
    std::string h = host;
    std::string portStr = "443";
    size_t cpos = h.rfind(':');
    if (cpos != std::string::npos && h.find(':') == cpos) {   // один ':' = host:port
        portStr = h.substr(cpos + 1);
        h = h.substr(0, cpos);
    }

    struct addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    if (getaddrinfo(h.c_str(), portStr.c_str(), &hints, &res) != 0 || !res) return false;

    SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
    if (!ctx) { freeaddrinfo(res); return false; }
    tlsConfigCtx(ctx);   // [fix F2] раньше: SSL_VERIFY_NONE безусловно

    bool ok = false;
    for (struct addrinfo* ai = res; ai && !ok; ai = ai->ai_next) {
        int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;
        struct timeval tv{10, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) != 0) { close(fd); continue; }

        SSL* ssl = SSL_new(ctx);
        if (!ssl) { close(fd); continue; }
        SSL_set_fd(ssl, fd);
        SSL_set_tlsext_host_name(ssl, h.c_str());
        if (!g_tlsNoVerify) tlsSetPeer(ssl, h);   // [fix F2]
        if (SSL_connect(ssl) == 1 &&
            (g_tlsNoVerify || SSL_get_verify_result(ssl) == X509_V_OK)) {
            std::string req = "GET " + path + " HTTP/1.1\r\nHost: " + host +
                              "\r\nUser-Agent: curl/7.81.0\r\nAccept: */*\r\n"
                              "Connection: close\r\n\r\n";
            if (SSL_write(ssl, req.data(), (int)req.size()) > 0) {
                std::string resp;
                char buf[8192];
                for (;;) {
                    int n = SSL_read(ssl, buf, sizeof buf);
                    if (n <= 0) break;
                    resp.append(buf, size_t(n));
                    if (resp.size() > 16 * 1024 * 1024) break;
                }
                size_t hp = resp.find("\r\n\r\n");
                if (hp != std::string::npos) {
                    std::string head = resp.substr(0, hp);
                    std::string body = resp.substr(hp + 4);
                    if (head.find("chunked") != std::string::npos) body = dechunk(body);
                    out = body;
                    ok = !body.empty();
                }
            }
        }
        SSL_free(ssl);
        close(fd);
    }
    freeaddrinfo(res);
    SSL_CTX_free(ctx);
    return ok;
}

HttpClient::HttpClient(const std::string& hostWithOptPort, bool noVerify) {
    host_ = hostWithOptPort;
    port_ = 443;
    size_t c = host_.find(':');
    if (c != std::string::npos) {
        port_ = std::atoi(host_.c_str() + c + 1);
        host_ = host_.substr(0, c);
    }
    noVerify_ = noVerify;
    ctx_ = SSL_CTX_new(TLS_client_method());
    if (ctx_) {
        SSL_CTX_set_min_proto_version(ctx_, TLS1_2_VERSION);
        if (noVerify_) {
            SSL_CTX_set_verify(ctx_, SSL_VERIFY_NONE, nullptr);
        } else {   // [fix F2] раньше VERIFY не включался -> сертификат не проверялся
            SSL_CTX_set_default_verify_paths(ctx_);
            SSL_CTX_set_verify(ctx_, SSL_VERIFY_PEER, nullptr);
        }
    }
}

HttpClient::~HttpClient() {
    disconnect();
    if (ctx_) SSL_CTX_free(ctx_);
}

void HttpClient::disconnect() {
    if (bio_) { BIO_free_all(bio_); bio_ = nullptr; }
    if (fd_ >= 0) { ::close(fd_); fd_ = -1; }   // [fix F11] BIO_NOCLOSE -> fd наш
}

HttpResp HttpClient::request(const std::string& method, const std::string& path,
                     const std::string& body,
                     const std::vector<std::pair<std::string, std::string>>& hdrs,
                     int timeoutMs) {
    HttpResp r;
    double t0 = nowMsSteady();
    for (int attempt = 0; attempt < 2; ++attempt) {
        if (!ensureConnected()) {
            if (attempt == 0) continue;
            r.rttMs = nowMsSteady() - t0;
            return r;
        }
        wrote_ = false;
        if (doRequest(method, path, body, hdrs, r, timeoutMs)) {
            r.transportOk = true;
            r.rttMs = nowMsSteady() - t0;
            return r;
        }
        disconnect();   // обрыв/таймаут — новый коннект на retry
        // [fix F19] неидемпотентный POST, уже ушедший в сокет, НЕ повторяем:
        // биржа могла его исполнить. Решает вызывающий (resolve по cid).
        if (wrote_ && method == "POST") break;
    }
    r.rttMs = nowMsSteady() - t0;
    return r;
}

bool HttpClient::ensureConnected() {
    // [fix F19] keep-alive мог быть закрыт сервером, пока мы молчали:
    // в простое сокет не должен быть читаемым (EOF / close_notify / RST)
    if (bio_ && fd_ >= 0) {
        struct pollfd pfd{fd_, POLLIN, 0};
        if (::poll(&pfd, 1, 0) != 0) disconnect();
    }
    if (bio_) return true;
    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    std::string portStr = std::to_string(port_);
    if (getaddrinfo(host_.c_str(), portStr.c_str(), &hints, &res) != 0 || !res)
        return false;
    bool ok = false;
    for (struct addrinfo* ai = res; ai && !ok; ai = ai->ai_next) {
        int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;
        struct timeval tv{5, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) != 0) { close(fd); continue; }
        SSL* ssl = SSL_new(ctx_);
        if (!ssl) { close(fd); continue; }
        SSL_set_fd(ssl, fd);
        SSL_set_tlsext_host_name(ssl, host_.c_str());
        if (!noVerify_) tlsSetPeer(ssl, host_);   // [fix F2]
        if (SSL_connect(ssl) == 1 &&
            (noVerify_ || SSL_get_verify_result(ssl) == X509_V_OK)) {
            BIO* cbio = BIO_new(BIO_s_socket());
            BIO_set_fd(cbio, fd, BIO_NOCLOSE);
            BIO* sbio = BIO_new(BIO_f_ssl());
            BIO_set_ssl(sbio, ssl, BIO_CLOSE);
            bio_ = BIO_push(sbio, cbio);
            fd_ = fd;   // [fix F11]
            ok = true;
        } else {
            SSL_free(ssl);
            close(fd);
        }
    }
    freeaddrinfo(res);
    return ok;
}

bool HttpClient::writeAll(const std::string& s) {
    size_t sent = 0;
    while (sent < s.size()) {
        int r = BIO_write(bio_, s.data() + sent, int(s.size() - sent));
        if (r <= 0) return false;
        wrote_ = true;   // [fix F19] хоть что-то ушло
        sent += size_t(r);
    }
    return true;
}

bool HttpClient::doRequest(const std::string& method, const std::string& path,
                   const std::string& body,
                   const std::vector<std::pair<std::string, std::string>>& hdrs,
                   HttpResp& r, int timeoutMs) {
    std::string req = method + " " + path + " HTTP/1.1\r\nHost: " + host_ +
                      "\r\nUser-Agent: spred/3.0\r\nAccept: */*\r\n"
                      "Connection: keep-alive\r\n";
    for (const auto& h : hdrs)
        req += h.first + ": " + h.second + "\r\n";
    // [F51] MEXC (700013 Invalid content Type): пустое тело ->
    // application/json, как в официальном SDK; form — только с телом
    if (!body.empty())
        req += "Content-Type: application/x-www-form-urlencoded\r\n"
               "Content-Length: " + std::to_string(body.size()) + "\r\n";
    else if (method == "POST" || method == "PUT" || method == "DELETE")
        req += "Content-Type: application/json\r\n"
               "Content-Length: 0\r\n";
    req += "\r\n";
    req += body;
    if (!writeAll(req)) return false;

    // заголовки
    std::string head;
    char buf[8192];
    double deadline = nowMsSteady() + timeoutMs;
    while (head.find("\r\n\r\n") == std::string::npos) {
        if (nowMsSteady() > deadline) return false;
        int n = BIO_read(bio_, buf, sizeof buf);
        if (n <= 0) return false;
        head.append(buf, size_t(n));
        if (head.size() > 65536) return false;
    }
    size_t hp = head.find("\r\n\r\n");
    std::string rest = head.substr(hp + 4);
    head = head.substr(0, hp);
    {
        // "HTTP/1.1 200 OK"
        size_t sp = head.find(' ');
        if (sp != std::string::npos)
            r.code = std::atoi(head.c_str() + sp + 1);
    }
    bool chunked = false;
    long contentLen = -1;
    {
        std::string low = head;
        for (auto& c : low) c = (char)tolower((unsigned char)c);
        chunked = low.find("transfer-encoding: chunked") != std::string::npos;
        size_t cl = low.find("content-length:");
        if (cl != std::string::npos)
            contentLen = std::atol(low.c_str() + cl + 15);
        size_t ra = low.find("\nretry-after:");   // [fix F41]
        if (ra != std::string::npos) r.retryAfter = std::atof(low.c_str() + ra + 13);
    }
    std::string rawBody = std::move(rest);
    auto readMore = [&](size_t need) -> bool {
        while (rawBody.size() < need) {
            if (nowMsSteady() > deadline) return false;
            int n = BIO_read(bio_, buf, sizeof buf);
            if (n <= 0) return false;
            rawBody.append(buf, size_t(n));
            if (rawBody.size() > 16 * 1024 * 1024) return false;
        }
        return true;
    };
    if (chunked) {
        // [fix F8] инкрементальный разбор чанков. Раньше цикл каждый раз
        // заново разбирал ПЕРВЫЙ чанк и не двигался дальше: ответ из 3+
        // чанков (или нулевой чанк, пришедший отдельной записью) висел до
        // таймаута -> transportOk=false при нормальном ответе сервера.
        std::string out;
        size_t pos = 0;   // начало строки размера текущего чанка в rawBody
        for (;;) {
            size_t e;
            while ((e = rawBody.find("\r\n", pos)) == std::string::npos)
                if (!readMore(rawBody.size() + 1)) return false;
            std::string szs = rawBody.substr(pos, e - pos);
            size_t semi = szs.find(';');              // chunk-extension
            if (semi != std::string::npos) szs.resize(semi);
            if (szs.empty() || !isxdigit((unsigned char)szs[0])) return false;
            unsigned long long len = strtoull(szs.c_str(), nullptr, 16);
            if (len > (16ull << 20)) return false;
            pos = e + 2;
            if (len == 0) {
                // trailer-заголовки (если есть) и пустая строка = конец ответа
                for (;;) {
                    size_t te;
                    while ((te = rawBody.find("\r\n", pos)) == std::string::npos)
                        if (!readMore(rawBody.size() + 1)) return false;
                    if (te == pos) break;
                    pos = te + 2;
                }
                r.body = std::move(out);
                return true;
            }
            if (!readMore(pos + (size_t)len + 2)) return false;
            out.append(rawBody, pos, (size_t)len);
            if (out.size() > (16u << 20)) return false;
            pos += (size_t)len + 2;
        }
    } else if (contentLen >= 0) {
        if (!readMore((size_t)contentLen)) return false;
        r.body = rawBody.substr(0, (size_t)contentLen);
        return true;
    } else {
        // до закрытия (Connection: close)
        for (;;) {
            int n = BIO_read(bio_, buf, sizeof buf);
            if (n <= 0) break;
            rawBody.append(buf, size_t(n));
            if (rawBody.size() > 16 * 1024 * 1024) break;
        }
        r.body = rawBody;
        return true;
    }
}

} // namespace spred
