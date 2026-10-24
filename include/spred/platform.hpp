#pragma once

// Linux/POSIX and standard-library dependencies inherited from the original bot.
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <sys/select.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

#include <fcntl.h>
#include <poll.h>
#include <cerrno>
#include <signal.h>
#include <csignal>
#include <cstdarg>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <array>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <atomic>
#include <cctype>
#include <utility>
