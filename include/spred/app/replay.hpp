#pragma once

#include "spred/platform.hpp"

namespace spred {

// [double recvMs][uint32 len][payload]xN, только бинарные фреймы.
struct ReplaySrc {
    FILE* f = nullptr;
    bool haveNext = false, eof = false;
    double nextRecvMs = 0;
    std::string nextPayload;
    bool readNext() {
        if (eof || !f) return false;
        double recvMs = 0;
        uint32_t len = 0;
        if (std::fread(&recvMs, sizeof recvMs, 1, f) != 1 ||
            std::fread(&len, sizeof len, 1, f) != 1) { eof = true; return false; }
        nextPayload.assign(len, '\0');
        if (len && std::fread(&nextPayload[0], 1, len, f) != len) {
            eof = true;
            return false;
        }
        nextRecvMs = recvMs;
        haveNext = true;
        return true;
    }
};

extern ReplaySrc RSrc;

} // namespace spred
