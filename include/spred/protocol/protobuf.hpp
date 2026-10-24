#pragma once

#include "spred/platform.hpp"

namespace spred {

namespace pb {

struct Reader {
    const unsigned char* p;
    const unsigned char* end;

    bool varint(uint64_t& out) {
        out = 0;
        int shift = 0;
        while (p < end) {
            unsigned char b = *p++;
            out |= (uint64_t)(b & 0x7F) << shift;
            if (!(b & 0x80)) return true;
            shift += 7;
            if (shift >= 64) return false;
        }
        return false;
    }

    // [fix F6] проверка остатка ДО сдвига указателя: p += v с огромным v
    // давал переполнение указателя (UB) и обход проверки p <= end
    bool skipField(uint64_t tag) {
        int wire = int(tag & 7);
        uint64_t v;
        switch (wire) {
            case 0: return varint(v);
            case 1: if ((uint64_t)(end - p) < 8) return false; p += 8; return true;
            case 2: {
                if (!varint(v)) return false;
                if (v > (uint64_t)(end - p)) return false;
                p += v;
                return true;
            }
            case 5: if ((uint64_t)(end - p) < 4) return false; p += 4; return true;
            default: return false;
        }
    }
};

inline bool readTag(Reader& r, uint64_t& tag) { return r.varint(tag); }

} // namespace pb

} // namespace spred
