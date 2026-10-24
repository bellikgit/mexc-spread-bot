#include "spred/protocol/mexc.hpp"
#include "spred/protocol/protobuf.hpp"

namespace spred {

static bool parseDouble(const std::string& s, double& out) {
    if (s.empty()) return false;
    char* e = nullptr;
    out = strtod(s.c_str(), &e);
    return e && *e == '\0';
}


static Level parseLimitDepth(const unsigned char* data, size_t len) {
    Level lv;
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        if ((tag & 7) == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            std::string s(reinterpret_cast<const char*>(r.p), size_t(l));
            if ((tag >> 3) == 1) parseDouble(s, lv.price);
            else if ((tag >> 3) == 2) parseDouble(s, lv.qty);
            r.p += l;
        } else if (!r.skipField(tag)) break;
    }
    return lv;
}

static void parseLimitDepths(const unsigned char* data, size_t len, MexcPush& out) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        if ((tag & 7) == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            int field = int(tag >> 3);
            if (field == 1) out.asks.push_back(parseLimitDepth(r.p, size_t(l)));
            else if (field == 2) out.bids.push_back(parseLimitDepth(r.p, size_t(l)));
            r.p += l;
        } else if (!r.skipField(tag)) break;
    }
    out.hasLimitDepth = !out.bids.empty() && !out.asks.empty();
}

static void parseBookTicker(const unsigned char* data, size_t len, MexcPush& out) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        if ((tag & 7) == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            std::string s(reinterpret_cast<const char*>(r.p), size_t(l));
            switch (tag >> 3) {
                case 1: parseDouble(s, out.bidP); break;
                case 2: parseDouble(s, out.bidQ); break;
                case 3: parseDouble(s, out.askP); break;
                case 4: parseDouble(s, out.askQ); break;
                default: break;
            }
            r.p += l;
        } else if (!r.skipField(tag)) break;
    }
    out.hasTicker = out.bidP > 0 && out.askP > 0 && out.askP > out.bidP;
}

static void parseDeal(const unsigned char* data, size_t len, MexcPush::Deal& d) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        if ((tag & 7) == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            std::string s(reinterpret_cast<const char*>(r.p), size_t(l));
            if ((tag >> 3) == 1) parseDouble(s, d.price);
            else if ((tag >> 3) == 2) parseDouble(s, d.qty);
            r.p += l;
        } else if ((tag & 7) == 0) {
            uint64_t v;
            if (!r.varint(v)) break;
            if ((tag >> 3) == 3) d.side = int(v);
            else if ((tag >> 3) == 4) d.timeMs = (int64_t)v;
        } else if (!r.skipField(tag)) break;
    }
}

static void parseDeals(const unsigned char* data, size_t len, MexcPush& out) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        if ((tag & 7) == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            if ((tag >> 3) == 1) {
                MexcPush::Deal d;
                parseDeal(r.p, size_t(l), d);
                if (d.price > 0 && d.qty > 0) out.deals.push_back(d);
            }
            r.p += l;
        } else if (!r.skipField(tag)) break;
    }
    out.hasDeals = !out.deals.empty();
}

// [v3.0 L2] приватные protobuf-парсеры (поля — из официальных .proto MEXC)
static void parsePrivOrder(const unsigned char* data, size_t len,
                           MexcPush::PrivOrder& o) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        int field = int(tag >> 3);
        int wire = int(tag & 7);
        if (wire == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            std::string s(reinterpret_cast<const char*>(r.p), size_t(l));
            double dv;
            switch (field) {
                case 1: o.id = s; break;
                case 2: o.clientId = s; break;
                case 3: if (parseDouble(s, dv)) o.price = dv; break;
                case 4: if (parseDouble(s, dv)) o.qty = dv; break;
                case 5: if (parseDouble(s, dv)) o.amount = dv; break;
                case 6: if (parseDouble(s, dv)) o.avgPrice = dv; break;
                case 11: if (parseDouble(s, dv)) o.remainQty = dv; break;
                case 12: if (parseDouble(s, dv)) o.lastDealQty = dv; break;
                case 13: if (parseDouble(s, dv)) o.cumQty = dv; break;
                case 14: if (parseDouble(s, dv)) o.cumAmt = dv; break;
                default: break;
            }
            r.p += l;
        } else if (wire == 0) {
            uint64_t v;
            if (!r.varint(v)) break;
            switch (field) {
                case 7: o.orderType = int(v); break;
                case 8: o.tradeType = int(v); break;
                case 9: o.isMaker = (v != 0); break;
                case 15: o.status = int(v); break;
                case 16: o.createTime = (int64_t)v; break;
                default: break;
            }
        } else if (!r.skipField(tag)) break;
    }
}

static void parsePrivDeal(const unsigned char* data, size_t len,
                          MexcPush::PrivDeal& d) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        int field = int(tag >> 3);
        int wire = int(tag & 7);
        if (wire == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            std::string s(reinterpret_cast<const char*>(r.p), size_t(l));
            double dv;
            switch (field) {
                case 1: if (parseDouble(s, dv)) d.price = dv; break;
                case 2: if (parseDouble(s, dv)) d.qty = dv; break;
                case 3: if (parseDouble(s, dv)) d.amount = dv; break;
                case 7: d.tradeId = s; break;
                case 8: d.clientOrderId = s; break;
                case 9: d.orderId = s; break;
                case 10: if (parseDouble(s, dv)) d.feeAmount = dv; break;
                case 11: d.feeCurrency = s; break;
                default: break;
            }
            r.p += l;
        } else if (wire == 0) {
            uint64_t v;
            if (!r.varint(v)) break;
            switch (field) {
                case 4: d.tradeType = int(v); break;
                case 5: d.isMaker = (v != 0); break;
                case 6: d.isSelfTrade = (v != 0); break;
                case 12: d.timeMs = (int64_t)v; break;
                default: break;
            }
        } else if (!r.skipField(tag)) break;
    }
}

static void parsePrivAccount(const unsigned char* data, size_t len,
                             MexcPush::PrivAccount& a) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        int field = int(tag >> 3);
        int wire = int(tag & 7);
        if (wire == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            std::string s(reinterpret_cast<const char*>(r.p), size_t(l));
            double dv;
            switch (field) {
                case 1: a.coin = s; break;
                case 3: if (parseDouble(s, dv)) a.balance = dv; break;
                case 4: if (parseDouble(s, dv)) a.balanceChange = dv; break;
                case 5: if (parseDouble(s, dv)) a.frozen = dv; break;
                case 6: if (parseDouble(s, dv)) a.frozenChange = dv; break;
                default: break;
            }
            r.p += l;
        } else if (!r.skipField(tag)) break;
    }
}

bool parsePush(const std::string& msg, MexcPush& out) {
    pb::Reader r{reinterpret_cast<const unsigned char*>(msg.data()),
                 reinterpret_cast<const unsigned char*>(msg.data()) + msg.size()};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        int field = int(tag >> 3);
        int wire = int(tag & 7);
        if (wire == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            if (field == 1) {
                out.channel.assign(reinterpret_cast<const char*>(r.p), size_t(l));
            } else if (field == 3) {
                out.symbol.assign(reinterpret_cast<const char*>(r.p), size_t(l));
            } else if (field == 303) {
                parseLimitDepths(r.p, size_t(l), out);
            } else if (field == 314) {
                parseDeals(r.p, size_t(l), out);
            } else if (field == 315) {
                parseBookTicker(r.p, size_t(l), out);
            } else if (field == 304) {   // [v3.0] spot@private.orders.v3.api.pb
                parsePrivOrder(r.p, size_t(l), out.pord);
                out.hasPrivOrder = true;
            } else if (field == 306) {   // [v3.0] spot@private.deals.v3.api.pb
                parsePrivDeal(r.p, size_t(l), out.pdeal);
                out.hasPrivDeal = true;
            } else if (field == 307) {   // [v3.0] spot@private.account.v3.api.pb
                parsePrivAccount(r.p, size_t(l), out.pacct);
                out.hasPrivAcct = true;
            }
            r.p += l;
        } else if (wire == 0) {
            uint64_t v;
            if (!r.varint(v)) break;
            if (field == 6) out.sendTimeMs = (int64_t)v;
        } else if (!r.skipField(tag)) break;
    }
    return out.hasTicker || out.hasLimitDepth || out.hasDeals ||
           out.hasPrivOrder || out.hasPrivDeal || out.hasPrivAcct;
}

} // namespace spred
