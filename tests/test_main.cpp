#ifdef SPRED_LEGACY_REFERENCE
#define main originalApplicationMain
#include "reference/spred_legacy.cpp"
#undef main
#else
#include "spred/app/application.hpp"
#include "spred/app/replay.hpp"
#include "spred/core/config.hpp"
#include "spred/core/encoding.hpp"
#include "spred/core/rate_limit.hpp"
#include "spred/exchange/rest_client.hpp"
#include "spred/market/flow.hpp"
#include "spred/protocol/protobuf.hpp"
#include "spred/trading/live_ledger.hpp"
#include "spred/trading/paper.hpp"
using namespace spred;
#endif
#include "cases.hpp"
int main(int argc,char** argv) {
    if(argc>1&&std::string(argv[1])=="--run-cli") {
#ifdef SPRED_LEGACY_REFERENCE
        return originalApplicationMain(argc-1,argv+1);
#else
        return spred::runApplication(argc-1,argv+1);
#endif
    }
    try{return runCases();}
    catch(const std::exception& e){std::cerr<<"FAIL: "<<e.what()<<"\n";return 1;}
}
