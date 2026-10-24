#pragma once
#include <iostream>
#include <stdexcept>
#include <iomanip>

static int checks=0;
static void require(bool pass, const char* message) {
    ++checks;
    if (!pass) throw std::runtime_error(message);
}
static bool near(double a,double b,double eps=1e-9) {return std::fabs(a-b)<eps;}
static std::string vi(uint64_t n) {
    std::string out;
    do {unsigned char c=n&127;n>>=7;if(n)c|=128;out.push_back(char(c));}while(n);
    return out;
}
static std::string fld(int id,const std::string& value) {return vi((uint64_t(id)<<3)|2)+vi(value.size())+value;}
static int runCases() {
    g_replay=true;g_simNowMs=100000;
    require(near(nowSecs(),100),"replay time");
    setenv("MM_TEST_CONFIG","2.5",1);require(near(envD("MM_TEST_CONFIG",9),2.5),"environment number");
    setenv("MM_TEST_CONFIG","bad",1);require(near(envD("MM_TEST_CONFIG",9),9),"environment fallback");
    setenv("MM_TEST_CONFIG","3",1);setenv("MM_TEST_CONFIG@TESTUSDT","4",1);
    require(near(envDsym("MM_TEST_CONFIG","TESTUSDT",0),4),"symbol override");
    auto symbols=splitSymbols(" BTCUSDT, ETHUSDT , ,X ");require(symbols.size()==3&&symbols[1]=="ETHUSDT","symbols trim");
    require(b64((const unsigned char*)"abc",3)=="YWJj","base64");
    require(b64((const unsigned char*)"a",1)=="YQ==","base64 padding");
    require(hmacSha256Hex("key","The quick brown fox jumps over the lazy dog")=="f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8","hmac vector");
    require(fmtDec(0.3)=="0.3"&&fmtDec(-0.0)=="0","decimal formatting");
    require(base36(35)=="z"&&base36(36)=="10","client id encoding");
    auto signedQs=buildSignedQs(std::string(100,'x'),123456,10000,"test-secret-not-real");
    require(signedQs.find("timestamp=123456&recvWindow=10000&signature=")!=std::string::npos,"long signed query");
    double number=0;std::string word;
    require(jsonFindNum("{\"n\":\"1.25\"}","n",number)&&near(number,1.25),"json number");
    require(!jsonFindNum("{}","n",number),"json missing");
    require(jsonFindStr("{\"id\":123}","id",word)&&word=="123","json numeric id");
    require(jsonFindStr("{\"id\":\"abc\"}","id",word)&&word=="abc","json string id");
    auto array=jsonExtractArray("{\"balances\":[{\"x\":\"}\\\"[\"},{\"x\":2}]}","balances");
    require(jsonSplitObjects(array).size()==2,"json nested string delimiters");
    require(dechunk("3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n")=="abcde","http chunks");
    std::string v=vi(300);pb::Reader reader{(const unsigned char*)v.data(),(const unsigned char*)v.data()+v.size()};uint64_t n=0;
    require(reader.varint(n)&&n==300,"protobuf varint");
    std::string malformed(12,char(0x80));pb::Reader bad{(const unsigned char*)malformed.data(),(const unsigned char*)malformed.data()+malformed.size()};
    require(!bad.varint(n),"protobuf malformed varint");
    auto ticker=fld(1,"100")+fld(2,"4")+fld(3,"102")+fld(4,"6");
    auto payload=fld(3,"TESTUSDT")+fld(315,ticker);MexcPush push;
    require(parsePush(payload,push)&&push.hasTicker&&push.symbol=="TESTUSDT"&&near(push.askP,102),"ticker decoder");
    MexcPush empty;require(!parsePush("",empty),"empty protobuf");
    MexcPush truncated;require(!parsePush(vi((315<<3)|2)+vi(1000)+"x",truncated),"truncated protobuf");
    BookState book;book.bids={{100,10},{99,20}};book.asks={{102,10},{103,20}};
    require(book.ok()&&near(book.mid(),101),"book validation");
    require(near(book.imbalance(),0.5),"book imbalance");
    double filled=0;double avg=walkBook(book,true,15,filled);
    require(near(filled,15)&&near(avg,(1020.0+515)/15),"book vwap");
    require(near(walkBookSkip(book,true,5,filled,1),103)&&near(filled,5),"stress book vwap");
    require(near(walkBudgetBuy(book,1020),10),"budget sizing");
    BookState crossed=book;crossed.asks[0].price=99;require(!crossed.ok(),"crossed book");
    TokenBucket bucket;bucket.init(2);require(bucket.take()&&bucket.take()&&!bucket.take(),"token capacity");
    g_simNowMs+=500;require(bucket.take(),"token refill");
    FlowTracker flow;flow.minDeals=1;MexcPush::Deal deal;deal.qty=3;deal.side=1;
    flow.onDeal(deal,100,false,0);require(near(flow.tfi(100),1)&&flow.run(100)==1,"trade flow");
    require(near(flow.tfi(200),0),"trade flow expiry");
    PaperTrader trader;trader.quote=trader.initialQuote=10000;
    trader.applyTrade(2,100);trader.applyTrade(-1,110);trader.applyTrade(-2,90);trader.applyTrade(1,80);
    require(near(trader.base,0)&&near(trader.quote,10010)&&near(trader.realized,10),"paper accounting");
    g_simNowMs=100000;PaperTrader orders;orders.rng.seed(42);orders.rate.init(100);orders.orderLatMs=100;orders.cancelLatMs=100;
    std::vector<std::string> events;orders.requestQuote(orders.buy,Order::BUY,100,1,0.01,100,events);
    require(orders.buy.st==QState::WAIT_PLACE,"paper placement");
    orders.advance(orders.buy,book,0.01,100.05);require(orders.buy.st==QState::WAIT_PLACE,"paper latency");
    orders.advance(orders.buy,book,0.01,100.11);require(orders.buy.st==QState::LIVE&&near(orders.buy.queueAhead,10),"paper queue");
    orders.dealOffInit=true;orders.requestCancel(orders.buy,100.2,events);require(orders.buy.st==QState::WAIT_CANCEL,"paper cancel");
    orders.advance(orders.buy,book,0.01,100.31);require(orders.buy.st==QState::NONE&&orders.ghostBuy.active,"paper late-fill ghost");
    LiveLedger ledger;ledger.applyRealFill(true,100,2,0.1,"USDT","TEST","USDT",100,0);
    ledger.applyRealFill(false,110,1,0.2,"USDT","TEST","USDT",101,0);
    require(near(ledger.baseInt,1)&&near(ledger.quoteNet,-90.3)&&near(ledger.realized,10),"live ledger accounting");
    require(near(ledger.sessionPnl(105),14.7)&&near(ledger.feesQuote,0.3),"live ledger pnl and fees");
    ledger.updateMarkouts(161,105);require(std::isfinite(ledger.markout(2)),"live markouts");
    ledger.applyFee(0.01,"TEST","TEST","USDT");require(near(ledger.baseInt,0.99),"base currency fee");
    // Multiple deterministic, non-network accounting paths; the legacy and modular snapshots must match exactly.
    std::mt19937 random(2026);
    for(int scenario=0;scenario<50;++scenario){
        PaperTrader t;t.quote=t.initialQuote=10000;
        for(int i=0;i<100;++i){double q=(int(random()%21)-10)*0.01;double px=100+int(random()%200)*0.01;t.applyTrade(q,px);}
        require(std::isfinite(t.totalPnl(101)),"finite accounting path");
        std::cout<<"path "<<scenario<<" "<<std::setprecision(17)<<t.base<<" "<<t.quote<<" "<<t.realized<<" "<<t.avgEntry<<"\n";
    }
    ReplaySrc replay;replay.f=std::tmpfile();require(replay.f!=nullptr,"temporary replay");
    double when=1234;uint32_t length=payload.size();fwrite(&when,sizeof when,1,replay.f);fwrite(&length,sizeof length,1,replay.f);fwrite(payload.data(),1,payload.size(),replay.f);rewind(replay.f);
    require(replay.readNext()&&near(replay.nextRecvMs,1234)&&replay.nextPayload==payload,"raw replay frame");
    require(!replay.readNext()&&replay.eof,"raw replay eof");fclose(replay.f);
    std::cout<<"PASS "<<checks<<" checks\n";
    return 0;
}
