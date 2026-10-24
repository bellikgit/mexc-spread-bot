#pragma once

#include "spred/app/symbol_context.hpp"
#include "spred/app/session_options.hpp"

namespace spred {

void flushEvents(SymCtx& C, SessionOptions& options);
void applyEvent(SymCtx& C, const PendEv& e, SessionOptions& options);
void flushEvq(SymCtx& C, SessionOptions& options);
void tickSymbol(SymCtx& C, double now, SessionOptions& options);
void reportSymbol(SymCtx& C, double now, SessionOptions& options);
void printSummary(SymCtx& C, FILE* fp, bool sessionHeader, SessionOptions& options);

} // namespace spred
