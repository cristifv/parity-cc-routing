// The phase-1 restart rule (PLAN §8): a monotonic incarnation counter in --state-dir, bumped and
// fsynced at every start before the gRPC port opens. leader_epoch = incarnation;
// attach_generation and delivery_seq start at (incarnation - 1) * 2^32, so nothing a later
// incarnation sends is ever at or below what an earlier one sent.
#pragma once

#include <cstdint>
#include <string>

namespace pcr {

// One process per state dir (CR-059): holds flock(LOCK_EX | LOCK_NB) on <state_dir>/lock for the
// life of the process (the descriptor is never closed). False, with *error, when another process
// holds it: a second RE on the same volume or PVC must not reuse this one's epochs.
bool lock_state_dir(const std::string& state_dir, std::string* error);
// Returns the new incarnation (>= 1), or 0 with *error set. A state dir without an incarnation
// file is refused unless `first_start` (CR-059): an emptied or replaced volume must never restart
// silently at 1 while IE or the gateway still hold the old epochs' watermarks. (A wall-clock floor
// was rejected: it would push state_version, a JSON integer on the desktop, past 2^53.)
uint64_t bump_incarnation(const std::string& state_dir, bool first_start, std::string* error);
// Health/drain files (ARCH §7.3): /tmp/parity-cc-routing.{healthy,drained}.
void write_file(const std::string& path, const std::string& text);
void remove_file(const std::string& path);

}  // namespace pcr
