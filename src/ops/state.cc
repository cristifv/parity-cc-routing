#include "src/ops/state.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <cerrno>

#include <cstdio>
#include <fstream>
#include <sstream>

namespace pcr {

namespace {
bool fsync_path(const std::string& p, int flags) {
  const int fd = ::open(p.c_str(), flags | O_CLOEXEC);
  if (fd < 0) return false;
  const bool ok = ::fsync(fd) == 0;
  ::close(fd);
  return ok;
}
}  // namespace

bool lock_state_dir(const std::string& dir, std::string* error) {
  const std::string path = dir + "/lock";
  const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
  if (fd < 0) {
    *error = "cannot open the state dir's lock file";
    return false;
  }
  if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
    *error = errno == EWOULDBLOCK
                 ? "another parity-cc-routing holds this state dir (phase 1 is single-instance)"
                 : "cannot lock the state dir";
    ::close(fd);
    return false;
  }
  return true;  // fd stays open: the lock lives as long as the process
}

uint64_t bump_incarnation(const std::string& dir, bool first_start, std::string* error) {
  const std::string path = dir + "/incarnation";
  uint64_t n = 0;
  {
    std::ifstream in(path);
    if (!in && !first_start) {
      *error = "the state dir has no incarnation: a new deployment starts once with --first-start=yes "
               "(with interaction-engine and parity-gateway fresh too); an emptied state dir would reuse "
               "epochs and sequence numbers";
      return 0;
    }
    if (in) {
      std::string s;
      std::getline(in, s);
      try {
        n = std::stoull(s);
      } catch (...) {
        *error = "the incarnation file is unreadable: refusing to guess (never reuse an epoch)";
        return 0;
      }
    }
  }
  ++n;
  const std::string tmp = path + ".tmp";
  {
    std::ofstream out(tmp, std::ios::trunc);
    if (!out) {
      *error = "cannot write the state dir";
      return 0;
    }
    out << n << "\n";
  }
  if (!fsync_path(tmp, O_RDONLY) || std::rename(tmp.c_str(), path.c_str()) != 0 ||
      !fsync_path(dir, O_RDONLY | O_DIRECTORY)) {
    *error = "cannot persist the incarnation";
    return 0;
  }
  return n;
}

void write_file(const std::string& path, const std::string& text) {
  std::ofstream out(path, std::ios::trunc);
  out << text;
}

void remove_file(const std::string& path) { std::remove(path.c_str()); }

}  // namespace pcr
