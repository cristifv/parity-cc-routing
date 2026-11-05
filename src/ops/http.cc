#include "src/ops/http.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>

#include "src/ops/log.h"

namespace pcr {

namespace {

// CR-060: a whole-request budget. SO_RCVTIMEO bounds one recv() only, so a client sending a byte
// every second could hold this one-thread loop (and /livez with it) for as long as it likes.
constexpr auto kRequestBudget = std::chrono::seconds(2);

// Waits until fd is readable (or writable) or the deadline passes.
bool ready(int fd, short events, std::chrono::steady_clock::time_point deadline) {
  const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
  if (left <= 0) return false;
  pollfd p{fd, events, 0};
  return ::poll(&p, 1, static_cast<int>(left)) > 0;
}

}  // namespace

bool split_addr(const std::string& addr, std::string* host, int* port) {
  const auto c = addr.rfind(':');
  if (c == std::string::npos) return false;
  *host = addr.substr(0, c);
  try {
    *port = std::stoi(addr.substr(c + 1));
  } catch (...) {
    return false;
  }
  return *port >= 0 && *port <= 65535;
}

bool HttpServer::start(const std::string& addr, Handler h, size_t max_body) {
  std::string host;
  int port = 0;
  if (!split_addr(addr, &host, &port)) return false;
  fd_ = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
  if (fd_ < 0) return false;
  int one = 1;
  ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
  sockaddr_in sa{};
  sa.sin_family = AF_INET;
  sa.sin_port = htons(static_cast<uint16_t>(port));
  if (::inet_pton(AF_INET, host.c_str(), &sa.sin_addr) != 1 ||
      ::bind(fd_, reinterpret_cast<sockaddr*>(&sa), sizeof sa) != 0 || ::listen(fd_, 64) != 0) {
    LogRec("error", "startup.refused", "http: cannot listen").error("unavailable").attr("addr", addr);
    ::close(fd_);
    fd_ = -1;
    return false;
  }
  socklen_t len = sizeof sa;
  ::getsockname(fd_, reinterpret_cast<sockaddr*>(&sa), &len);
  port_ = ntohs(sa.sin_port);
  handler_ = std::move(h);
  max_body_ = max_body;
  thread_ = std::thread([this] { loop(); });
  return true;
}

void HttpServer::stop() {
  stop_ = true;
  if (thread_.joinable()) thread_.join();
  if (fd_ >= 0) ::close(fd_);
  fd_ = -1;
}

void HttpServer::loop() {
  while (!stop_) {
    pollfd p{fd_, POLLIN, 0};
    if (::poll(&p, 1, 200) <= 0) continue;
    int c = ::accept4(fd_, nullptr, nullptr, SOCK_CLOEXEC);
    if (c < 0) continue;
    timeval tv{2, 0};
    ::setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    ::setsockopt(c, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
    serve(c);
    ::close(c);
  }
}

void HttpServer::serve(int fd) {
  std::string in;
  char buf[8192];
  size_t hdr_end = std::string::npos;
  auto deadline = std::chrono::steady_clock::now() + kRequestBudget;
  while (hdr_end == std::string::npos) {
    if (!ready(fd, POLLIN, deadline)) return;
    const ssize_t n = ::recv(fd, buf, sizeof buf, 0);
    if (n <= 0) return;
    in.append(buf, static_cast<size_t>(n));
    hdr_end = in.find("\r\n\r\n");
    if (in.size() > 16384 && hdr_end == std::string::npos) return;
  }
  const std::string head = in.substr(0, hdr_end);
  const auto sp1 = head.find(' ');
  const auto sp2 = head.find(' ', sp1 + 1);
  if (sp1 == std::string::npos || sp2 == std::string::npos) return;
  const std::string method = head.substr(0, sp1);
  const std::string path = head.substr(sp1 + 1, sp2 - sp1 - 1);
  size_t clen = 0;
  for (size_t pos = head.find("\r\n"); pos != std::string::npos; pos = head.find("\r\n", pos + 2)) {
    const std::string line = head.substr(pos + 2, head.find("\r\n", pos + 2) - pos - 2);
    if (line.size() > 15 && strncasecmp(line.c_str(), "content-length:", 15) == 0) {
      try {
        clen = std::stoul(line.substr(15));
      } catch (...) {
        return;
      }
    }
  }
  HttpReply rep;
  if (clen > max_body_) {
    rep = HttpReply{413, "text/plain", "too large\n"};
  } else {
    std::string body = in.substr(hdr_end + 4);
    while (body.size() < clen) {
      if (!ready(fd, POLLIN, deadline)) return;
      const ssize_t n = ::recv(fd, buf, sizeof buf, 0);
      if (n <= 0) return;
      body.append(buf, static_cast<size_t>(n));
    }
    body.resize(clen);
    try {
      rep = handler_(method, path, body);
    } catch (const std::exception& e) {
      rep = HttpReply{400, "text/plain", std::string("bad request: ") + e.what() + "\n"};
    }
  }
  std::string out = "HTTP/1.1 " + std::to_string(rep.status) + " X\r\nContent-Type: " +
                    rep.content_type + "\r\nContent-Length: " + std::to_string(rep.body.size()) +
                    "\r\nConnection: close\r\n\r\n" + rep.body;
  size_t off = 0;
  deadline = std::chrono::steady_clock::now() + kRequestBudget;  // the reply gets its own budget
  while (off < out.size()) {
    if (!ready(fd, POLLOUT, deadline)) return;
    const ssize_t n = ::send(fd, out.data() + off, out.size() - off, MSG_NOSIGNAL);
    if (n <= 0) return;
    off += static_cast<size_t>(n);
  }
}

}  // namespace pcr
