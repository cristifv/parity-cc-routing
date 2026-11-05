// A minimal HTTP/1.1 server for /livez /readyz /metrics (9810) and the test control (9811). One
// thread, one request per connection, bounded sizes, and a 2 s budget for reading a whole request
// (and another for writing the reply: CR-060); nothing on the call path.
#pragma once

#include <atomic>
#include <functional>
#include <string>
#include <thread>

namespace pcr {

struct HttpReply {
  int status = 200;
  std::string content_type = "text/plain; charset=utf-8";
  std::string body;
};

class HttpServer {
 public:
  using Handler = std::function<HttpReply(const std::string& method, const std::string& path,
                                          const std::string& body)>;
  // addr "host:port"; returns false (and logs) if it can't bind.
  bool start(const std::string& addr, Handler h, size_t max_body);
  void stop();
  int port() const { return port_; }
  ~HttpServer() { stop(); }

 private:
  void loop();
  void serve(int fd);
  int fd_ = -1;
  int port_ = 0;
  size_t max_body_ = 0;
  Handler handler_;
  std::atomic<bool> stop_{false};
  std::thread thread_;
};

// Splits "host:port"; false when malformed.
bool split_addr(const std::string& addr, std::string* host, int* port);

}  // namespace pcr
