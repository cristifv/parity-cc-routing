#pragma once

#include <metric.h>
#include <string>

struct metrics_stream {

  metrics_stream(const std::string &csv_path, const std::string &site,
                 const std::string &queue)
      : site_(site), queue_(queue) {
    file_.open(csv_path);
    if (!file_) {
      throw std::runtime_error("Nu pot deschide " + csv_path);
    }
    std::getline(file_, header_);
  }

  metric next() {
    if (file_.eof() || file_.fail()) {
      file_.clear();
      file_.seekg(0);
      std::getline(file_, header_); // sărim din nou header-ul
    }

    std::string line;
    while (std::getline(file_, line)) {
      std::istringstream ss(line);
      std::string tok;
      metric m;
      m.site_id = site_;
      m.queue_id = queue_;
      try {
        std::getline(ss, tok, ',');
        m.timestamp_epoch = std::stod(tok);
        std::getline(ss, tok, ',');
        m.inbound_calls = std::stod(tok);
        std::getline(ss, tok, ',');
        m.aht_sec = std::stod(tok);
        std::getline(ss, tok, ',');
        m.current_agents = std::stoi(tok);
        std::getline(ss, tok, ',');
        m.agents_absent_planned = std::stoi(tok);
        std::getline(ss, tok, ',');
        m.agents_absent_unplanned = std::stoi(tok);
        std::getline(ss, tok, ',');
        m.agents_on_break = std::stoi(tok);
        std::getline(ss, tok, ',');
        m.hour = std::stoi(tok);
        std::getline(ss, tok, ',');
        m.dayofweek = std::stoi(tok);
        std::getline(ss, tok, ',');
        m.month = std::stoi(tok);
        return m;
      } catch (...) {
        continue;
      }
    }

    throw std::runtime_error("CSV gol sau corupt pentru " + site_ + "/" +
                             queue_);
  }

private:
  std::ifstream file_;
  std::string header_;
  std::string site_;
  std::string queue_;
};
