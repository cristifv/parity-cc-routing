#pragma once

#include <app_config.h>
#include <metric.h>
#include <performance_record.h>
#include <shrinkage_record.h>
#include <fstream>
#include <iostream>

class data_manager {
public:
    explicit data_manager(const app_config& cfg) : cfg_(cfg) {}

    std::vector<metric> load_recent(int max_steps = 10000) {
        std::vector<metric> r;
        std::ifstream f(cfg_.metrics_csv);
        if (!f) return r;
        std::string line; std::getline(f, line);
        while (std::getline(f, line)) {
            std::istringstream ss(line); std::string tok; metric m;
            try {
                std::getline(ss,tok,','); m.timestamp_epoch        = std::stod(tok);
                std::getline(ss,tok,','); m.inbound_calls          = std::stod(tok);
                std::getline(ss,tok,','); m.aht_sec                = std::stod(tok);
                std::getline(ss,tok,','); m.current_agents         = std::stoi(tok);
                std::getline(ss,tok,','); m.agents_absent_planned  = std::stoi(tok);
                std::getline(ss,tok,','); m.agents_absent_unplanned= std::stoi(tok);
                std::getline(ss,tok,','); m.agents_on_break        = std::stoi(tok);
                std::getline(ss,tok,','); m.hour                   = std::stoi(tok);
                std::getline(ss,tok,','); m.dayofweek              = std::stoi(tok);
                std::getline(ss,tok,','); m.month                  = std::stoi(tok);
                r.push_back(m);
            } catch(...) {}
        }
        if ((int)r.size() > max_steps)
            r.erase(r.begin(), r.end() - max_steps);
        return r;
    }

        std::vector<performance_record> load_performance() {
        std::vector<performance_record> r;
        std::ifstream f(cfg_.perf_csv);
        if (!f) return r;
        std::string line; std::getline(f, line);
        while (std::getline(f, line)) {
            std::istringstream ss(line); std::string tok; performance_record pr;
            try {
                std::getline(ss,tok,','); pr.agent_id           = std::stoi(tok);
                std::getline(ss,tok,','); // week
                std::getline(ss,tok,','); // seniority
                std::getline(ss,tok,','); // prefers_ot
                std::getline(ss,tok,','); pr.fcr_rate           = std::stod(tok);
                std::getline(ss,tok,','); pr.csat_score         = std::stod(tok);
                std::getline(ss,tok,','); pr.adherence_rate     = std::stod(tok);
                std::getline(ss,tok,','); pr.avg_handle_time_s  = std::stod(tok);
                std::getline(ss,tok,','); pr.alert_response_s   = std::stod(tok);
                std::getline(ss,tok,','); pr.calls_handled      = std::stoi(tok);
                std::getline(ss,tok,','); pr.quality_score      = std::stod(tok);
                r.push_back(pr);
            } catch(...) {}
        }
        return r;
    }

    std::vector<shrinkage_record> load_shrinkage() {
        std::vector<shrinkage_record> r;
        std::ifstream f(cfg_.shrink_csv);
        if (!f) return r;
        std::string line; std::getline(f, line);
        while (std::getline(f, line)) {
            std::istringstream ss(line); std::string tok; shrinkage_record sr;
            try {
                std::getline(ss,tok,','); // date
                std::getline(ss,tok,','); // dow
                std::getline(ss,tok,','); // month
                std::getline(ss,tok,','); sr.total_scheduled = std::stoi(tok);
                std::getline(ss,tok,','); sr.absent_vacation = std::stoi(tok);
                std::getline(ss,tok,','); sr.absent_training = std::stoi(tok);
                std::getline(ss,tok,','); sr.absent_sick     = std::stoi(tok);
                std::getline(ss,tok,','); sr.absent_noshow   = std::stoi(tok);
                std::getline(ss,tok,','); sr.on_break        = std::stoi(tok);
                std::getline(ss,tok,','); sr.on_coaching     = std::stoi(tok);
                std::getline(ss,tok,','); sr.on_aux          = std::stoi(tok);
                r.push_back(sr);
            } catch(...) {}
        }
        return r;
    }

private:
    const app_config& cfg_;
};
