#include "src/opt/levers.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <numeric>

namespace pcr::opt {

namespace {

using model::AgentQueue;
using model::Quality;

std::vector<model::Importance> importance(const std::vector<LeverQueue>& q) {
  std::vector<model::Importance> out;
  for (const auto& x : q) out.push_back(model::Importance{x.alpha, x.beta, x.vip_alpha, x.vip_beta});
  return out;
}

model::AQueue aqueue(const LeverQueue& x, bool horizon) {
  model::AQueue a;
  a.lambda = horizon ? x.horizon_lambda : x.lambda;
  a.vip_lambda = std::min(a.lambda, horizon ? x.horizon_vip_lambda : x.vip_lambda);
  a.mu = x.mu;
  a.theta = x.theta;
  a.delay_s = x.delay_s;
  a.sla_s = x.sla_s;
  a.vip_sla_s = x.vip_sla_s;
  a.weight = x.weight;
  a.min_agents = x.min_agents;
  a.max_agents = x.max_agents;
  a.vip_reserved = x.vip_reserved;
  return a;
}

// A queue's share of J (its missed-SL and abandoned calls per second, weighted).
double contribution(const LeverQueue& q, const AgentQueue& m) {
  std::vector<model::AQueue> one{aqueue(q, false)};
  return model::objective(one, {m}, importance({q}));
}

Quality label(const AgentQueue& m) { return m.m.ok ? m.m.quality : Quality::kLong; }

// Queue i's metrics in a model of its own pool only, with `extra` more agents that take only queue i
// (> 0) or `extra` fewer of the pool's agents (< 0, a pool of queue i alone): lever (c)'s what-if.
model::QueueMetrics with_agents(const std::vector<model::AQueue>& aq, const std::vector<model::AAgent>& ag,
                                const model::AgentResult& base, int i, int extra, const model::AgentOptions& mo,
                                model::Stats* st) {
  std::vector<int> pq;
  const int p = base.q[i].pool;
  if (p >= 0) {
    pq = base.pools[p].queues;
  } else {
    pq = {i};
  }
  std::vector<int> local(aq.size(), -1);
  std::vector<model::AQueue> sq;
  for (int x : pq) {
    local[x] = static_cast<int>(sq.size());
    sq.push_back(aq[x]);
  }
  std::vector<model::AAgent> sa;
  if (p >= 0) {
    for (int a : base.pools[p].agents) {
      model::AAgent y;
      const model::AAgent& x = ag[a];
      for (size_t k = 0; k < x.queues.size(); ++k) {
        const int qi = x.queues[k];
        if (qi < 0 || static_cast<size_t>(qi) >= aq.size() || local[qi] < 0) continue;
        y.queues.push_back(local[qi]);
        y.tiers.push_back(k < x.tiers.size() ? x.tiers[k] : 1);
        y.speeds.push_back(k < x.speeds.size() ? x.speeds[k] : 1.0);
      }
      y.vip = x.vip;
      if (!y.queues.empty()) sa.push_back(std::move(y));
    }
  }
  if (extra < 0) {
    sa.resize(sa.size() - std::min<size_t>(sa.size(), static_cast<size_t>(-extra)));
  } else {
    for (int k = 0; k < extra; ++k) sa.push_back(model::AAgent{{local[i]}, {1}, {1.0}, false});
  }
  model::AgentModel m(std::move(sq), std::move(sa), mo, st);
  return m.base().q[local[i]].m;
}

}  // namespace

void model_inputs(const ModelSnapshot& s, const std::vector<LeverQueue>& q, std::vector<model::AQueue>* aq,
                  std::vector<model::AAgent>* ag, model::Dispatch* d) {
  aq->clear();
  for (const auto& x : q) aq->push_back(aqueue(x, false));
  ag->clear();
  *d = s.tiers ? model::Dispatch::kTiers : model::Dispatch::kLongestIdle;
  for (const auto& a : s.agents) {
    if (!a.counts || a.set.empty()) continue;
    model::AAgent x;
    for (size_t k = 0; k < a.set.size(); ++k) {
      const int i = a.set[k].first;
      if (i < 0 || static_cast<size_t>(i) >= q.size()) continue;
      x.queues.push_back(i);
      x.tiers.push_back(*d == model::Dispatch::kTiers ? a.set[k].second : 1);
      x.speeds.push_back(k < a.speed.size() ? a.speed[k] : 1.0);
    }
    x.vip = a.vip;
    if (!x.queues.empty()) ag->push_back(std::move(x));
  }
}

LeverResult plan_levers(const ModelSnapshot& s, const std::vector<LeverQueue>& q, LeverMemory* mem, bool search,
                        bool recommend, const LeverOptions& o, model::Stats* st) {
  LeverResult r;
  const auto t0 = std::chrono::steady_clock::now();
  auto over = [&] {
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t0).count() >
           o.time_cap_us;
  };
  std::vector<model::AQueue> aq;
  std::vector<model::AAgent> ag;
  model::Dispatch d;
  model_inputs(s, q, &aq, &ag, &d);
  r.dispatch = d;
  r.counted = static_cast<int>(ag.size());
  model::AgentOptions mo = o.model;
  mo.dispatch = d;
  model::AgentModel m(aq, ag, mo, st);
  const model::AgentResult& base = m.base();
  if (!base.ok) {
    r.why = base.why.empty() ? "the model refused the inputs" : base.why;
    return r;
  }
  const size_t nq = q.size();
  const auto imp = importance(q);
  r.pools = static_cast<int>(base.pools.size());
  r.current = base.q;
  r.planned = base.q;
  r.j_current = r.j_planned = model::objective(aq, r.current, imp);
  std::vector<double> w(nq), dl(nq);
  for (size_t i = 0; i < nq; ++i) {
    w[i] = q[i].weight;
    dl[i] = q[i].delay_s;
  }
  r.w_planned = w;
  r.d_planned = dl;
  if (search) {
    std::vector<int> cand;
    for (size_t i = 0; i < nq; ++i) {
      if (q[i].lambda > 0 && r.current[i].m.ok && r.current[i].m.quality == Quality::kExact) cand.push_back(static_cast<int>(i));
    }
    std::stable_sort(cand.begin(), cand.end(), [&](int x, int y) {
      return contribution(q[x], r.current[x]) > contribution(q[y], r.current[y]);
    });
    std::vector<double> bw = w, bd = dl;
    std::vector<AgentQueue> bm = r.current;
    double bj = r.j_current;
    for (bool improved = true; improved;) {
      improved = false;
      double sj = bj;
      std::vector<double> sw, sd;
      std::vector<AgentQueue> smet;
      for (int i : cand) {
        std::vector<std::pair<double, double>> moves;
        const double wu = std::clamp(bw[i] * o.weight_step, q[i].weight_min, q[i].weight_max);
        const double wd = std::clamp(bw[i] / o.weight_step, q[i].weight_min, q[i].weight_max);
        if (std::abs(wu - bw[i]) > 1e-9) moves.emplace_back(wu, bd[i]);
        if (std::abs(wd - bw[i]) > 1e-9) moves.emplace_back(wd, bd[i]);
        // A delay matters only where some agent takes the queue as a SECONDARY.
        if (d == model::Dispatch::kTiers && base.q[i].P < base.q[i].R) {
          const double du = std::clamp(bd[i] + o.delay_step_s, q[i].delay_min_s, q[i].delay_max_s);
          const double dd = std::clamp(bd[i] - o.delay_step_s, q[i].delay_min_s, q[i].delay_max_s);
          if (std::abs(du - bd[i]) > 1e-9) moves.emplace_back(bw[i], du);
          if (std::abs(dd - bd[i]) > 1e-9) moves.emplace_back(bw[i], dd);
        }
        for (const auto& [cw_i, cd_i] : moves) {
          if (r.evaluated >= o.max_candidates || over()) {
            r.capped = true;
            break;
          }
          std::vector<double> cw = bw, cd = bd;
          cw[i] = cw_i;
          cd[i] = cd_i;
          std::vector<AgentQueue> met = m.evaluate(cw, cd);
          ++r.evaluated;
          // Only exact results drive a lever: a candidate that makes any queue approx is no candidate.
          bool exact = true;
          for (size_t k = 0; k < nq && exact; ++k) {
            exact = met[k].m.ok && (met[k].m.quality == Quality::kExact || label(r.current[k]) != Quality::kExact);
          }
          if (!exact) continue;
          const double j = model::objective(aq, met, imp);
          if (j < sj - 1e-15) {
            sj = j;
            sw = std::move(cw);
            sd = std::move(cd);
            smet = std::move(met);
          }
        }
        if (r.capped) break;
      }
      if (!sw.empty()) {
        bw = std::move(sw);
        bd = std::move(sd);
        bm = std::move(smet);
        bj = sj;
        improved = !r.capped;
      }
    }
    std::map<std::string, std::pair<double, double>> proposal;
    for (size_t i = 0; i < nq; ++i) {
      if (std::abs(bw[i] - w[i]) > 1e-9 || std::abs(bd[i] - dl[i]) > 1e-9) proposal[q[i].id] = {bw[i], bd[i]};
    }
    if (!proposal.empty()) {
      // PLAN §4.3: a change must lower J by >= 2 % (or 0.5 missed-SL calls per hour) and be the
      // previous tick's proposal too.
      bool keep = r.j_current - bj >= std::max(o.hysteresis_rel * r.j_current, o.hysteresis_abs);
      if (!keep) {
        r.held = "gain below hysteresis";
      } else {
        for (const auto& [id, wd] : proposal) {
          auto it = mem->proposed.find(id);
          if (it == mem->proposed.end() || std::abs(it->second.first - wd.first) > 1e-9 ||
              std::abs(it->second.second - wd.second) > 1e-9) {
            keep = false;
          }
        }
        if (!keep) r.held = "not confirmed";
      }
      if (keep) {
        r.w_planned = bw;
        r.d_planned = bd;
        r.planned = std::move(bm);
        r.j_planned = bj;
      }
    }
    mem->proposed = std::move(proposal);
  }
  r.q_quality.resize(nq);
  r.quality = Quality::kExact;
  for (size_t i = 0; i < nq; ++i) {
    r.q_quality[i] = model::worst(label(r.current[i]), label(r.planned[i]));
    r.quality = model::worst(r.quality, r.q_quality[i]);
  }
  if (recommend && !over()) {
    // Lever (c): the forecast over the horizon, the levers in force.
    std::vector<model::AQueue> hq;
    for (const auto& x : q) hq.push_back(aqueue(x, true));
    model::AgentModel hm(hq, ag, mo, st);
    const model::AgentResult& hb = hm.base();
    std::vector<int> short_q;
    for (size_t i = 0; i < nq; ++i) {
      if (hq[i].lambda > 0 && hb.q[i].m.ok) short_q.push_back(static_cast<int>(i));
    }
    std::stable_sort(short_q.begin(), short_q.end(), [&](int x, int y) {
      return hb.q[x].m.sl - q[x].sl_target < hb.q[y].m.sl - q[y].sl_target;
    });
    int looked = 0;
    for (int i : short_q) {
      if (looked >= o.max_recommend_queues || over()) break;
      const model::QueueMetrics& now_m = hb.q[i].m;
      const LeverQueue& x = q[i];
      const bool alone = hb.q[i].pool >= 0 && hb.pools[hb.q[i].pool].queues.size() == 1;
      if (now_m.sl < x.sl_target) {
        ++looked;
        int lo = 0, hi = o.max_extra_agents;
        model::QueueMetrics at_hi = with_agents(hq, ag, hb, i, hi, mo, st);
        Recommendation rec;
        rec.kind = "ready_agents";
        rec.queue = x.id;
        rec.before = now_m;
        rec.current = static_cast<int>(std::lround(hb.q[i].R));
        if (!at_hi.ok || at_hi.sl < x.sl_target) {
          rec.delta = hi;
          rec.after = at_hi;
          rec.reason = "short of sl_target even with the most agents looked at";
        } else {
          model::QueueMetrics best = at_hi;
          while (hi - lo > 1) {
            const int mid = (lo + hi) / 2;
            const model::QueueMetrics mm = with_agents(hq, ag, hb, i, mid, mo, st);
            if (mm.ok && mm.sl >= x.sl_target) {
              hi = mid;
              best = mm;
            } else {
              lo = mid;
            }
          }
          rec.delta = hi;
          rec.after = best;
          rec.reason = "predicted SL below sl_target at the forecast";
        }
        rec.quality = model::worst(now_m.quality, rec.after.ok ? rec.after.quality : Quality::kLong);
        r.recs.push_back(std::move(rec));
      } else if (alone && now_m.sl >= x.sl_target + 0.05 && hb.q[i].R >= 2) {
        // A queue served only by its own agents with room: how many fewer still meet the target.
        ++looked;
        int lo = 0, hi = static_cast<int>(hb.q[i].R) - 1;
        model::QueueMetrics best = now_m;
        while (hi > lo) {
          const int mid = (lo + hi + 1) / 2;
          const model::QueueMetrics mm = with_agents(hq, ag, hb, i, -mid, mo, st);
          if (mm.ok && mm.sl >= x.sl_target) {
            lo = mid;
            best = mm;
          } else {
            hi = mid - 1;
          }
        }
        if (lo > 0) {
          Recommendation rec;
          rec.kind = "ready_agents";
          rec.queue = x.id;
          rec.delta = -lo;
          rec.current = static_cast<int>(std::lround(hb.q[i].R));
          rec.before = now_m;
          rec.after = best;
          rec.quality = model::worst(now_m.quality, best.quality);
          rec.reason = "predicted SL above sl_target with fewer agents";
          r.recs.push_back(std::move(rec));
        }
      }
    }
    // Lever (b) advice: for a queue that shares its agents, the agents it needs to meet its target on
    // its own load (min_agents), where that differs from the configured value.
    for (size_t i = 0; i < nq && !over(); ++i) {
      const int p = hb.q[i].pool;
      if (p < 0 || hb.pools[p].queues.size() < 2 || hq[i].lambda <= 0) continue;
      int need = 0;
      model::QueueMetrics mm;
      const int cap = static_cast<int>(std::lround(hb.q[i].R));
      model::Options oq = mo.q;
      oq.sla_s = q[i].sla_s > 0 ? q[i].sla_s : oq.sla_s;
      for (int k = 1; k <= cap; ++k) {
        mm = model::stationary_metrics(model::QueueParams{hq[i].lambda, hq[i].mu, hq[i].theta, static_cast<double>(k), -1, 0},
                                       oq, st);
        if (mm.ok && mm.sl >= q[i].sl_target) {
          need = k;
          break;
        }
      }
      if (need == 0 || need == q[i].min_agents) continue;
      Recommendation rec;
      rec.kind = "min_agents";
      rec.queue = q[i].id;
      rec.value = need;
      rec.current = q[i].min_agents;
      rec.before = hb.q[i].m;
      rec.after = mm;
      rec.quality = model::worst(label(hb.q[i]), Quality::kApprox);  // the queue alone: a bound, not the pool's
      rec.reason = "agents its own load needs to meet sl_target";
      r.recs.push_back(std::move(rec));
    }
  }
  r.ok = true;
  return r;
}

}  // namespace pcr::opt
