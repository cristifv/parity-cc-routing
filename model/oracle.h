// The exact oracle (PLAN §3.2, §3.3; tests only, never on the optimization path): the true
// multi-queue chain of an N-design, solved with a sparse generator. Queue 1 has its own group
// (n1 agents); group 2 (n2 agents) works queue 2 first and takes queue 1's calls only when no
// queue-2 caller waits (strict primary-first, non-idling, FCFS per queue). State: (busy of group 1,
// group 2 busy on queue 1, group 2 busy on queue 2, waiting in 1, waiting in 2). Steady state by
// SparseLU; service level and the virtual wait by tagged-customer chains on (position, the other
// queue, the group-2 split); abandonment by Little's law.
#pragma once

#include <string>

namespace pcr::model {

struct NDesign {
  int n1 = 5, n2 = 5;
  double lambda1 = 0, lambda2 = 0, mu1 = 0, mu2 = 0, theta1 = 0, theta2 = 0;
  int k1 = 40, k2 = 40;  // waiting truncation per queue
  double sla_s = 20, short_abandon_s = 5;
};

struct NDesignResult {
  bool ok = false;
  std::string why;
  size_t states = 0;
  double sl1 = 0, sl2 = 0, abandon1 = 0, abandon2 = 0, ewt1 = 0, ewt2 = 0;
  double lent_busy = 0;   // E[group 2 busy on queue 1]
  double idle2 = 0;       // E[group 2 idle]
  double boundary_mass = 0;  // stationary mass on the truncation edges
};

NDesignResult solve_ndesign(const NDesign& d);

}  // namespace pcr::model
