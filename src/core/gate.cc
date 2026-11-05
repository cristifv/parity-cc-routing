#include "src/core/gate.h"

namespace pcr::gate {

namespace {
thread_local Batch* tl_batch = nullptr;
}  // namespace

Batch* current() { return tl_batch; }

void set_current(Batch* b) { tl_batch = b; }

void after_commit(std::function<void()> fn) {
  if (Batch* b = tl_batch) {
    b->effects.push_back(std::move(fn));
  } else {
    fn();
  }
}

}  // namespace pcr::gate
