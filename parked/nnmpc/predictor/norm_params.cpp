#include <norm_params.h>

#include <fstream>
#include <iostream>

float norm_params::norm_arr(double v) const {
  return static_cast<float>((v - arr_lo) / std::max(1e-6f, arr_hi - arr_lo));
}

float norm_params::norm_aht(double v) const {
  return static_cast<float>((v - aht_lo) / std::max(1e-6f, aht_hi - aht_lo));
}

double norm_params::denorm_arr(float n) const {
  return n * (arr_hi - arr_lo) * arr_lo;
}

double norm_params::denorm_aht(float n) const {
  return n * (aht_hi - aht_lo) * aht_lo;
}

void norm_params::save(const std::string& path) const {
  std::ofstream f(path, std::ios::binary);
  f.write(reinterpret_cast<const char*>(this), sizeof(*this));
}

bool norm_params::load(const std::string& path) {
  std::ifstream f(path, std::ios::binary);

  if (!f) return false;

  f.read(reinterpret_cast<char*>(this), sizeof(*this));
  return f.good();
}
