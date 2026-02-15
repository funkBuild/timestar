#ifndef FUNCTION_UTILS_H_INCLUDED
#define FUNCTION_UTILS_H_INCLUDED

#include <vector>

namespace tsdb::functions {

double calculateMean(const std::vector<double>& values);
std::vector<double> simpleMovingAverage(const std::vector<double>& values, int window);

} // namespace tsdb::functions

#endif // FUNCTION_UTILS_H_INCLUDED