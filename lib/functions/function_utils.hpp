#ifndef __FUNCTION_UTILS_H_INCLUDED__
#define __FUNCTION_UTILS_H_INCLUDED__

#include <vector>

namespace tsdb::functions {

double calculateMean(const std::vector<double>& values);
std::vector<double> simpleMovingAverage(const std::vector<double>& values, int window);

} // namespace tsdb::functions

#endif // __FUNCTION_UTILS_H_INCLUDED__