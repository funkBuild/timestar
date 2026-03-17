#include "function_pipeline_executor.hpp"

#include "../core/engine.hpp"
#include "../utils/logger.hpp"

#include <seastar/core/coroutine.hh>

namespace timestar::functions {

FunctionPipelineExecutor::FunctionPipelineExecutor(seastar::sharded<Engine>* engine) : engine_(engine) {}

seastar::future<void> FunctionPipelineExecutor::executeFunction(
    const std::string& functionName,
    const std::map<std::string, std::variant<int64_t, double, std::string>>& parameters, std::vector<double>& data) {
    if (functionName == "sma") {
        int64_t window = 3;  // default window size
        auto it = parameters.find("window");
        if (it != parameters.end() && std::holds_alternative<int64_t>(it->second)) {
            window = std::get<int64_t>(it->second);
        }
        if (window > 1000)
            window = 1000;  // consistent with SMAFunction validation
        if (window > 0 && data.size() >= static_cast<size_t>(window)) {
            std::vector<double> result(data.size());
            double sum = 0.0;
            size_t w = static_cast<size_t>(window);
            for (size_t i = 0; i < w; ++i)
                sum += data[i];
            result[w - 1] = sum / static_cast<double>(w);
            for (size_t i = w; i < data.size(); ++i) {
                sum += data[i] - data[i - w];
                result[i] = sum / static_cast<double>(w);
            }
            // Fill leading values with first valid average
            for (size_t i = 0; i + 1 < w; ++i)
                result[i] = result[w - 1];
            data = std::move(result);
        }
        co_return;
    } else if (functionName == "ema") {
        int64_t window = 3;
        auto it = parameters.find("window");
        if (it != parameters.end() && std::holds_alternative<int64_t>(it->second)) {
            window = std::get<int64_t>(it->second);
        }
        if (window > 0 && !data.empty()) {
            double alpha = 2.0 / (static_cast<double>(window) + 1.0);
            double ema = data[0];
            data[0] = ema;
            for (size_t i = 1; i < data.size(); ++i) {
                ema = alpha * data[i] + (1.0 - alpha) * ema;
                data[i] = ema;
            }
        }
        co_return;
    } else if (functionName == "add") {
        auto it = parameters.find("value");
        if (it != parameters.end() && std::holds_alternative<double>(it->second)) {
            double addValue = std::get<double>(it->second);
            for (auto& val : data) {
                val += addValue;
            }
        }
        co_return;
    } else if (functionName == "multiply") {
        auto it = parameters.find("factor");
        if (it != parameters.end() && std::holds_alternative<double>(it->second)) {
            double factor = std::get<double>(it->second);
            for (auto& val : data) {
                val *= factor;
            }
        }
        co_return;
    }

    timestar::query_log.warn("Unknown pipeline function: {}", functionName);
    co_return;
}

}  // namespace timestar::functions