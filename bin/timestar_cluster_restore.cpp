#include "cluster/features/cluster_restore_seeder.hpp"

#include <filesystem>
#include <iostream>
#include <string_view>
#include <vector>

namespace {

int usage(const char* program) {
    std::cerr << "usage: " << program << " finalize --output <release.tsr1> <node-marker>...\n";
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5 || std::string_view(argv[1]) != "finalize" || std::string_view(argv[2]) != "--output" ||
        std::string_view(argv[3]).empty())
        return usage(argv[0]);

    std::vector<std::filesystem::path> markers;
    markers.reserve(static_cast<size_t>(argc - 4));
    for (int i = 4; i < argc; ++i) {
        if (std::string_view(argv[i]).empty())
            return usage(argv[0]);
        markers.emplace_back(argv[i]);
    }
    try {
        timestar::features::ClusterRestoreSeeder::finalizeRelease(markers, argv[3]);
        std::cout << "cluster restore release finalized for " << markers.size() << " participant(s): " << argv[3]
                  << '\n';
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "cluster restore finalization failed: " << e.what() << '\n';
        return 1;
    }
}
