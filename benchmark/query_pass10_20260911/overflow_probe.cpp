#include <cstdint>
#include <cstdio>
int main() {
 volatile uint64_t base[] = {0, UINT64_MAX / 2, UINT64_MAX};
 size_t i = 0;
        int64_t x = (static_cast<int64_t>(base[i + 2]) - static_cast<int64_t>(base[i + 1])) -
                    (static_cast<int64_t>(base[i + 1]) - static_cast<int64_t>(base[i]));
 std::printf("%lld\n", (long long)x);
}
