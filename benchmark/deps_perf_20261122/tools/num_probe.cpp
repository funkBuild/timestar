#include <glaze/glaze.hpp>
#include <cstdio>
#include <string>
int main() {
    for (const char* tok : {"1.0","100.0","12.50","0.0","-0.0","1e5","1E5","1e+5","1.5e1","1e-1","5e-1","1.0e2","2.5E+10","0e0","1e0","-1e2","1e19","1e20","1.5","123456789012345680000.0","9223372036854775807","9223372036854775808","18446744073709551615","18446744073709551616","-9223372036854775808","-9223372036854775809","-0","-1","0","1e18","9e18","1.0e0","10e-1","15e-1","1e400","4.9e-324","1e-400","0.1e1","1.25e2","1.255e2"}) {
        glz::generic_u64 doc{};
        std::string body = std::string("[") + tok + "]";
        auto ec = glz::read_json(doc, body);
        if (ec) { printf("%-28s -> PARSE ERROR\n", tok); continue; }
        auto& v = doc.get<glz::generic_u64::array_t>()[0];
        const char* kind = v.holds<uint64_t>() ? "uint64" : v.holds<int64_t>() ? "int64" : v.holds<double>() ? "double" : "other";
        if (v.holds<double>()) printf("%-28s -> %s %a\n", tok, kind, v.get<double>());
        else if (v.holds<uint64_t>()) printf("%-28s -> %s %llu  as<int64>=%lld\n", tok, kind, (unsigned long long)v.get<uint64_t>(), (long long)v.as<int64_t>());
        else if (v.holds<int64_t>()) printf("%-28s -> %s %lld\n", tok, kind, (long long)v.get<int64_t>());
        else printf("%-28s -> %s\n", tok, kind);
    }
}
