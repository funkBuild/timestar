#!/bin/bash

# Fix engine.cpp debug statements - wrap them with #if TSDB_LOG_QUERY_PATH
sed -i '59,66s/^/#if TSDB_LOG_INSERT_PATH\n/; 66a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '141s/^/#if TSDB_LOG_QUERY_PATH\n/; 143a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '148s/^  /#if TSDB_LOG_QUERY_PATH\n  /; 148a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '154s/^  /#if TSDB_LOG_QUERY_PATH\n  /; 154a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '159s/^  /#if TSDB_LOG_QUERY_PATH\n  /; 159a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '164,166s/^  /#if TSDB_LOG_QUERY_PATH\n  /; 166a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '174,175s/^  /#if TSDB_LOG_QUERY_PATH\n  /; 175a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '179s/^  /#if TSDB_LOG_QUERY_PATH\n  /; 179a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '183s/^  /#if TSDB_LOG_QUERY_PATH\n  /; 183a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '192,193s/^  /#if TSDB_LOG_QUERY_PATH\n  /; 193a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '206,212s/^      /#if TSDB_LOG_QUERY_PATH\n      /; 212a#endif' lib/core/engine.cpp 2>/dev/null || true

# Fix DELETE_DEBUG statements - wrap with macro
sed -i '327,333s/^/#if TSDB_LOG_INSERT_PATH\n/; 333a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '346,349s/^/#if TSDB_LOG_INSERT_PATH\n/; 349a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '383,384s/^/#if TSDB_LOG_INSERT_PATH\n/; 384a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '387,392s/^/#if TSDB_LOG_INSERT_PATH\n/; 392a#endif' lib/core/engine.cpp 2>/dev/null || true
sed -i '404,405s/^/#if TSDB_LOG_INSERT_PATH\n/; 405a#endif' lib/core/engine.cpp 2>/dev/null || true

# Fix query_runner.cpp QUERY_DEBUG statements
sed -i '96s/^/#if TSDB_LOG_QUERY_PATH\n/; 97a#endif' lib/query/query_runner.cpp 2>/dev/null || true
sed -i '104s/^/#if TSDB_LOG_QUERY_PATH\n/; 105a#endif' lib/query/query_runner.cpp 2>/dev/null || true
sed -i '110s/^/#if TSDB_LOG_QUERY_PATH\n/; 110a#endif' lib/query/query_runner.cpp 2>/dev/null || true

# Fix memory_store.hpp MEMSTORE_DEBUG statements
sed -i '88s/^/#if TSDB_LOG_QUERY_PATH\n/; 88a#endif' lib/storage/memory_store.hpp 2>/dev/null || true
sed -i '92s/^/#if TSDB_LOG_QUERY_PATH\n/; 92a#endif' lib/storage/memory_store.hpp 2>/dev/null || true
sed -i '96,97s/^/#if TSDB_LOG_QUERY_PATH\n/; 97a#endif' lib/storage/memory_store.hpp 2>/dev/null || true
sed -i '102s/^/#if TSDB_LOG_QUERY_PATH\n/; 102a#endif' lib/storage/memory_store.hpp 2>/dev/null || true
sed -i '106,109s/^/#if TSDB_LOG_QUERY_PATH\n/; 109a#endif' lib/storage/memory_store.hpp 2>/dev/null || true
sed -i '127,128s/^/#if TSDB_LOG_QUERY_PATH\n/; 128a#endif' lib/storage/memory_store.hpp 2>/dev/null || true
sed -i '139,140s/^/#if TSDB_LOG_QUERY_PATH\n/; 140a#endif' lib/storage/memory_store.hpp 2>/dev/null || true

echo "Debug logging fix completed"