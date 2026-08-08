cmake_minimum_required(VERSION 3.20)
project(lead_lag_arb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# ---------------------------------------------------------------------------
# Production flags (generated binaries only — the test suite intentionally does
# NOT get -ffast-math so its rounding/edge assertions are FP-correct).
# ---------------------------------------------------------------------------
if(CMAKE_BUILD_TYPE MATCHES "Release|RelWithDebInfo")
  add_link_options(-flto)
endif()
add_compile_options(-Wall -Wextra -pedantic)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)

# protobuf + protoc for the cTrader Open API wire schemas (proto2).
find_package(Protobuf QUIET)
if(Protobuf_FOUND)
  message(STATUS "protobuf: using libprotobuf ${Protobuf_VERSION}")
else()
  message(FATAL_ERROR "libprotobuf-dev / protobuf-compiler not found. Run scripts/deploy_oanda.sh or scripts/install_deps.sh first.")
endif()

# ZeroMQ
find_path(ZMQ_INCLUDE_DIR zmq.h)
find_library(ZMQ_LIBRARY NAMES zmq REQUIRED)
if(NOT ZMQ_INCLUDE_DIR OR NOT ZMQ_LIBRARY)
  message(FATAL_ERROR "libzmq not found. Run scripts/install_deps.sh first.")
endif()

# sodium (optional, for ZMQ CURVE)
find_library(SODIUM_LIBRARY NAMES sodium)

# nlohmann/json
find_path(NLOHMANN_JSON_INCLUDE_DIR nlohmann/json.hpp)
if(NOT NLOHMANN_JSON_INCLUDE_DIR)
  message(FATAL_ERROR "nlohmann-json not found. Run scripts/install_deps.sh first.")
endif()

# Boost.Beast needs Boost headers (already found via Boost::system header usage).
find_package(Boost REQUIRED)

# ---------------------------------------------------------------------------
# cTrader Open API protobuf schemas (vendored official proto2 definitions).
# protoc generates *.pb.h / *.pb.cc into ${CMAKE_BINARY_DIR}/openapi-gen.
# ---------------------------------------------------------------------------
set(OPENAPI_PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/openapi")
set(OPENAPI_GEN_DIR "${CMAKE_BINARY_DIR}/openapi-gen")
file(GLOB OPENAPI_PROTO_FILES "${OPENAPI_PROTO_DIR}/*.proto")

# Explicit protoc invocation (version-independent; protobuf::protoc imported by
# find_package(Protobuf) — the protobuf_generate(<var>) macro's variable
# handling changed across CMake versions).
set(OPENAPI_GEN_SRC)
set(OPENAPI_GEN_HDR)
foreach(_proto ${OPENAPI_PROTO_FILES})
  get_filename_component(_base "${_proto}" NAME_WE)
  set(_gen_cc "${OPENAPI_GEN_DIR}/${_base}.pb.cc")
  set(_gen_h  "${OPENAPI_GEN_DIR}/${_base}.pb.h")
  list(APPEND OPENAPI_GEN_SRC "${_gen_cc}")
  list(APPEND OPENAPI_GEN_HDR "${_gen_h}")
  add_custom_command(
    OUTPUT "${_gen_cc}" "${_gen_h}"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${OPENAPI_GEN_DIR}"
    COMMAND protobuf::protoc
      --proto_path=${OPENAPI_PROTO_DIR}
      --cpp_out=${OPENAPI_GEN_DIR}
      ${_proto}
    DEPENDS "${_proto}"
    COMMENT "protoc ${_base}.proto"
    VERBATIM
  )
endforeach()
if(NOT OPENAPI_GEN_SRC)
  message(FATAL_ERROR "no .proto files found in ${OPENAPI_PROTO_DIR}")
endif()
set_source_files_properties(${OPENAPI_GEN_SRC} PROPERTIES GENERATED TRUE)

add_library(llm_core
  src/config.cpp
  src/zmq_pub.cpp
  src/aggregator.cpp
  src/strategy.cpp
  src/telemetry.cpp
  src/wire.cpp
  src/oanda_executor.cpp
  src/fix_executor.cpp
  src/ctrader_executor.cpp
  ${OPENAPI_GEN_SRC}
)
target_include_directories(llm_core PUBLIC include ${ZMQ_INCLUDE_DIR})
target_include_directories(llm_core PUBLIC ${NLOHMANN_JSON_INCLUDE_DIR})
target_include_directories(llm_core PUBLIC ${OPENAPI_GEN_DIR})
target_compile_definitions(llm_core PUBLIC BOOST_BEAST_USE_STD_STRING_VIEW)
target_link_libraries(llm_core PUBLIC
  Threads::Threads
  ${ZMQ_LIBRARY}
  ${SODIUM_LIBRARY}
  Boost::system
  protobuf::libprotobuf
)
if(OpenSSL_FOUND)
  target_link_libraries(llm_core PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()

# Production binaries get the full tuning set; the test target below does not
# (keeps floating-point behavior deterministic for its assertions).
if(CMAKE_BUILD_TYPE MATCHES "Release|RelWithDebInfo")
  target_compile_options(llm_core PUBLIC
    -O3 -march=native -DNDEBUG -flto -funroll-loops -ffast-math
  )
endif()

add_executable(lead_lag
  src/main.cpp
  src/binance_ws.cpp
  src/bybit_ws.cpp
  src/deribit_ws.cpp
  src/broker_sub.cpp
  src/execution.cpp
  src/fix_client.cpp
)
target_link_libraries(lead_lag PRIVATE llm_core Boost::system)

# Standalone OANDA v2 order probe (test/diagnostics): sends one market order
# using OANDA_TOKEN/ACCOUNT_ID/HOST/INSTRUMENT env vars, then exits. Useful to
# smoke-test the executor without the full engine.
add_executable(oanda_probe src/bin/oanda_probe_main.cpp)
target_include_directories(oanda_probe PRIVATE include)
target_link_libraries(oanda_probe PRIVATE llm_core Boost::system)

# Standalone cTrader Open API order probe (diagnostics): one market order
# against IC Markets demo using CTRADER_CLIENT_ID/SECRET/ACCOUNT/TOKEN env vars.
add_executable(ctrader_probe src/bin/ctrader_probe_main.cpp)
target_include_directories(ctrader_probe PRIVATE include)
target_link_libraries(ctrader_probe PRIVATE llm_core Boost::system)

# Standalone IC Markets cTrader FIX 4.4 probe (diagnostics): one NewOrder
# Single (35=D) over the FIX_HOST/FIX_PORT/FIX_PASSWORD/FIX_ACCOUNT_ID env
# vars, verifying the TLS session + ExecutionReport round-trip.
add_executable(fix_probe src/bin/fix_probe_main.cpp)
target_include_directories(fix_probe PRIVATE include)
target_link_libraries(fix_probe PRIVATE llm_core Boost::system)

# Pure C++20 native MT5 execution bridge (no Python).
add_executable(mt5_bridge
  src/mt5_bridge_main.cpp
  src/mt5_bridge.cpp
)
target_link_libraries(mt5_bridge PRIVATE llm_core)
if(WIN32)
  target_link_libraries(mt5_bridge PRIVATE ws2_32)
endif()

# ---------------------------------------------------------------------------
# Backtest / replay subsystem — dependency-free (pure config/strategy/
# telemetry + backtest sources; NO libzmq/Boost/OpenSSL), so both the tick
# replay binary and its unit tests build on a fresh box.
# ---------------------------------------------------------------------------
add_library(llm_backtest STATIC
  src/backtest/tick_binary.cpp
  src/backtest/execution_simulator.cpp
  src/backtest/backtest_engine.cpp
  src/config.cpp
  src/strategy.cpp
  src/telemetry.cpp
)
target_include_directories(llm_backtest PUBLIC include)
target_compile_definitions(llm_backtest PUBLIC BOOST_BEAST_USE_STD_STRING_VIEW)
target_link_libraries(llm_backtest PUBLIC Threads::Threads)

# Tick replay / backtest CLI.
add_executable(tick_replay src/bin/tick_replay_main.cpp)
target_link_libraries(tick_replay PRIVATE llm_backtest)
if(CMAKE_BUILD_TYPE MATCHES "Release|RelWithDebInfo")
  target_compile_options(tick_replay PRIVATE
    -O3 -march=native -DNDEBUG -flto -funroll-loops -ffast-math
  )
endif()

# ---------------------------------------------------------------------------
# Unit + benchmark suite — dependency-free (pure config/strategy/aggregator/
# telemetry/wire; NO libzmq/Boost/OpenSSL), so `ctest` passes even on a fresh
# box that has not installed the runtime deps.
# ---------------------------------------------------------------------------
enable_testing()
add_executable(latency_tests
  tests/test_main.cpp
  src/config.cpp
  src/strategy.cpp
  src/aggregator.cpp
  src/telemetry.cpp
  src/wire.cpp
)
target_include_directories(latency_tests PRIVATE include)
# alloc-counter operator new/delete override pairs malloc/free correctly, but
# GCC's -Wmismatched-new-delete is a false positive on that idiom.
target_compile_options(latency_tests PRIVATE -O2 -g -Wno-mismatched-new-delete)
target_link_libraries(latency_tests PRIVATE Threads::Threads)
add_test(NAME latency_tests COMMAND latency_tests)

# Backtest subsystem unit tests (execution simulator fill mechanics).
add_executable(backtest_tests tests/test_backtest.cpp)
target_include_directories(backtest_tests PRIVATE include)
target_compile_options(backtest_tests PRIVATE -O2 -g)
target_link_libraries(backtest_tests PRIVATE llm_backtest Threads::Threads)
add_test(NAME backtest_tests COMMAND backtest_tests)

include(GNUInstallDirs)
install(TARGETS lead_lag mt5_bridge RUNTIME DESTINATION bin)
install(TARGETS tick_replay RUNTIME DESTINATION bin)
install(TARGETS oanda_probe RUNTIME DESTINATION bin)
install(TARGETS ctrader_probe RUNTIME DESTINATION bin)
install(TARGETS fix_probe RUNTIME DESTINATION bin)