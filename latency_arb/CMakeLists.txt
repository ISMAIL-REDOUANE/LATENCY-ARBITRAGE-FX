cmake_minimum_required(VERSION 3.20)
project(lead_lag_arb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(CMAKE_BUILD_TYPE MATCHES "Release|RelWithDebInfo")
  add_compile_options(-O3 -march=native -DNDEBUG -flto)
  add_link_options(-flto)
endif()
add_compile_options(-Wall -Wextra -pedantic)

find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)

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

add_library(llm_core
  src/config.cpp
  src/zmq_pub.cpp
  src/aggregator.cpp
  src/strategy.cpp
  src/telemetry.cpp
)
target_include_directories(llm_core PUBLIC include ${ZMQ_INCLUDE_DIR})
target_include_directories(llm_core PUBLIC ${NLOHMANN_JSON_INCLUDE_DIR})
target_compile_definitions(llm_core PUBLIC BOOST_BEAST_USE_STD_STRING_VIEW)
target_link_libraries(llm_core PUBLIC
  Threads::Threads
  ${ZMQ_LIBRARY}
  ${SODIUM_LIBRARY}
)
if(OpenSSL_FOUND)
  target_link_libraries(llm_core PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endif()

add_executable(lead_lag
  src/main.cpp
  src/binance_ws.cpp
  src/deribit_ws.cpp
  src/broker_sub.cpp
  src/execution.cpp
  src/fix_client.cpp
)
target_link_libraries(lead_lag PRIVATE llm_core Boost::system)

# Pure C++20 native MT5 execution bridge (no Python).
add_executable(mt5_bridge
  src/mt5_bridge_main.cpp
  src/mt5_bridge.cpp
)
target_link_libraries(mt5_bridge PRIVATE llm_core)
if(WIN32)
  target_link_libraries(mt5_bridge PRIVATE ws2_32)
endif()

include(GNUInstallDirs)
install(TARGETS lead_lag mt5_bridge RUNTIME DESTINATION bin)

enable_testing()
add_executable(llm_ring_test tests/ring_test.cpp)
target_link_libraries(llm_ring_test PRIVATE llm_core)
add_test(NAME llm_ring_test COMMAND llm_ring_test)