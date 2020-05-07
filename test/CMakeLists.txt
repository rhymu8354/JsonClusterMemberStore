# CMakeLists.txt for JsonClusterMemberStoreTests

cmake_minimum_required(VERSION 3.8)
set(This JsonClusterMemberStoreTests)

set(Sources
    src/JsonDatabaseTests.cpp
)

add_executable(${This} ${Sources})
set_target_properties(${This} PROPERTIES
    FOLDER Tests
)

target_include_directories(${This} PRIVATE ..)

target_link_libraries(${This} PUBLIC
    gtest_main
    Json
    JsonClusterMemberStore
    SystemAbstractions
)

add_test(
    NAME ${This}
    COMMAND ${This}
)
