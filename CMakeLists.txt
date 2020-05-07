# CMakeLists.txt for JsonClusterMemberStore
#
# © 2018 by Richard Walters

cmake_minimum_required(VERSION 3.8)
set(This JsonClusterMemberStore)

set(Headers
    include/JsonClusterMemberStore/JsonDatabase.hpp
)

set(Sources
    src/JsonDatabase.cpp
)

add_library(${This} STATIC ${Sources} ${Headers})
set_target_properties(${This} PROPERTIES
    FOLDER Libraries
)

target_include_directories(${This} PUBLIC include)

target_link_libraries(${This} PUBLIC
    ClusterMemberStore
    Json
    SystemAbstractions
)

add_subdirectory(test)
