CMAKE_MINIMUM_REQUIRED(VERSION 2.6)
PROJECT(SCFDM_part CXX)

find_package(LLVM REQUIRED CONFIG)
find_package(MPI REQUIRED)

set(SOURCE_FILES main.cpp)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x -O2 -Wall -Wextra")
FILE(GLOB_RECURSE DataSrcs data/*.cpp)
FILE(GLOB_RECURSE AlgSrcs algorithms/*.cpp)
FILE(GLOB_RECURSE UtilSrcs util/*.cpp)
include_directories(SYSTEM ${MPI_INCLUDE_PATH})
include_directories(${LLVM_INCLUDE_DIRS} )
add_definitions(${LLVM_DEFINITIONS})

ADD_EXECUTABLE(SCFDM_part main.cpp ${DataSrcs} ${AlgSrcs} ${UtilSrcs})
# openMP configuration
FIND_PACKAGE(OpenMP REQUIRED)
if (OPENMP_FOUND)
    message("OPENMP FOUND")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif ()
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
llvm_map_components_to_libnames(llvm_libs support core irreader)
#execute_process(COMMAND llvm-config --libs all OUTPUT_VARIABLE llvm_libraries)


target_link_libraries(SCFDM_part PRIVATE MPI::MPI_CXX)
