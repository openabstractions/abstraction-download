cmake_minimum_required(VERSION 3.16)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    project(abstraction_download LANGUAGES CXX VERSION 0.1.0)
    set(ABSTRACTION_DOWNLOAD_STANDALONE ON)
else()
    set(ABSTRACTION_DOWNLOAD_STANDALONE OFF)
endif()

include(GNUInstallDirs)
find_package(Threads REQUIRED)

option(ABSTRACTION_BUILD_TESTS "Build the abstraction's own tests"
    ${ABSTRACTION_DOWNLOAD_STANDALONE})

# Three ways a sibling layer can already be here, and no fourth: a superproject
# added it, an install of it is on CMAKE_PREFIX_PATH, or its checkout is beside
# ours. Nothing is fetched -- a configure step that reaches the network is the
# thing this build refuses to hand downstream.
macro(abstraction_need name)
    if(NOT TARGET abstraction::${name})
        find_package(abstraction_${name} CONFIG QUIET)
        if(TARGET abstraction::${name})
            set_property(GLOBAL APPEND PROPERTY
                ABSTRACTION_PACKAGE_DEPS abstraction_${name})
        endif()
    endif()
    if(NOT TARGET abstraction::${name})
        foreach(d ../../${name}/cpp ../../abstraction-${name}/cpp)
            if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${d}/CMakeLists.txt)
                add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/${d} abstraction_${name}-build)
                get_property(_have GLOBAL PROPERTY ABSTRACTION_SOURCE_TARGETS)
                if(NOT "abstraction_${name}" IN_LIST _have)
                    set_property(GLOBAL APPEND PROPERTY
                        ABSTRACTION_SOURCE_TARGETS abstraction_${name})
                    set_property(GLOBAL APPEND PROPERTY
                        ABSTRACTION_SOURCE_INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/${d}/include/)
                endif()
                break()
            endif()
        endforeach()
    endif()
    if(NOT TARGET abstraction::${name})
        message(FATAL_ERROR "abstraction_${name} not found. Install it and put its "
            "prefix on CMAKE_PREFIX_PATH, or clone abstraction-${name} beside this "
            "repository. Nothing is downloaded for you.")
    endif()
endmacro()

abstraction_need(job)
# IPC has its own package even when built from the identity sibling source.
if(NOT TARGET abstraction::ipc)
    find_package(abstraction_ipc CONFIG QUIET)
endif()
if(NOT TARGET abstraction::ipc AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../../abstraction-identity/cpp/CMakeLists.txt")
    add_subdirectory(../../abstraction-identity/cpp abstraction_ipc-build)
endif()
if(NOT TARGET abstraction::ipc)
    message(FATAL_ERROR "abstraction_ipc not found: install it or provide the abstraction-identity sibling")
endif()
set_property(GLOBAL APPEND PROPERTY ABSTRACTION_PACKAGE_DEPS abstraction_ipc)

# What a download job is, and where its bytes may NOT land: header-only and
# dependency-free, the string rules a reader needs in order to say the same
# "no" about a record as the implementation that would run it. Kept separate
# from the runner below so an adopter who only reads records takes neither a
# parser nor a build system.
add_library(abstraction_download INTERFACE)
target_include_directories(abstraction_download INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
# Namespaced names on purpose: abstraction::job is an ALIAS when the layer came
# from source and an IMPORTED target when it came from an install, and only the
# namespaced name is spelled the same in both cases.
target_link_libraries(abstraction_download INTERFACE abstraction::job_layout)
target_compile_features(abstraction_download INTERFACE cxx_std_17)
set_target_properties(abstraction_download PROPERTIES EXPORT_NAME download)
add_library(abstraction::download ALIAS abstraction_download)

# Its own target, one that abstraction_job neither links nor is linked by, so
# the forbidden dependency of job on discovery cannot form by accident.
add_library(abstraction_discovery STATIC src/discovery_client.cpp)
target_include_directories(abstraction_discovery PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(abstraction_discovery PUBLIC abstraction::json abstraction::ipc)
target_compile_features(abstraction_discovery PUBLIC cxx_std_17)
set_target_properties(abstraction_discovery PROPERTIES EXPORT_NAME discovery)
add_library(abstraction::discovery ALIAS abstraction_discovery)

# WinHTTP on Windows: a platform facility, already installed, already trusted,
# and its certificate trust is the machine's own store rather than a root
# bundle frozen into our binary. macOS has NSURLSession, which is the same
# bargain and is not written yet. Linux furnishes neither, so a build there
# registers no https fetcher and says so -- see fetcher.h, https_available().
add_library(abstraction_download_runner STATIC
    src/fetchers.cpp
    src/runner.cpp
    src/sha256.cpp
    src/spec.cpp
    src/wanted.cpp
)
target_link_libraries(abstraction_download_runner PUBLIC
    abstraction::job abstraction::download)
target_compile_features(abstraction_download_runner PUBLIC cxx_std_17)
# Names the one thing a consumer of abstraction::download alone cannot do:
# define the functions runner.h only declares. Guarded on in runner.h itself,
# so linking the reader without the runner fails to compile, naming the header,
# instead of failing to link with an unresolved symbol.
target_compile_definitions(abstraction_download_runner
    PUBLIC ABSTRACTION_DOWNLOAD_RUNNER_LINKED)
if(WIN32)
    target_link_libraries(abstraction_download_runner PRIVATE winhttp)
endif()
set_target_properties(abstraction_download_runner PROPERTIES EXPORT_NAME download_runner)
add_library(abstraction::download_runner ALIAS abstraction_download_runner)

if(ABSTRACTION_BUILD_TESTS)
    enable_testing()

    # This one stands up a real listener -- a named pipe on Windows, a unix
    # socket elsewhere -- so it needs threads.
    add_executable(test_discovery_client test/test_discovery_client.cpp)
    target_link_libraries(test_discovery_client PRIVATE abstraction_discovery Threads::Threads)
    add_test(NAME DiscoveryClient COMMAND test_discovery_client)
    add_executable(test_local_stream test/test_local_stream.cpp)
    target_link_libraries(test_local_stream PRIVATE Threads::Threads abstraction::ipc)
    target_compile_features(test_local_stream PRIVATE cxx_std_17)
    add_test(NAME LocalStream COMMAND test_local_stream)
    set_tests_properties(DiscoveryClient LocalStream PROPERTIES TIMEOUT 10)

    add_executable(test_sink_containment test/test_sink_containment.cpp)
    target_link_libraries(test_sink_containment PRIVATE abstraction_download)
    add_test(NAME SinkContainment COMMAND test_sink_containment)

    add_executable(test_credential test/test_credential.cpp)
    target_link_libraries(test_credential PRIVATE abstraction_download)
    add_test(NAME Credential COMMAND test_credential)

    add_executable(test_runner test/test_runner.cpp)
    target_link_libraries(test_runner PRIVATE abstraction_download_runner)
    add_test(NAME DownloadRunner COMMAND test_runner)

    add_executable(test_wanted test/test_wanted.cpp)
    target_link_libraries(test_wanted PRIVATE abstraction_download_runner)
    add_test(NAME DownloadWanted COMMAND test_wanted)

    # The corpus is beside the source and never beside the build, so its path is
    # handed in rather than guessed at from the working directory ctest happens
    # to choose.
    add_executable(test_failure test/test_failure.cpp)
    target_link_libraries(test_failure PRIVATE abstraction_download_runner)
    add_test(NAME DownloadFailure COMMAND test_failure
             ${CMAKE_CURRENT_SOURCE_DIR}/../testdata/failures)
endif()

if(ABSTRACTION_DOWNLOAD_STANDALONE)
    include(CMakePackageConfigHelpers)
    set(ABSTRACTION_DOWNLOAD_CMAKEDIR ${CMAKE_INSTALL_LIBDIR}/cmake/abstraction_download)

    # Whatever came in from source travels in this package: a target linked by
    # an exported target and exported nowhere makes an install that cannot be
    # consumed, and CMake says so at generate time.
    get_property(ABSTRACTION_DOWNLOAD_EXTRA GLOBAL PROPERTY ABSTRACTION_SOURCE_TARGETS)
    get_property(ABSTRACTION_DOWNLOAD_EXTRA_INC GLOBAL PROPERTY ABSTRACTION_SOURCE_INCLUDES)

    # A sibling that came from an install stays an install for our consumers
    # too, so the config file asks for exactly the packages this build used.
    get_property(ABSTRACTION_DOWNLOAD_PKGS GLOBAL PROPERTY ABSTRACTION_PACKAGE_DEPS)
    set(ABSTRACTION_DOWNLOAD_DEPS "")
    foreach(p ${ABSTRACTION_DOWNLOAD_PKGS})
        string(APPEND ABSTRACTION_DOWNLOAD_DEPS "find_dependency(${p} CONFIG)\n")
    endforeach()

    install(TARGETS
        abstraction_download abstraction_discovery abstraction_download_runner
        ${ABSTRACTION_DOWNLOAD_EXTRA}
        EXPORT abstraction_downloadTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
    install(DIRECTORY include/ ${ABSTRACTION_DOWNLOAD_EXTRA_INC}
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
    install(EXPORT abstraction_downloadTargets
        FILE abstraction_downloadTargets.cmake
        NAMESPACE abstraction::
        DESTINATION ${ABSTRACTION_DOWNLOAD_CMAKEDIR})

    configure_package_config_file(cmake/abstraction_downloadConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/abstraction_downloadConfig.cmake
        INSTALL_DESTINATION ${ABSTRACTION_DOWNLOAD_CMAKEDIR})
    write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/abstraction_downloadConfigVersion.cmake
        COMPATIBILITY SameMinorVersion)
    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/abstraction_downloadConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/abstraction_downloadConfigVersion.cmake
        DESTINATION ${ABSTRACTION_DOWNLOAD_CMAKEDIR})
else()
    set_property(GLOBAL APPEND PROPERTY ABSTRACTION_SOURCE_TARGETS
        abstraction_download abstraction_discovery abstraction_download_runner)
    set_property(GLOBAL APPEND PROPERTY ABSTRACTION_SOURCE_INCLUDES
        ${CMAKE_CURRENT_SOURCE_DIR}/include/)
endif()
