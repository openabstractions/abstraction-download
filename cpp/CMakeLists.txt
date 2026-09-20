cmake_minimum_required(VERSION 3.16)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    project(abstraction_download LANGUAGES CXX VERSION 0.1.0)
    set(ABSTRACTION_DOWNLOAD_STANDALONE ON)
else()
    set(ABSTRACTION_DOWNLOAD_STANDALONE OFF)
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
# The generated request vocabulary: header-only, standard library only.
add_library(abstraction_download_request INTERFACE)
add_library(abstraction::download_request ALIAS abstraction_download_request)
set_target_properties(abstraction_download_request PROPERTIES EXPORT_NAME download_request)
target_compile_features(abstraction_download_request INTERFACE cxx_std_17)
target_include_directories(abstraction_download_request INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
install(TARGETS abstraction_download_request EXPORT abstraction_download_requestTargets)
install(DIRECTORY abstraction/download/request DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/abstraction/download)
set(request_config_dir ${CMAKE_INSTALL_LIBDIR}/cmake/abstraction_download_request)
install(EXPORT abstraction_download_requestTargets NAMESPACE abstraction:: DESTINATION ${request_config_dir})
configure_package_config_file(cmake/abstraction_download_requestConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/abstraction_download_requestConfig.cmake
    INSTALL_DESTINATION ${request_config_dir})
write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/abstraction_download_requestConfigVersion.cmake
    VERSION 0.1.0 COMPATIBILITY SameMinorVersion)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/abstraction_download_requestConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/abstraction_download_requestConfigVersion.cmake DESTINATION ${request_config_dir})
