#!/bin/sh
# What every published package must carry, asserted on the built artifact and
# never on the manifest. On 2026-09-09 all five Python wheels built with a
# six-line METADATA — licence declared, licence text absent, no README —
# while every pyproject.toml said license = "Apache-2.0"
# (research/pkg157/RESULTS.md). A manifest is a promise; the archive is the
# package, and only opening it shows what a stranger receives.
#
# The contract is one list; each ecosystem spells it its own way
# (research/pkg157/RESULTS.md §2 has the table and what was rejected):
#
#   licence text    Apache-2.0 §4(a) binds every recipient who redistributes
#                   to hand the licence on; a package without it leaves them
#                   unable to. Wheel: License-File and .dist-info/licenses/;
#                   sdist: LICENSE at the root; Go zip: LICENSE at the module
#                   root; C++ install: share/doc/<pkg>/LICENSE.
#   description     what the ecosystem's page renders. PyPI: the README as
#                   the Description body; pkg.go.dev: the package doc comment;
#                   C++: Description in lib/pkgconfig/<pkg>.pc.
#   floor           the oldest platform we measured. Requires-Python; the go
#                   directive; cxx_std_NN in the exported CMake targets.
#   repository      the way back to the contract page. Project-URL: Source;
#                   the module path itself; URL in the .pc file.
#   version = tag   what is inside equals the tag it was cut from. Go: by
#                   construction, the proxy names the zip after the tag.
#                   Python, C++: the newest python/vX.Y.Z or cpp/vX.Y.Z tag on
#                   the repository; none yet is ABSENT, never a pass.
#   own code only   nothing vendored. A wheel's top level is abstraction_*;
#                   a Go zip has no vendor/; an install's include/ holds only
#                   abstraction/.
#
# Where each artifact comes from, because a check that builds from the wrong
# place passes for the wrong reason:
#
#   Python   wheel and sdist built by setuptools.build_meta from
#            .split/<repo>/python, the published image — the licence and the
#            README beside pyproject.toml are generated files only the split
#            produces, and setuptools refuses to read above its project
#            directory. .split lags HEAD until scripts/split.sh runs; the
#            state line says how old it is.
#   Go       the .zip proxy.golang.org serves for the newest tag. The go
#            command builds that zip from the published repository, adding
#            the repository's root LICENSE unasked; no checkout here can
#            reproduce it, so it is checked where it exists.
#   C++      cmake --install of .split/<repo>/cpp into scratch, for the four
#            layers that carry a project() of their own.
#
#   scripts/package.sh                   every language
#   scripts/package.sh python go cpp     a subset
#   scripts/package.sh --artifact FILE   one built wheel, sdist or module zip,
#                                        as a publish step would check it
#
# A package this run cannot build or fetch is UNPROVEN, by name, with the
# reason. A package nothing has published is ABSENT. Neither is a pass.
# Network: git ls-remote and single foregrounded curl calls with --max-time,
# never a clone, never backgrounded. Nothing is uploaded, nothing installed.
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
ORG="${ABSTRACTION_ORG:-openabstractions}"
SPLIT="${ABSTRACTION_SPLIT:-$ROOT/.split}"
SERIES="$ROOT/research/gate/series.tsv"
OUTDIR="$ROOT/research/pkg157"
TABLE="$OUTDIR/PACKAGES.md"
DATE="$(date +%Y-%m-%d)"
COMMIT="$(git rev-parse --short HEAD)"
NET_TIMEOUT="${PACKAGE_TIMEOUT:-15}"
MIN_DESCRIPTION=200
START=$(date +%s)

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
LOG="$WORK/lines"
: > "$LOG"

NOK=0; NFAIL=0; NUNPROVEN=0; NABSENT=0
record() {
    status=$1; shift
    case "$status" in
        ok)       NOK=$((NOK + 1));             printf '  \033[32mok\033[0m        %s\n' "$*" ;;
        FAIL)     NFAIL=$((NFAIL + 1));         printf '  \033[31mFAIL\033[0m      %s\n' "$*" ;;
        UNPROVEN) NUNPROVEN=$((NUNPROVEN + 1)); printf '  \033[33mUNPROVEN\033[0m  %s\n' "$*" ;;
        ABSENT)   NABSENT=$((NABSENT + 1));     printf '  \033[33mABSENT\033[0m    %s\n' "$*" ;;
    esac
    printf '%s\t%s\n' "$status" "$*" >> "$LOG"
}

PY="${ABSTRACTION_PYTHON:-}"
if [ -z "$PY" ]; then
    for c in "${LOCALAPPDATA:-}/Programs/Python/Python312/python.exe" python3 python; do
        "$c" -c 'import sys' >/dev/null 2>&1 && { PY="$c"; break; }
    done
fi
SETUPTOOLS=""
[ -n "$PY" ] && SETUPTOOLS=$("$PY" -c 'import setuptools; print(setuptools.__version__)' 2>/dev/null || true)

CMAKE="${ABSTRACTION_CMAKE:-$(command -v cmake 2>/dev/null || true)}"
for c in "/c/Program Files/Microsoft Visual Studio/18/Community/Common7/IDE/CommonExtensions/Microsoft/CMake/CMake/bin/cmake.exe" \
         "/c/Program Files/CMake/bin/cmake.exe"; do
    [ -n "$CMAKE" ] || [ ! -x "$c" ] || CMAKE="$c"
done

has_apache_text() { grep -q 'Apache License' "$1" 2>/dev/null && [ "$(wc -c < "$1")" -gt 10000 ]; }

newest_tag() {   # $1 repo $2 prefix ("python/", "cpp/", "go/") -> newest vX.Y.Z after the prefix, or empty
    git ls-remote --tags "https://github.com/$ORG/$1.git" "$2v[0-9]*.[0-9]*.[0-9]*" 2>/dev/null \
        | awk '{print $2}' | sed 's#refs/tags/##' | grep -v '\^{}$' | sort -V | tail -1 | sed "s#^$2##"
}

tag_check() {   # $1 label $2 repo $3 prefix $4 version inside the artifact
    tag=$(newest_tag "$2" "$3")
    if [ -z "$tag" ]; then
        record ABSENT "$1: no ${3}vX.Y.Z tag on github.com/$ORG/$2 — version $4 inside the artifact matches nothing yet"
    elif [ "$tag" = "$4" ]; then
        record ok "$1: version $4 is the newest tag ${3}v$tag"
    else
        record FAIL "$1: version $4 inside the artifact, newest tag is ${3}v$tag"
    fi
}

# ---- one artifact, opened ------------------------------------------------------
# Each check_* fills MISSING with what is not there and VERSION with what is.

check_metadata() {   # $1 METADATA or PKG-INFO file
    # setuptools on Windows writes the whole file CRLF; the blank line that
    # ends the headers is then "\r", which no ^$ matches.
    tr -d '\r' < "$1" > "$WORK/meta.lf"; set -- "$WORK/meta.lf"
    grep -q '^License-Expression: ' "$1" || MISSING="$MISSING; no License-Expression"
    grep -q '^License-File: LICENSE' "$1" || MISSING="$MISSING; no License-File"
    grep -q '^Summary: .' "$1" || MISSING="$MISSING; no Summary"
    grep -q '^Requires-Python: .' "$1" || MISSING="$MISSING; no Requires-Python (the floor)"
    grep -q "^Project-URL: Source, https://github.com/$ORG/" "$1" || MISSING="$MISSING; no Project-URL: Source back to the repository"
    body=$(awk 'blank{print} /^$/{blank=1}' "$1" | wc -c | tr -d ' ')
    if ! grep -q '^Description-Content-Type: ' "$1" || [ "$body" -lt "$MIN_DESCRIPTION" ]; then
        MISSING="$MISSING; no description body ($body bytes) — the index would render the Summary line and nothing else"
    fi
    VERSION=$(sed -n 's/^Version: //p' "$1" | head -1)
}

check_wheel() {   # $1 wheel
    MISSING=""; VERSION=""
    unzip -Z1 "$1" > "$WORK/names" 2>/dev/null || { MISSING="; not a zip"; return; }
    distinfo=$(grep -E '^[^/]+\.dist-info/METADATA$' "$WORK/names" | head -1)
    [ -n "$distinfo" ] || { MISSING="; no .dist-info/METADATA"; return; }
    unzip -p "$1" "$distinfo" > "$WORK/METADATA"
    check_metadata "$WORK/METADATA"
    lic="${distinfo%METADATA}licenses/LICENSE"
    if grep -qx "$lic" "$WORK/names"; then
        unzip -p "$1" "$lic" > "$WORK/LICENSE"
        has_apache_text "$WORK/LICENSE" || MISSING="$MISSING; $lic is not the Apache-2.0 text"
    else
        MISSING="$MISSING; no licence text in the archive (expected $lic)"
    fi
    foreign=$(grep -v '\.dist-info/' "$WORK/names" | grep -v '^abstraction_[a-z_]*\(\.py\|/\)' || true)
    [ -z "$foreign" ] || MISSING="$MISSING; carries files that are not ours: $(printf '%s' "$foreign" | tr '\n' ' ')"
}

check_sdist() {   # $1 tar.gz
    MISSING=""; VERSION=""
    tar -tzf "$1" > "$WORK/names" 2>/dev/null || { MISSING="; not a tar.gz"; return; }
    top=$(head -1 "$WORK/names" | cut -d/ -f1)
    pkginfo=$(grep -x "$top/PKG-INFO" "$WORK/names" | head -1)
    [ -n "$pkginfo" ] || { MISSING="; no PKG-INFO"; return; }
    tar -xzf "$1" -C "$WORK" "$pkginfo"
    check_metadata "$WORK/$pkginfo"
    if grep -qx "$top/LICENSE" "$WORK/names"; then
        tar -xzf "$1" -C "$WORK" "$top/LICENSE"
        has_apache_text "$WORK/$top/LICENSE" || MISSING="$MISSING; LICENSE is not the Apache-2.0 text"
    else
        MISSING="$MISSING; no LICENSE at the sdist root"
    fi
    grep -qx "$top/README.md" "$WORK/names" || MISSING="$MISSING; no README.md at the sdist root"
    foreign=$(grep -E "^$top/[^/]+\.py$" "$WORK/names" | grep -v "^$top/\(abstraction_[a-z_]*\|test_[a-z_]*\)\.py$" || true)
    [ -z "$foreign" ] || MISSING="$MISSING; carries code that is not ours: $(printf '%s' "$foreign" | tr '\n' ' ')"
}

check_gozip() {   # $1 zip $2 module path $3 version
    MISSING=""; VERSION=$3
    root="$2@$3/"
    unzip -Z1 "$1" > "$WORK/names" 2>/dev/null || { MISSING="; not a zip"; return; }
    if grep -qx "${root}LICENSE" "$WORK/names"; then
        unzip -p "$1" "${root}LICENSE" > "$WORK/LICENSE"
        has_apache_text "$WORK/LICENSE" || MISSING="$MISSING; LICENSE is not the Apache-2.0 text"
    else
        MISSING="$MISSING; no LICENSE at the module root"
    fi
    if grep -qx "${root}go.mod" "$WORK/names"; then
        unzip -p "$1" "${root}go.mod" > "$WORK/go.mod"
        grep -q "^module github.com/$ORG/" "$WORK/go.mod" || MISSING="$MISSING; module path is not under github.com/$ORG"
        grep -q '^go [0-9]' "$WORK/go.mod" || MISSING="$MISSING; no go directive (the floor)"
    else
        MISSING="$MISSING; no go.mod"
    fi
    documented=0
    for f in $(grep -E "^$root[^/]+\.go$" "$WORK/names" | grep -v '_test\.go$'); do
        unzip -p "$1" "$f" | grep -q '^// Package [a-z]' && { documented=1; break; }
    done
    [ "$documented" = 1 ] || MISSING="$MISSING; no package doc comment (// Package ...) — pkg.go.dev shows no description"
    grep -q '/vendor/' "$WORK/names" && MISSING="$MISSING; carries a vendor/ directory" || true
}

check_install() {   # $1 prefix $2 package name (abstraction_x)
    MISSING=""; VERSION=""
    cm="$1/lib/cmake/$2"
    if has_apache_text "$1/share/doc/$2/LICENSE"; then :; else
        MISSING="$MISSING; no share/doc/$2/LICENSE"
    fi
    pc="$1/lib/pkgconfig/$2.pc"
    if [ -f "$pc" ]; then
        grep -q '^Description: .' "$pc" || MISSING="$MISSING; $2.pc has no Description"
        grep -q "^URL: https://github.com/$ORG/" "$pc" || MISSING="$MISSING; $2.pc has no URL back to the repository"
    else
        MISSING="$MISSING; no lib/pkgconfig/$2.pc (description, URL)"
    fi
    if [ -f "$cm/${2}ConfigVersion.cmake" ]; then
        VERSION=$(sed -n 's/^set(PACKAGE_VERSION "\([^"]*\)")/\1/p' "$cm/${2}ConfigVersion.cmake" | head -1)
        [ -n "$VERSION" ] || MISSING="$MISSING; ${2}ConfigVersion.cmake carries no PACKAGE_VERSION"
    else
        MISSING="$MISSING; no ${2}ConfigVersion.cmake"
    fi
    grep -qs 'cxx_std_[0-9]' "$cm/${2}Targets.cmake" || MISSING="$MISSING; no cxx_std_NN in the exported targets (the floor)"
    foreign=$(ls "$1/include" 2>/dev/null | grep -vx abstraction || true)
    [ -z "$foreign" ] || MISSING="$MISSING; include/ carries headers that are not ours: $(printf '%s' "$foreign" | tr '\n' ' ')"
}

verdict() {   # $1 label
    if [ -z "$MISSING" ]; then record ok "$1 — licence text, description, floor, repository link, own code only"
    else record FAIL "$1${MISSING}"; fi
}

if [ "${1:-}" = "--artifact" ]; then
    f=$2
    [ -f "$f" ] || { printf 'no such file: %s\n' "$f"; exit 2; }
    case "$f" in
        *.whl)    check_wheel "$f" ;;
        *.tar.gz) check_sdist "$f" ;;
        *.zip)    modpath=$(unzip -Z1 "$f" | head -1 | sed 's/@.*//')
                  version=$(unzip -Z1 "$f" | head -1 | sed 's/^[^@]*@//; s#/.*##')
                  check_gozip "$f" "$modpath" "$version" ;;
        *)        printf 'not a wheel, sdist or module zip: %s\n' "$f"; exit 2 ;;
    esac
    verdict "$(basename "$f")"
    [ "$NFAIL" = 0 ]
    exit
fi

WANT="${*:-python go cpp}"
want() { case " $WANT " in *" $1 "*) return 0 ;; esac; return 1; }

if [ -d "$SPLIT" ]; then
    split_age=$(( ($(date +%s) - $(date -r "$SPLIT/README.md" +%s 2>/dev/null || date +%s)) / 60 ))
    printf 'state: HEAD %s; .split generated %s minute(s) ago by scripts/split.sh; python %s setuptools %s; cmake %s\n\n' \
        "$COMMIT" "$split_age" "${PY:-none}" "${SETUPTOOLS:-none}" "${CMAKE:-none}"
else
    printf 'state: HEAD %s; no .split — run scripts/split.sh first; python %s; cmake %s\n\n' "$COMMIT" "${PY:-none}" "${CMAKE:-none}"
fi

# ---- Python -------------------------------------------------------------------

if want python; then
printf '\033[1mPython — wheel and sdist built from the published image\033[0m\n'
py_pkgs='
abstraction-cas cas
abstraction-download download
abstraction-job job
abstraction-model model
abstraction-watch watch
'
printf '%s\n' "$py_pkgs" > "$WORK/py_pkgs"
while IFS=' ' read -r repo short; do
    [ -n "$repo" ] || continue
    src="$SPLIT/$repo/python"
    if [ ! -f "$src/pyproject.toml" ]; then
        record UNPROVEN "$repo: $src/pyproject.toml is not there — run scripts/split.sh, the wheel is built from the published image"
        continue
    fi
    if [ -z "$PY" ] || [ -z "$SETUPTOOLS" ] || [ "${SETUPTOOLS%%.*}" -lt 77 ]; then
        record UNPROVEN "$repo: no Python with setuptools>=77 (found ${SETUPTOOLS:-none}) — set ABSTRACTION_PYTHON"
        continue
    fi
    b="$WORK/py-$short"
    rm -rf "$b"; mkdir -p "$b/dist"
    cp -R "$src/." "$b/src"
    # One process each: after build_wheel, setuptools' build_sdist in the same
    # process writes the tarball under <src>/bdist_wheel/, not where it was told.
    built=1
    for hook in build_sdist build_wheel; do
        (cd "$b/src" && "$PY" -c 'import sys, setuptools.build_meta as m; getattr(m, sys.argv[1])(sys.argv[2])' "$hook" "$b/dist") >> "$b/build.log" 2>&1 || built=0
    done
    if [ "$built" = 0 ]; then
        record FAIL "$repo: setuptools refused to build from $src — $(grep -E 'Error|error:' "$b/build.log" | tail -1)"
        continue
    fi
    whl=$(ls "$b"/dist/*.whl 2>/dev/null | head -1); sdist=$(ls "$b"/dist/*.tar.gz 2>/dev/null | head -1)
    if [ -z "$whl" ] || [ -z "$sdist" ]; then
        record FAIL "$repo: build finished but produced $(ls "$b/dist" | tr '\n' ' ') — expected one .whl and one .tar.gz"
        continue
    fi
    check_wheel "$whl"; verdict "$repo wheel $(basename "$whl")"
    wheel_version=$VERSION
    check_sdist "$sdist"; verdict "$repo sdist $(basename "$sdist")"
    tag_check "$repo python" "$repo" "python/" "$wheel_version"
done < "$WORK/py_pkgs"
fi

# ---- Go -----------------------------------------------------------------------

if want go; then
printf '\n\033[1mGo — the module-proxy zip for the newest tag\033[0m\n'
go_pkgs='
abstraction-job go
abstraction-download go
abstraction-storage go
abstraction-config go
abstraction-logging go
abstraction-model go
abstraction-facade go
abstraction-identity -
abstraction-cas go
abstraction-watch go
abstraction-rights go
abstraction-asks go
'
printf '%s\n' "$go_pkgs" > "$WORK/go_pkgs"
while IFS=' ' read -r repo suffix; do
    [ -n "$repo" ] || continue
    [ "$suffix" = "-" ] && suffix=""
    modpath="github.com/$ORG/$repo${suffix:+/$suffix}"
    version=$(newest_tag "$repo" "${suffix:+$suffix/}")
    if [ -z "$version" ]; then
        record ABSENT "$modpath: no ${suffix:+$suffix/}vX.Y.Z tag on github.com/$ORG/$repo — nothing published to open"
        continue
    fi
    zip="$WORK/$repo.zip"
    code=$(curl -s -o "$zip" -w '%{http_code}' --max-time "$NET_TIMEOUT" --connect-timeout "$NET_TIMEOUT" \
        "https://proxy.golang.org/$modpath/@v/$version.zip" 2>/dev/null) || code=000
    if [ "$code" != "200" ] || [ ! -s "$zip" ]; then
        record UNPROVEN "$modpath@$version: module proxy did not serve the zip (http $code)"
        continue
    fi
    check_gozip "$zip" "$modpath" "$version"; verdict "$modpath@$version"
done < "$WORK/go_pkgs"
fi

# ---- C++ ----------------------------------------------------------------------

if want cpp; then
printf '\n\033[1mC++ — cmake --install of the published layer, into scratch\033[0m\n'
record ABSENT "abstraction-cas cpp: no CMakeLists.txt is published (split.manifest drops it) — the repository is the package; nothing to install"
for repo in abstraction-download abstraction-job abstraction-storage abstraction-watch; do
    name="abstraction_${repo#abstraction-}"
    src="$SPLIT/$repo/cpp"
    if [ ! -f "$src/CMakeLists.txt" ]; then
        record UNPROVEN "$repo cpp: $src/CMakeLists.txt is not there — run scripts/split.sh"
        continue
    fi
    if [ -z "$CMAKE" ]; then
        record UNPROVEN "$repo cpp: no cmake — set ABSTRACTION_CMAKE"
        continue
    fi
    b="$WORK/cpp-$repo"
    if ! "$CMAKE" -S "$src" -B "$b/build" -DABSTRACTION_BUILD_TESTS=OFF -DCMAKE_BUILD_TYPE=Release > "$b.configure.log" 2>&1; then
        record FAIL "$repo cpp: does not configure from the published sources — $(grep -A3 'CMake Error' "$b.configure.log" | sed '1d; /^Call Stack/,$d' | tr -s ' \n' ' ')"
        continue
    fi
    if ! "$CMAKE" --build "$b/build" --config Release > "$b.build.log" 2>&1; then
        record FAIL "$repo cpp: does not build — $(grep -m1 -i ' error ' "$b.build.log")"
        continue
    fi
    if ! "$CMAKE" --install "$b/build" --config Release --prefix "$b/prefix" > "$b.install.log" 2>&1; then
        record FAIL "$repo cpp: does not install — $(tail -1 "$b.install.log")"
        continue
    fi
    check_install "$b/prefix" "$name"; verdict "$repo cpp install of $name"
    tag_check "$repo cpp" "$repo" "cpp/" "${VERSION:-?}"
done
fi

# ---- the generated table -----------------------------------------------------

mkdir -p "$OUTDIR"
{
    printf '# Package contract, checked\n\n'
    printf 'Generated by `scripts/package.sh` — never hand-edit this file, rerun the\n'
    printf 'script. Last run %s at commit `%s`. Each line is one built artifact,\n' "$DATE" "$COMMIT"
    printf 'opened; the header of `scripts/package.sh` says what every package must\n'
    printf 'carry and where each artifact was taken from.\n\n'
    printf '| verdict | artifact |\n|---|---|\n'
    while IFS='	' read -r status line; do printf '| %s | %s |\n' "$status" "$line"; done < "$LOG"
} > "$TABLE"

END=$(date +%s)
printf '%s\t%s\t%s\t%s\t%s\n' "$DATE" "$COMMIT" package_fail "$NFAIL" package.sh >> "$SERIES"
printf '%s\t%s\t%s\t%s\t%s\n' "$DATE" "$COMMIT" package_seconds "$((END - START))" package.sh >> "$SERIES"
printf '\n  table regenerated: %s\n' "$TABLE"
printf '  runtime: %ss\n' "$((END - START))"
printf '\n  %s artifact(s) opened: %s ok, %s failing the contract, %s unproven, %s absent\n' \
    "$((NOK + NFAIL))" "$NOK" "$NFAIL" "$NUNPROVEN" "$NABSENT"

if [ "$NFAIL" -gt 0 ]; then
    printf '  \033[31mFAIL\033[0m  %s artifact(s) do not carry what the contract says — see above\n' "$NFAIL"
    exit 1
elif [ "$NUNPROVEN" -gt 0 ]; then
    printf '  \033[33mUNPROVEN\033[0m  %s artifact(s) could not be built or fetched — never counted as a pass\n' "$NUNPROVEN"
    exit 2
else
    printf '  \033[32mOK\033[0m  every artifact opened carries the contract\n'
    exit 0
fi
