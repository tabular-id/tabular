#!/usr/bin/env bash
# ==============================================================================
# Tabular release: VERSION -> Cargo.toml + Xcode -> commit -> tag -> push
# ==============================================================================
# Usage:
#   ./release.sh              # baca VERSION, sync, commit, tag vX.Y.Z, push
#   ./release.sh --no-push    # semua langkah kecuali push
#   ./release.sh -y           # tanpa konfirmasi sebelum push
#
# Versi diambil dari file VERSION (mis. "1.5.1"). Sinkronisasi file dilakukan oleh
# scripts/sync-version.sh (Cargo.toml, ketiga project Xcode: MARKETING_VERSION dan
# CURRENT_PROJECT_VERSION, plus metadata paket lain).
# ==============================================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m'

print_info()    { echo -e "${BLUE}==>${NC} ${BOLD}$1${NC}"; }
print_success() { echo -e "${GREEN}==>${NC} $1"; }
print_warning() { echo -e "${YELLOW}WARNING:${NC} $1"; }
print_error()   { echo -e "${RED}ERROR:${NC} $1" >&2; }

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

PUSH=1
ASSUME_YES=0
for arg in "$@"; do
    case "$arg" in
        --no-push) PUSH=0 ;;
        -y|--yes)  ASSUME_YES=1 ;;
        -h|--help) sed -n '2,13p' "$0"; exit 0 ;;
        *) print_error "Unknown option: $arg"; exit 1 ;;
    esac
done

REMOTE="${RELEASE_REMOTE:-origin}"

# --- 1. Baca & validasi VERSION -------------------------------------------------
if [[ ! -f VERSION ]]; then
    print_error "VERSION file not found in ${ROOT_DIR}"
    exit 1
fi
VERSION="$(tr -d '[:space:]' < VERSION)"
VERSION="${VERSION#v}"
if [[ -z "$VERSION" ]]; then
    print_error "VERSION file is empty"
    exit 1
fi
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
    print_error "Invalid version '${VERSION}' in VERSION (expected SemVer, e.g. 1.5.1)"
    exit 1
fi
TAG="v${VERSION}"
BRANCH="$(git rev-parse --abbrev-ref HEAD)"

print_info "Releasing Tabular ${TAG} from branch ${BRANCH}"

# --- 2. Cegah tag ganda -----------------------------------------------------------
if git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null; then
    print_error "Tag ${TAG} already exists locally. Bump VERSION first."
    exit 1
fi
if [[ $PUSH -eq 1 ]] && git ls-remote --exit-code --tags "$REMOTE" "refs/tags/${TAG}" >/dev/null 2>&1; then
    print_error "Tag ${TAG} already exists on ${REMOTE}. Bump VERSION first."
    exit 1
fi

# --- 3. Sinkronkan Cargo.toml, Xcode (version + build number), metadata ---------
./scripts/sync-version.sh "$VERSION"

# Cargo.lock ikut diperbarui agar versi crate tabular cocok dengan Cargo.toml.
if command -v cargo >/dev/null 2>&1; then
    if ! cargo update -p tabular --offline >/dev/null 2>&1; then
        cargo update -p tabular >/dev/null 2>&1 || print_warning "Could not refresh Cargo.lock; run 'cargo check' manually"
    fi
else
    print_warning "cargo not found; Cargo.lock not refreshed"
fi

# --- 4. Commit hanya file versi (agent/orang lain bisa punya perubahan lain) ----
RELEASE_FILES=(
    VERSION
    Cargo.toml
    Cargo.lock
    Tabular.xcodeproj/project.pbxproj
    macos/Xcode/Tabular.xcodeproj/project.pbxproj
    ios/Xcode/TabulariOS/TabulariOS.xcodeproj/project.pbxproj
    id.tabular.database.metainfo.xml
    flatpak/id.tabular.database.flathub.yml
    PKGBUILD
    .SRCINFO
    aur/tabular-bin/PKGBUILD
    aur/tabular-bin/.SRCINFO
    SECURITY.md
)
EXISTING_FILES=()
for f in "${RELEASE_FILES[@]}"; do
    [[ -e "$f" ]] && EXISTING_FILES+=("$f")
done
git add -- "${EXISTING_FILES[@]}"

if git diff --cached --quiet -- "${EXISTING_FILES[@]}"; then
    print_warning "No version changes to commit; tagging current HEAD"
else
    git commit -m "chore: release ${TAG}" -- "${EXISTING_FILES[@]}"
    print_success "Committed version bump"
fi

# --- 5. Tag ---------------------------------------------------------------------
git tag -a "$TAG" -m "Tabular ${TAG}"
print_success "Created tag ${TAG}"

# --- 6. Push --------------------------------------------------------------------
if [[ $PUSH -eq 0 ]]; then
    print_info "Skipping push. To publish later:"
    echo "  git push ${REMOTE} ${BRANCH} && git push ${REMOTE} ${TAG}"
    exit 0
fi

if [[ $ASSUME_YES -eq 0 ]]; then
    read -r -p "Push ${BRANCH} and ${TAG} to ${REMOTE}? [y/N] " answer
    if [[ ! "$answer" =~ ^[Yy]$ ]]; then
        print_warning "Push cancelled. Commit and tag ${TAG} stay local."
        echo "  Publish: git push ${REMOTE} ${BRANCH} && git push ${REMOTE} ${TAG}"
        echo "  Undo:    git tag -d ${TAG} && git reset --soft HEAD~1"
        exit 0
    fi
fi

git push "$REMOTE" "$BRANCH"
git push "$REMOTE" "$TAG"
print_success "Released ${TAG} (pushed ${BRANCH} and ${TAG} to ${REMOTE})"
