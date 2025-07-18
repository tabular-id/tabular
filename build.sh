#!/bin/bash
# Quick build script for Tabular
# Usage: ./build.sh [platform]
# Platforms: macos, linux, windows, all

set -e

APP_NAME="Tabular"
VERSION="0.1.0"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if required tools are installed
check_dependencies() {
    print_status "Checking dependencies..."
    
    if ! command -v cargo &> /dev/null; then
        print_error "Cargo is not installed. Please install Rust first."
        exit 1
    fi
    
    if ! command -v make &> /dev/null; then
        print_error "Make is not installed. Please install make."
        exit 1
    fi
    
    print_success "All dependencies are available!"
}

# Show help
show_help() {
    echo "🛠️  Tabular Build Script"
    echo "======================="
    echo ""
    echo "Usage: $0 [PLATFORM] [OPTIONS]"
    echo ""
    echo "Platforms:"
    echo "  macos     - Build universal macOS binary and .app bundle"
    echo "  linux     - Build Linux binaries for x86_64 and aarch64"
    echo "  windows   - Build Windows binaries for x86_64 and aarch64"
    echo "  all       - Build for all platforms (default)"
    echo ""
    echo "Options:"
    echo "  --deps    - Install build dependencies first"
    echo "  --clean   - Clean before building"
    echo "  --help    - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 macos          # Build macOS only"
    echo "  $0 linux --clean  # Clean and build Linux"
    echo "  $0 all --deps     # Install deps and build all"
    echo ""
}

# Parse command line arguments
PLATFORM="all"
INSTALL_DEPS=false
CLEAN_FIRST=false

while [[ $# -gt 0 ]]; do
    case $1 in
        macos|linux|windows|all)
            PLATFORM="$1"
            shift
            ;;
        --deps)
            INSTALL_DEPS=true
            shift
            ;;
        --clean)
            CLEAN_FIRST=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Main build function
main() {
    print_status "Starting build for platform: $PLATFORM"
    
    check_dependencies
    
    # Install dependencies if requested
    if [ "$INSTALL_DEPS" = true ]; then
        print_status "Installing build dependencies..."
        make install-deps
    fi
    
    # Clean if requested
    if [ "$CLEAN_FIRST" = true ]; then
        print_status "Cleaning previous builds..."
        make clean
    fi
    
    # Build based on platform
    case $PLATFORM in
        macos)
            print_status "Building macOS universal binary..."
            make bundle-macos
            print_success "macOS build completed!"
            ;;
        linux)
            print_status "Building Linux binaries..."
            make bundle-linux
            print_success "Linux build completed!"
            ;;
        windows)
            print_status "Building Windows binaries..."
            make bundle-windows
            print_success "Windows build completed!"
            ;;
        all)
            print_status "Building for all platforms..."
            make release
            print_success "All platform builds completed!"
            ;;
        *)
            print_error "Unknown platform: $PLATFORM"
            exit 1
            ;;
    esac
    
    # Show build results
    echo ""
    print_success "🎉 Build completed successfully!"
    echo ""
    print_status "📦 Generated files:"
    
    if [ -d "dist" ]; then
        find dist -type f \( -name "*.dmg" -o -name "*.app" -o -name "*.tar.gz" -o -name "*.zip" \) | while read -r file; do
            size=$(ls -lh "$file" | awk '{print $5}')
            echo "  📁 $file ($size)"
        done
    else
        print_warning "No distribution files found. Build may have failed."
    fi
    
    echo ""
    print_status "✨ Ready for distribution!"
}

# Run main function
main "$@"
