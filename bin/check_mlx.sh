#!/bin/bash
#
# check_mlx.sh - Verify MLX is properly configured and findable
#
# This script tests the MLX build environment without modifying any code.
# Useful for debugging rustmlx/mlx-sys build issues.
#
# Usage: ./bin/check_mlx.sh
#

set -e

echo "=========================================="
echo "MLX Build Environment Verification"
echo "=========================================="
echo ""

# Track if all checks pass
ALL_OK=true

# Check 1: Check if Xcode is installed
echo "[1/5] Checking Xcode installation..."
if command -v xcodebuild &> /dev/null; then
    XCODEBUILD_VERSION=$(xcodebuild -version 2>&1 | head -1)
    echo "  ✓ xcodebuild is installed: $XCODEBUILD_VERSION"
else
    echo "  ✗ xcodebuild is NOT installed"
    ALL_OK=false
fi
echo ""

# Check 2: Check if clang is available
echo "[2/5] Checking clang..."
if command -v clang &> /dev/null; then
    CLANG_VERSION=$(clang --version 2>&1 | head -1)
    echo "  ✓ clang is available: $CLANG_VERSION"
    CLANG_PATH=$(which clang)
    echo "    Path: $CLANG_PATH"
else
    echo "  ✗ clang is NOT found in PATH"
    ALL_OK=false
fi
echo ""

# Check 3: Check if MLX framework exists
echo "[3/5] Checking MLX framework..."

# First check if MLX is installed via Homebrew
MLX_CELLAR=$(brew --cellar mlx 2>/dev/null || echo "")
OMLX_CELLAR=$(brew --cellar omlx 2>/dev/null || echo "")

# Search in priority order: native MLX cellar, then common locations
MLX_INSTALL_PATHS=()

if [ -n "$MLX_CELLAR" ] && [ -d "$MLX_CELLAR" ]; then
    MLX_INSTALL_PATHS+=("$MLX_CELLAR")
    echo "  Found MLX cellar via Homebrew: $MLX_CELLAR"
elif [ -n "$OMLX_CELLAR" ] && [ -d "$OMLX_CELLAR" ]; then
    # omlx is a Python wrapper - check if it has native MLX libs
    if [ -f "$OMLX_CELLAR/lib/libmlx.1.dylib" ] || [ -f "$OMLX_CELLAR/lib/libmlx.so" ]; then
        MLX_INSTALL_PATHS+=("$OMLX_CELLAR")
        echo "  Found omlx with native MLX libraries: $OMLX_CELLAR"
    else
        echo "  ⚠ omlx found but without native MLX libraries (Python wrapper only)"
        ALL_OK=false
    fi
else
    MLX_INSTALL_PATHS=(
        "/opt/homebrew/opt/mlx"
        "/usr/local/opt/mlx"
        "$HOME/.cargo/mlx"
        "$HOME/mlx"
        "/Library/Frameworks/MLX.framework"
        "/opt/homebrew/Frameworks/MLX.framework"
    )
    echo "  MLX not found in cellar, checking common locations..."
fi

FOUND_MLX=false
for path in "${MLX_INSTALL_PATHS[@]}"; do
    if [ -d "$path" ]; then
        # Validate that this path actually has MLX headers/libs
        MLX_INCLUDE="$path/include"
        MLX_LIB="$path/lib"
        if [ -d "$MLX_INCLUDE" ] && ([ -f "$MLX_INCLUDE/mlx.h" ] || [ -f "$MLX_INCLUDE/mlx/core.h" ]); then
            echo "  ✓ MLX found at: $path"
            FOUND_MLX=true
            MLX_PATH="$path"
            break
        fi
    fi
done

if [ "$FOUND_MLX" = false ]; then
    echo "  ✗ MLX framework NOT found in common locations"
    echo "  Searched: ${MLX_INSTALL_PATHS[*]}"
    echo ""
    echo "  Quick install option:"
    echo "    brew install mlx"
    ALL_OK=false
fi
echo ""

# Check 4: Verify MLX headers are present
if [ "$FOUND_MLX" = true ]; then
    echo "[4/5] Checking MLX headers..."
    MLX_INCLUDE="$MLX_PATH/include"
    MLX_LIB="$MLX_PATH/lib"
    
    if [ -d "$MLX_INCLUDE" ]; then
        echo "  ✓ MLX include directory exists: $MLX_INCLUDE"
        # Check for mlx.h or mlx/core.h
        if [ -f "$MLX_INCLUDE/mlx.h" ] || [ -f "$MLX_INCLUDE/mlx/core.h" ]; then
            echo "  ✓ MLX header files present"
        else
            echo "  ⚠ MLX header files may be missing or in unexpected location"
            ls -la "$MLX_INCLUDE/" 2>/dev/null | head -10
        fi
    else
        echo "  ✗ MLX include directory NOT found"
        ALL_OK=false
    fi
    
    if [ -d "$MLX_LIB" ]; then
        echo "  ✓ MLX lib directory exists: $MLX_LIB"
        # Check for mlx.1.dylib or libmlx.so
        if [ -f "$MLX_LIB/libmlx.1.dylib" ] || [ -f "$MLX_LIB/libmlx.so" ]; then
            echo "  ✓ MLX library files present"
        else
            echo "  ⚠ MLX library files may be missing"
            ls -la "$MLX_LIB/" 2>/dev/null | head -10
        fi
    else
        echo "  ✗ MLX lib directory NOT found"
        ALL_OK=false
    fi
else
    echo "[4/5] Checking MLX headers... (skipped - MLX not found)"
fi
echo ""

# Check 5: Test if rustc can find MLX with correct flags
echo "[5/5] Testing rustc MLX detection..."
if [ "$FOUND_MLX" = true ]; then
    # Create a temporary test file
    TMP_TEST="/tmp/mlx_test.rs"
    cat > "$TMP_TEST" << 'EOF'
extern "C" {
    fn mlx_version() -> i32;
}

fn main() {
    let version = unsafe { mlx_version() };
    println!("MLX version: {}", version);
}
EOF

    # Try to compile with MLX flags
    MLX_CFLAGS="-I$MLX_INCLUDE"
    MLX_LDFLAGS="-L$MLX_LIB -lmlx"
    
    echo "  Testing compilation with:"
    echo "    CFLAGS: $MLX_CFLAGS"
    echo "    LDFLAGS: $MLX_LDFLAGS"
    echo ""
    
    if rustc --crate-type bin -- "$MLX_CFLAGS" -- "$MLX_LDFLAGS" "$TMP_TEST" -o /tmp/mlx_test 2>&1; then
        echo "  ✓ rustc successfully compiled test with MLX"
        # Run the test if executable
        if [ -x /tmp/mlx_test ]; then
            /tmp/mlx_test
        fi
        rm -f /tmp/mlx_test /tmp/mlx_test.rs
    else
        echo "  ✗ rustc failed to compile test with MLX"
        echo "  Error output above"
        ALL_OK=false
    fi
else
    echo "[5/5] Testing rustc MLX detection... (skipped - MLX not found)"
fi
echo ""

# Summary
echo "=========================================="
echo "Summary"
echo "=========================================="

if [ "$ALL_OK" = true ]; then
    echo "✓ All checks passed! MLX appears to be properly configured."
    echo ""
    echo "To use with rustmlx/mlx-sys, you may need to:"
    echo "  export MLX_CFLAGS=-I$MLX_INCLUDE"
    echo "  export MLX_LDFLAGS=-L$MLX_LIB -lmlx"
    echo "  cargo build --features mlx --no-default-features"
    exit 0
else
    echo "✗ Some checks failed. Please address the issues above."
    echo ""
    echo "Quick install option:"
    echo "  brew install mlx"
    echo ""
    echo "Common fixes:"
    echo "  - If using Homebrew: brew install mlx"
    echo "  - If using rustmlx: rustup component add rustfmt --toolchain nightly-aarch64-apple-darwin"
    echo "  - May need to set: export MLX_PATH=/path/to/mlx"
    echo "  - Or run: cargo build --features mlx --no-default-features"
    exit 1
fi
