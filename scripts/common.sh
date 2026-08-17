#!/usr/bin/env bash
# ==============================================================================
# Common Helper Functions and Environment Loader
# ==============================================================================

set -eo pipefail

# ANSI Color Codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Check and source environment file
if [[ -f "${ROOT_DIR}/env.sh" ]]; then
    # shellcheck source=/dev/null
    source "${ROOT_DIR}/env.sh"
elif [[ -f "${ROOT_DIR}/env.sh.example" ]]; then
    echo -e "${YELLOW}[WARN] 'env.sh' not found. Sourcing defaults from 'env.sh.example'...${NC}"
    # shellcheck source=/dev/null
    source "${ROOT_DIR}/env.sh.example"
else
    echo -e "${RED}[ERROR] Neither env.sh nor env.sh.example found in ${ROOT_DIR}!${NC}"
    exit 1
fi

log_info() {
    echo -e "${BLUE}[INFO] $(date +'%Y-%m-%d %H:%M:%S') - $1${NC}"
}

log_success() {
    echo -e "${GREEN}[SUCCESS] $(date +'%Y-%m-%d %H:%M:%S') - $1${NC}"
}

log_warn() {
    echo -e "${YELLOW}[WARN] $(date +'%Y-%m-%d %H:%M:%S') - $1${NC}"
}

log_error() {
    echo -e "${RED}[ERROR] $(date +'%Y-%m-%d %H:%M:%S') - $1${NC}" >&2
}

check_prerequisites() {
    if ! command -v gcloud &> /dev/null; then
        log_error "Google Cloud CLI ('gcloud') is not installed or not in PATH."
        exit 1
    fi

    if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "your-gcp-project-id" ]]; then
        log_error "PROJECT_ID is not configured. Please edit 'env.sh' with your GCP Project ID."
        exit 1
    fi
}
