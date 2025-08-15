#!/bin/bash

# Script to create and submit condor jobs for Monte Carlo production
# Usage: ./submit_production.sh <year> <sample_name> <gridpack_path> [options]

set -e

# Function to display usage
usage() {
    echo "Usage: $0 <year> <sample_name> <gridpack_path> [options]"
    echo ""
    echo "Arguments:"
    echo "  year           - Production year (e.g., Run3Summer22, Run3Summer24)"
    echo "  sample_name    - Sample name for the job"
    echo "  gridpack_path  - Full path to the gridpack tarball"
    echo ""
    echo "Options:"
    echo "  -n, --events     Number of events to generate (default: 3000)"
    echo "  -c, --cpus       Number of CPUs to request (default: 8)"
    echo "  -m, --memory     Memory to request (default: 8 GB)"
    echo "  -q, --queue      Number of jobs to queue (default: 50)"
    echo "  -f, --flavour    Job flavour (default: nextweek)"
    echo "  --dry-run        Create submit file but don't submit"
    echo "  -h, --help       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 Run3Summer24 VBSWWH_OSWW_C2V1p5_5f_LO /eos/user/a/aaarora/gridpacks/sample.tar.xz"
}

# Default values
EVENTS=3000
CPUS=8
MEMORY="8 GB"
QUEUE_SIZE=50
JOB_FLAVOUR="nextweek"
DRY_RUN=false

# Parse command line arguments
if [ $# -lt 3 ]; then
    echo "Error: Missing required arguments"
    usage
    exit 1
fi

YEAR=$1
SAMPLE_NAME=$2
GRIDPACK_PATH=$3
shift 3

# Parse optional arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--events)
            EVENTS="$2"
            shift 2
            ;;
        -c|--cpus)
            CPUS="$2"
            shift 2
            ;;
        -m|--memory)
            MEMORY="$2"
            shift 2
            ;;
        -q|--queue)
            QUEUE_SIZE="$2"
            shift 2
            ;;
        -f|--flavour)
            JOB_FLAVOUR="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate inputs
if [ ! -f "${GRIDPACK_PATH}" ]; then
    echo "Error: Gridpack file not found: ${GRIDPACK_PATH}"
    exit 1
fi

# Create year directory if it doesn't exist
YEAR_DIR="/afs/cern.ch/work/a/aaarora/production/${YEAR}"
if [ ! -d "${YEAR_DIR}" ]; then
    echo "Creating directory: ${YEAR_DIR}"
    mkdir -p "${YEAR_DIR}"
fi

# Create logs directory
LOGS_DIR="${YEAR_DIR}/logs"
if [ ! -d "${LOGS_DIR}" ]; then
    echo "Creating logs directory: ${LOGS_DIR}"
    mkdir -p "${LOGS_DIR}"
fi

# Determine the executable script
EXECUTABLE="${YEAR}.sh"
if [ ! -f "${YEAR_DIR}/${EXECUTABLE}" ]; then
    # Copy from the main directory if it exists there
    if [ -f "/afs/cern.ch/work/a/aaarora/production/${EXECUTABLE}" ]; then
        echo "Copying ${EXECUTABLE} to ${YEAR_DIR}/"
        cp "/afs/cern.ch/work/a/aaarora/production/${EXECUTABLE}" "${YEAR_DIR}/"
    else
        echo "Error: Executable script ${EXECUTABLE} not found"
        exit 1
    fi
fi

# Generate submit file name
SUBMIT_FILE="submit_${YEAR}_${SAMPLE_NAME}.jdl"

echo "Creating condor submit file: ${SUBMIT_FILE}"

# Create the condor submit file
cat > "${SUBMIT_FILE}" << EOF
executable              = ${EXECUTABLE}

arguments               = ${GRIDPACK_PATH} ${SAMPLE_NAME} ${EVENTS} \$(request_cpus)

log                     = logs/\$(Cluster).\$(Process).log
output                  = logs/\$(Cluster).\$(Process).out
error                   = logs/\$(Cluster).\$(Process).err

request_cpus            = ${CPUS}
request_memory          = ${MEMORY}

universe                = vanilla

+JobFlavour             = "${JOB_FLAVOUR}"
MY.WantOS               = "el8"

on_exit_remove          = (ExitBySignal == False) && (ExitCode == 0)
max_retries             = 3
requirements            = Machine =!= LastRemoteHost

queue ${QUEUE_SIZE}
EOF

echo "Submit file created successfully: ${SUBMIT_FILE}"
echo ""
echo "Job configuration:"
echo "  Year:          ${YEAR}"
echo "  Sample:        ${SAMPLE_NAME}"
echo "  Gridpack:      ${GRIDPACK_PATH}"
echo "  Events:        ${EVENTS}"
echo "  CPUs:          ${CPUS}"
echo "  Memory:        ${MEMORY}"
echo "  Queue size:    ${QUEUE_SIZE}"
echo "  Job flavour:   ${JOB_FLAVOUR}"
echo ""

# Submit the job unless dry run
if [ "$DRY_RUN" = true ]; then
    echo "Dry run mode - submit file created but not submitted"
    echo "To submit manually, run:"
    echo "  condor_submit submit_${YEAR}_${SAMPLE_NAME}.jdl"
else
    echo "Submitting job to condor..."
    # Check if condor_submit is available
    if ! command -v condor_submit &> /dev/null; then
        echo "Error: condor_submit command not found"
        echo "Please ensure HTCondor is properly installed and configured"
        exit 1
    fi
    
    # Submit the job
    SUBMIT_OUTPUT=$(condor_submit "submit_${YEAR}_${SAMPLE_NAME}.jdl")
    echo "${SUBMIT_OUTPUT}"
    
    # delete the submit file after submission if not in dry run mode
    if [ -f "${SUBMIT_FILE}" ]; then
        rm "${SUBMIT_FILE}"
    fi
    # Extract cluster ID if possible
    CLUSTER_ID=$(echo "${SUBMIT_OUTPUT}" | grep -oP 'submitted to cluster \K\d+' || echo "unknown")
    
    echo ""
    echo "Job submitted successfully!"
    echo "Cluster ID: ${CLUSTER_ID}"
fi