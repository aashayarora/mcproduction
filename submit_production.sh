#!/bin/bash

set -e

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

EVENTS=2000
CPUS=8
MEMORY="8 GB"
QUEUE_SIZE=75
JOB_FLAVOUR="nextweek"
DRY_RUN=false

    echo "Error: Missing required arguments"
    usage
    exit 1
fi

YEAR=$1
SAMPLE_NAME=$2
GRIDPACK_PATH=$3
shift 3

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

if [ ! -f "${GRIDPACK_PATH}" ]; then
    echo "Error: Gridpack file not found: ${GRIDPACK_PATH}"
    exit 1
fi

EXECUTABLE="${YEAR}.sh"
if [ ! -f "${EXECUTABLE}" ]; then
    echo "Error: Executable script not found: ${EXECUTABLE}"
    exit 1
fi

SUBMIT_FILE="submit_${YEAR}_${SAMPLE_NAME}.jdl"

echo "Creating condor submit file: ${SUBMIT_FILE}"

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

if [ "$DRY_RUN" = true ]; then
    echo "Dry run mode - submit file created but not submitted"
    echo "To submit manually, run:"
    echo "  condor_submit submit_${YEAR}_${SAMPLE_NAME}.jdl"
else
    echo "Submitting job to condor..."
    if ! command -v condor_submit &> /dev/null; then
        echo "Error: condor_submit command not found"
        echo "Please ensure HTCondor is properly installed and configured"
        exit 1
    fi
    
    SUBMIT_OUTPUT=$(condor_submit "submit_${YEAR}_${SAMPLE_NAME}.jdl")
    echo "${SUBMIT_OUTPUT}"
    
    if [ -f "${SUBMIT_FILE}" ]; then
        rm "${SUBMIT_FILE}"
    fi
    CLUSTER_ID=$(echo "${SUBMIT_OUTPUT}" | grep -oP 'submitted to cluster \K\d+' || echo "unknown")
    
    echo ""
    echo "Job submitted successfully!"
    echo "Cluster ID: ${CLUSTER_ID}"
fi