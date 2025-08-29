#!/bin/bash
set -e

GRIDPACK=$1
SAMPLE=$2
EVENTS=${3:-10000}
NUMTHREADS=${4:-4}
FRAGMENT="HIG-RunIII2024Summer24wmLHEGS-00758"
SEED=$(($(date +%s) % 100 + 1))

export X509_USER_PROXY=/afs/cern.ch/work/a/aaarora/x509up_u141045

mkdir -p ${SAMPLE}
cd ${SAMPLE}

curl -s -k https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_fragment/$FRAGMENT --retry 3 --create-dirs -o Configuration/GenProduction/python/$FRAGMENT-fragment.py
[ -s Configuration/GenProduction/python/$FRAGMENT-fragment.py ] || exit $?;

# replace the gridpack in the fragment
sed -i "s|cms.vstring('/cvmfs/cms.cern.ch/phys_generator/gridpacks/RunIII/13p6TeV/el8_amd64_gcc10/MadGraph5_aMCatNLO/TT4b_5f_LO_madspinON_el8_amd64_gcc10_CMSSW_12_4_8_tarball.tar.xz')|cms.vstring('$GRIDPACK')|g" Configuration/GenProduction/python/$FRAGMENT-fragment.py
sed -i "s|cms.untracked.uint32(5000)|cms.untracked.uint32(${EVENTS})|g" Configuration/GenProduction/python/$FRAGMENT-fragment.py
sed -i "s|generateConcurrently = cms.untracked.bool(False)|generateConcurrently = cms.untracked.bool(True)|g" Configuration/GenProduction/python/$FRAGMENT-fragment.py

sed -i '/processParameters = cms.vstring(/,/)/c\processParameters = cms.vstring(\
        '\''SpaceShower:dipoleRecoil = on'\'',\
        '\''25:m0 = 125.0'\'',\
        '\''25:onMode = off'\'',\
        '\''25:onIfAny = 5 -5'\''),' Configuration/GenProduction/python/$FRAGMENT-fragment.py

rm -rf Configuration/GenProduction/python/.sys*

function source_cmssw() {
  CMSSW_VERSION=$1
  export SCRAM_ARCH=$2
  source /cvmfs/cms.cern.ch/cmsset_default.sh
  if [ -r $CMSSW_VERSION/src ] ; then
    echo release $CMSSW_VERSION already exists
  else
    scram p CMSSW $CMSSW_VERSION
  fi
  cd $CMSSW_VERSION/src
  eval `scram runtime -sh`

  if [ -d ../../Configuration ]; then
    mv ../../Configuration .
  fi
  scram b -j 4
  cd ../..
}

# cmsDriver command  
source_cmssw CMSSW_14_0_19 el8_amd64_gcc12
cmsDriver.py Configuration/GenProduction/python/${FRAGMENT}-fragment.py \
    --python_filename Run3Summer24wmLHEGS-00067_1_cfg.py \
    --eventcontent RAWSIM,LHE \
    --customise Configuration/DataProcessing/Utils.addMonitoring \
    --datatier GEN-SIM,LHE \
    --fileout file:${SAMPLE}-wmLHEGS.root \
    --conditions 140X_mcRun3_2024_realistic_v26 \
    --beamspot DBrealistic \
    --customise_commands process.RandomNumberGeneratorService.externalLHEProducer.initialSeed="int(${SEED})" \
    --step LHE,GEN,SIM \
    --geometry DB:Extended \
    --era Run3_2024 \
    --no_exec \
    --mc -n $EVENTS || exit $? ;

cmsRun --numThreads ${NUMTHREADS} Run3Summer24wmLHEGS-00067_1_cfg.py || exit $? ;

PILEUP_FILES=$(shuf -n 10 /eos/user/a/aaarora/pileup24.txt | tr '\n' ',' | sed 's/,$//')

source_cmssw CMSSW_14_0_21 el8_amd64_gcc12
cmsDriver.py \
  --python_filename Run3Summer24DRPremix-00048_1_cfg.py \
  --eventcontent PREMIXRAW \
  --customise Configuration/DataProcessing/Utils.addMonitoring \
  --datatier GEN-SIM-RAW \
  --fileout file:${SAMPLE}-DRPremix.root \
  --pileup_input $PILEUP_FILES \
  --conditions 140X_mcRun3_2024_realistic_v26 \
  --step DIGI,DATAMIX,L1,DIGI2RAW,HLT:2024v14 \
  --procModifiers premix_stage2 \
  --geometry DB:Extended \
  --filein file:${SAMPLE}-wmLHEGS.root \
  --datamix PreMix \
  --era Run3_2024 \
  --no_exec \
  --mc -n $EVENTS || exit $? ;

cmsRun --numThreads ${NUMTHREADS} Run3Summer24DRPremix-00048_1_cfg.py || exit $? ;

cmsDriver.py  \
  --python_filename Run3Summer24DRPremix-00048_2_cfg.py \
  --eventcontent AODSIM \
  --customise Configuration/DataProcessing/Utils.addMonitoring \
  --datatier AODSIM \
  --fileout file:${SAMPLE}-AOD.root \
  --conditions 140X_mcRun3_2024_realistic_v26 \
  --step RAW2DIGI,L1Reco,RECO,RECOSIM \
  --geometry DB:Extended \
  --filein "file:${SAMPLE}-DRPremix.root" \
  --era Run3_2024 \
  --no_exec \
  --mc -n $EVENTS || exit $? ;

cmsRun --numThreads ${NUMTHREADS} Run3Summer24DRPremix-00048_2_cfg.py || exit $? ;

source_cmssw CMSSW_15_0_2 el8_amd64_gcc12
# fix reweighting issue in cmssw
cd $CMSSW_BASE/src
git-cms-addpkg PhysicsTools/NanoAOD
wget http://uaf-4.t2.ucsd.edu/~legianni/forAashay/patch_CMSSW_15_0_2
git apply patch_CMSSW_15_0_2
rm patch_CMSSW_15_0_2
scram b -j ${NUMTHREADS}
cd ../..

cmsDriver.py  \
  --python_filename Run3Summer24MiniAODv6-00091_1_cfg.py \
  --eventcontent MINIAODSIM1 \
  --customise Configuration/DataProcessing/Utils.addMonitoring \
  --datatier MINIAODSIM \
  --fileout file:${SAMPLE}-MiniAODv6.root \
  --conditions 150X_mcRun3_2024_realistic_v2 \
  --step PAT \
  --geometry DB:Extended \
  --filein "file:${SAMPLE}-AOD.root" \
  --era Run3_2024 \
  --no_exec \
  --mc -n $EVENTS || exit $? ;

cmsRun --numThreads ${NUMTHREADS} Run3Summer24MiniAODv6-00091_1_cfg.py || exit $? ;

cmsDriver.py  \
  --python_filename Run3Summer24NanoAODv15-00091_1_cfg.py \
  --eventcontent NANOAODSIM1 \
  --customise Configuration/DataProcessing/Utils.addMonitoring \
  --datatier NANOAODSIM \
  --fileout file:${SAMPLE}-NanoAODv15.root \
  --conditions 150X_mcRun3_2024_realistic_v2 \
  --step NANO \
  --scenario pp \
  --filein "file:${SAMPLE}-MiniAODv6.root" \
  --era Run3_2024 \
  --no_exec \
  --mc -n $EVENTS || exit $? ;

cmsRun --numThreads ${NUMTHREADS} Run3Summer24NanoAODv15-00091_1_cfg.py || exit $? ;

# if output dir doesn't exist create it
OUTPUT_DIR="/eos/user/a/aaarora/signal/Run3Summer24/${SAMPLE}_TuneCP5"
if [ ! -d $OUTPUT_DIR ]; then
  mkdir -p $OUTPUT_DIR
fi

cp ${SAMPLE}-NanoAODv15.root ${OUTPUT_DIR}/$(uuidgen).root
