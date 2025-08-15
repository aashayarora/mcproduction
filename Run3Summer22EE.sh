#!/bin/bash
set -e

GRIDPACK=$1
SAMPLE=$2
EVENTS=${3:-10000}
NUMTHREADS=${4:-4}
FRAGMENT="GEN-Run3Summer22EEwmLHEGS-00046"
SEED=$(($(date +%s) % 100 + 1))

export X509_USER_PROXY=/afs/cern.ch/work/a/aaarora/x509up_u141045

mkdir -p $SAMPLE
cd $SAMPLE

curl -s -k https://cms-pdmv-prod.web.cern.ch/mcm/public/restapi/requests/get_fragment/$FRAGMENT --retry 3 --create-dirs -o Configuration/GenProduction/python/$FRAGMENT-fragment.py
[ -s Configuration/GenProduction/python/$FRAGMENT-fragment.py ] || exit $?;
# replace the gridpack in the fragment
sed -i "s|cms.vstring('/cvmfs/cms.cern.ch/phys_generator/gridpacks/PdmV/Run3Summer22/MadGraph5_aMCatNLO/DY/DYto2L-2Jets_MLL-50_amcatnloFXFX-pythia8_slc7_amd64_gcc10_CMSSW_12_4_8_tarball.tar.xz')|cms.vstring('$GRIDPACK')|g" Configuration/GenProduction/python/$FRAGMENT-fragment.py
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
source_cmssw CMSSW_12_4_11_patch3 el8_amd64_gcc10

cmsDriver.py Configuration/GenProduction/python/${FRAGMENT}-fragment.py \
FRAGMENT="GEN-Run3Summer22EEwmLHEGS-00046"
  --eventcontent RAWSIM,LHE \
  --customise Configuration/DataProcessing/Utils.addMonitoring \
  --datatier GEN-SIM,LHE \
  --fileout file:$SAMPLE-wmLHEGS.root \
  --conditions 124X_mcRun3_2022_realistic_postEE_v1 \
  --beamspot Realistic25ns13p6TeVEarly2022Collision \
  --customise_commands process.RandomNumberGeneratorService.externalLHEProducer.initialSeed="int(${SEED})" \
  --step LHE,GEN,SIM \
  --geometry DB:Extended \
  --era Run3 \
  --no_exec \
  --mc -n $EVENTS || exit $? ;

FRAGMENT="GEN-Run3Summer22EEwmLHEGS-00046"

PILEUP_FILES=$(shuf -n 10 /eos/user/a/aaarora/pileup22.txt | tr '\n' ',' | sed 's/,$//')

cmsDriver.py  \
FRAGMENT="GEN-Run3Summer22EEwmLHEGS-00046"
  --eventcontent PREMIXRAW \
  --customise Configuration/DataProcessing/Utils.addMonitoring \
  --datatier GEN-SIM-RAW \
  --fileout file:$SAMPLE-DRPremix.root \
  --pileup_input $PILEUP_FILES \
  --conditions 124X_mcRun3_2022_realistic_postEE_v1 \
  --step DIGI,DATAMIX,L1,DIGI2RAW,HLT:2022v14 \
  --procModifiers premix_stage2,siPixelQualityRawToDigi \
  --geometry DB:Extended \
  --filein file:$SAMPLE-wmLHEGS.root \
  --datamix PreMix \
  --era Run3 \
  --no_exec \
  --mc -n $EVENTS || exit $? ;

FRAGMENT="GEN-Run3Summer22EEwmLHEGS-00046"

cmsDriver.py  \
FRAGMENT="GEN-Run3Summer22EEwmLHEGS-00046"
  --eventcontent AODSIM \
  --customise Configuration/DataProcessing/Utils.addMonitoring \
  --datatier AODSIM \
  --fileout file:$SAMPLE-AOD.root \
  --conditions 124X_mcRun3_2022_realistic_postEE_v1 \
  --step RAW2DIGI,L1Reco,RECO,RECOSIM \
  --procModifiers siPixelQualityRawToDigi \
  --geometry DB:Extended \
  --filein "file:$SAMPLE-DRPremix.root" \
  --era Run3 \
  --no_exec \
  --mc -n $EVENTS || exit $? ;

FRAGMENT="GEN-Run3Summer22EEwmLHEGS-00046"

source_cmssw CMSSW_13_0_13 el8_amd64_gcc11
cmsDriver.py  \
FRAGMENT="GEN-Run3Summer22EEwmLHEGS-00046"
  --eventcontent MINIAODSIM \
  --customise Configuration/DataProcessing/Utils.addMonitoring \
  --datatier MINIAODSIM \
  --fileout file:$SAMPLE-MiniAODv4.root \
  --conditions 130X_mcRun3_2022_realistic_postEE_v6 \
  --step PAT \
  --geometry DB:Extended \
  --filein "file:$SAMPLE-AOD.root" \
  --era Run3,run3_miniAOD_12X \
  --no_exec \
  --mc -n $EVENTS || exit $? ;

FRAGMENT="GEN-Run3Summer22EEwmLHEGS-00046"

cmsDriver.py  \
FRAGMENT="GEN-Run3Summer22EEwmLHEGS-00046"
  --eventcontent NANOAODSIM \
  --customise Configuration/DataProcessing/Utils.addMonitoring \
  --datatier NANOAODSIM \
  --fileout file:$SAMPLE-NanoAODv12.root \
  --conditions 130X_mcRun3_2022_realistic_postEE_v6 \
  --step NANO \
  --scenario pp \
  --filein "file:$SAMPLE-MiniAODv4.root" \
  --era Run3 \
  --no_exec \
  --mc -n $EVENTS || exit $? ;

FRAGMENT="GEN-Run3Summer22EEwmLHEGS-00046"

# if output dir doesn't exist create it
if [ ! -d /eos/user/a/aaarora/signal/Run3Summer22EE/${SAMPLE}_TuneCP5 ]; then
  mkdir -p /eos/user/a/aaarora/signal/Run3Summer22EE/${SAMPLE}_TuneCP5
fi

cp ${SAMPLE}-NanoAODv12.root /eos/user/a/aaarora/signal/Run3Summer22EE/${SAMPLE}_TuneCP5/$(uuidgen).root
