#!/bin/bash
#SBATCH -p debug
#SBATCH -N 1 
#SBATCH -C haswell
#SBATCH -t 00:3:00
#SBATCH -o test_1_%j.txt
#SBATCH -e test_1_%j.err
#SBATCH -L SCRATCH
#SBATCH -A m844

RUNS=(1) # Number of runs
#SRCDIR=/global/cscratch1/sd/dqwu/e3sm_output_files/FC5AV1C-H01B_ne30_ne30_512p
SRCDIR=/global/cscratch1/sd/dqwu/e3sm_output_files/FC5AV1C-H01B_ne120_oRRS18v3_21632p
INDIR=/global/cscratch1/sd/khl7265/FS_64_1M/E3SM/realdata/
OUTDIR=/global/cscratch1/sd/khl7265/FS_64_1M/E3SM/origin/
CNKDIR=/global/cscratch1/sd/khl7265/FS_64_1M/E3SM/chunked/
ZLIBDIR=/global/cscratch1/sd/khl7265/FS_64_1M/E3SM/chunked/

NN=${SLURM_NNODES}
#let NP=NN*1
let NP=NN*32 

#CONFIG=/global/cscratch1/sd/khl7265/FS_64_1M/E3SM/decom/FC5AV1C-H01B_ne120_oRRS18v3_21600p.nc
CONFIG=datasets/f_case_777602x72_21600p.nc
NREC=1
CASE=0
TL=5

echo "mkdir -p ${OUTDIR}"
mkdir -p ${OUTDIR}
echo "mkdir -p ${CNKDIR}"
mkdir -p ${CNKDIR}
echo "mkdir -p ${ZLIBDIR}"
mkdir -p ${ZLIBDIR}
echo "mkdir -p ${INDIR}"
mkdir -p ${INDIR}

echo "rm -f ${INDIR}/*"
rm -f ${INDIR}/*

ln -s ${SRCDIR}/FC5AV1C-H01B_ne120_oRRS18v3_21632.cam.h0.0001-01-01-00000.nc ${INDIR}/f_case_h0_varn.nc 
ln -s ${SRCDIR}/FC5AV1C-H01B_ne120_oRRS18v3_21632.cam.h1.0001-01-01-00000.nc ${INDIR}/f_case_h1_varn.nc 
echo "ls -lah ${OUTDIR}"
ls -lah ${OUTDIR}

ulimit -c unlimited

TSTARTTIME=`date +%s.%N`

export PNETCDF_SHOW_PERFORMANCE_INFO=1

for i in ${RUNS[@]}
do
    echo "srun -n 1 -t 10 /global/homes/k/khl7265/.local/nc_zip/bin/ncmpidump /global/cscratch1/sd/dqwu/e3sm_output_files/FC5AV1C-H01B_ne120_oRRS18v3_21632p/FC5AV1C-H01B_ne120_oRRS18v3_21632.cam.h0.0001-01-01-00000.nc -h"
    srun -n 1 -t 10 /global/homes/k/khl7265/.local/nc_zip/bin/ncmpidump /global/cscratch1/sd/dqwu/e3sm_output_files/FC5AV1C-H01B_ne120_oRRS18v3_21632p/FC5AV1C-H01B_ne120_oRRS18v3_21632.cam.h0.0001-01-01-00000.nc -h

    exit 0

    # Ncmpio    
    echo "========================== ORIGINAL =========================="
    >&2 echo "========================== ORIGINAL =========================="

    echo "#%$: io_driver: ncmpi"
    echo "#%$: number_of_nodes: ${NN}"
    echo "#%$: number_of_proc: ${NP}"
    
    STARTTIME=`date +%s.%N`

    echo "srun -n ${NP} -t ${TL} ./e3sm_io -t ${NREC} -k -w -o ${CNKDIR} -r -i ${OUTDIR} -c ${CASE} ${CONFIG}" 
    srun -n ${NP} -t ${TL} ./e3sm_io -t ${NREC} -k -w -o ${CNKDIR} -r -i ${OUTDIR} -c ${CASE} ${CONFIG} 

    #echo "srun -n ${NP} -t ${TL} ./e3sm_io -t ${NREC} -k -w -o ${OUTDIR} -c ${CASE} ${CONFIG}" 
    #srun -n ${NP} -t ${TL} ./e3sm_io -t ${NREC} -k -w -o ${OUTDIR} -c ${CASE} ${CONFIG} 

    ENDTIME=`date +%s.%N`
    TIMEDIFF=`echo "$ENDTIME - $STARTTIME" | bc | awk -F"." '{print $1"."$2}'`

    echo "#%$: exe_time: $TIMEDIFF"

    echo "ls -lah ${OUTDIR}"
    ls -lah ${OUTDIR}
    echo "lfs getstripe ${OUTDIR}"
    lfs getstripe ${OUTDIR}
    
    echo '-----+-----++------------+++++++++--+---'
    
    exit 0

    # Nczipio    
    echo "========================== CHUNKED PROC =========================="
    >&2 echo "========================== CHUNKED PROC =========================="

    echo "#%$: io_driver: nczip_proc"
    echo "#%$: number_of_nodes: ${NN}"
    echo "#%$: number_of_proc: ${NP}"

    echo "rm -f ${OUTDIR}/*"
    rm -f ${OUTDIR}/*
   
    export PNETCDF_HINTS="nc_compression=enable;nc_zip_delay_init=1;nc_zip_driver=0"
    export PNETCDF_PROFILE_PREFIX="chunked"

    STARTTIME=`date +%s.%N`
    
    echo "srun -n ${NP} -t ${TL} ./e3sm_io -t ${NREC} -k -w -o ${OUTDIR} -c ${CASE} ${CONFIG}"
    srun -n ${NP} -t ${TL} ./e3sm_io -t ${NREC} -k -w -o ${OUTDIR} -c ${CASE} ${CONFIG} 

    ENDTIME=`date +%s.%N`
    TIMEDIFF=`echo "$ENDTIME - $STARTTIME" | bc | awk -F"." '{print $1"."$2}'`

    unset PNETCDF_HINTS

    echo "#%$: exe_time: $TIMEDIFF"

    echo "ls -lah ${OUTDIR}"
    ls -lah ${OUTDIR}
    echo "lfs getstripe ${OUTDIR}"
    lfs getstripe ${OUTDIR}
    
    echo '-----+-----++------------+++++++++--+---'
    
    # Nczipio    
    echo "========================== CHUNKED CHUNK=========================="
    >&2 echo "========================== CHUNKED CHUNK=========================="

    echo "#%$: io_driver: nczip_chunk"
    echo "#%$: number_of_nodes: ${NN}"
    echo "#%$: number_of_proc: ${NP}"

    echo "rm -f ${OUTDIR}/*"
    rm -f ${OUTDIR}/*
   
    export PNETCDF_HINTS="nc_compression=enable;nc_zip_delay_init=1;nc_zip_driver=0;nc_zip_comm_unit=chunk"
    export PNETCDF_PROFILE_PREFIX="chunked"

    STARTTIME=`date +%s.%N`
    
    #echo "srun -n ${NP} -t ${TL} ./e3sm_io -t ${NREC} -k -w -o ${OUTDIR} -c ${CASE} ${CONFIG}"
    #srun -n ${NP} -t ${TL} ./e3sm_io -t ${NREC} -k -w -o ${OUTDIR} -c ${CASE} ${CONFIG} 

    ENDTIME=`date +%s.%N`
    TIMEDIFF=`echo "$ENDTIME - $STARTTIME" | bc | awk -F"." '{print $1"."$2}'`

    unset PNETCDF_HINTS

    echo "#%$: exe_time: $TIMEDIFF"

    echo "ls -lah ${OUTDIR}"
    ls -lah ${OUTDIR}
    echo "lfs getstripe ${OUTDIR}"
    lfs getstripe ${OUTDIR}
    
    echo '-----+-----++------------+++++++++--+---'

    # Nczipio_zlib    
    echo "========================== ZLIB =========================="
    >&2 echo "========================== ZLIB =========================="

    echo "#%$: io_driver: nczip_zlib"
    echo "#%$: number_of_nodes: ${NN}"
    echo "#%$: number_of_proc: ${NP}"

    echo "rm -f ${OUTDIR}/*"
    rm -f ${OUTDIR}/*
    
    export PNETCDF_HINTS="nc_compression=enable;nc_zip_delay_init=1;nc_zip_driver=1"
    export PNETCDF_PROFILE_PREFIX="chunked_zlib"

    STARTTIME=`date +%s.%N`

    echo "srun -n ${NP} -t ${TL} ./e3sm_io -t ${NREC} -k -r -i ${INDIR} -w -o ${OUTDIR} -c ${CASE} ${CONFIG}"
    srun -n ${NP} -t ${TL} ./e3sm_io -t ${NREC} -k -r -i ${INDIR} -w -o ${OUTDIR} -c ${CASE} ${CONFIG} 

    ENDTIME=`date +%s.%N`
    TIMEDIFF=`echo "$ENDTIME - $STARTTIME" | bc | awk -F"." '{print $1"."$2}'`

    unset PNETCDF_HINTS
    
    echo "#%$: exe_time: $TIMEDIFF"

    echo "ls -lah ${OUTDIR}"
    ls -lah ${OUTDIR}
    echo "lfs getstripe ${OUTDIR}"
    lfs getstripe ${OUTDIR}
    
    echo '-----+-----++------------+++++++++--+---'
done

ENDTIME=`date +%s.%N`
TIMEDIFF=`echo "$ENDTIME - $TSTARTTIME" | bc | awk -F"." '{print $1"."$2}'`
echo "-------------------------------------------------------------"
echo "total_exe_time: $TIMEDIFF"
echo "-------------------------------------------------------------"

