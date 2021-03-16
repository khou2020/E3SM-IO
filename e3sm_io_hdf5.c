#include "e3sm_io_hdf5.h"
#include <stdlib.h>
#include <string.h>
#define ENABLE_MULTIDATASET 0
#define MULTIDATASET_DEFINE 0

// hid_t dxplid_nb  = -1;
hid_t dxplid_coll     = -1;
hid_t dxplid_indep    = -1;
hid_t dxplid_coll_nb  = -1;
hid_t dxplid_indep_nb = -1;
hsize_t one[H5S_MAX_RANK];
MPI_Offset mone[H5S_MAX_RANK];
static int f_ndim;
hsize_t f_dims[1048576];
hid_t f_dids[1048576];
int dataset_size;
int dataset_size_limit;
int dataspace_recycle_size;
int dataspace_recycle_size_limit;
int memspace_recycle_size;
int memspace_recycle_size_limit;
int hyperslab_count;
double hyperslab_time;

#if MULTIDATASET_DEFINE == 1
typedef struct H5D_rw_multi_t
{
    hid_t dset_id;          /* dataset ID */
    hid_t dset_space_id;    /* dataset selection dataspace ID */
    hid_t mem_type_id;      /* memory datatype ID */
    hid_t mem_space_id;     /* memory selection dataspace ID */
    union {
        void *rbuf;         /* pointer to read buffer */
        const void *wbuf;   /* pointer to write buffer */
    } u;
} H5D_rw_multi_t;
#endif
H5D_rw_multi_t *multi_datasets;
hid_t *memspace_recycle;
hid_t *dataspace_recycle;
static int f_nd;

#ifdef ENABLE_LOGVOL
hid_t log_vlid = -1;
#endif

hid_t nc_type_to_hdf5_type (nc_type nctype) {
    switch (nctype) {
        case NC_INT:
            return H5T_NATIVE_INT;
        case NC_FLOAT:
            return H5T_NATIVE_FLOAT;
        case NC_DOUBLE:
            return H5T_NATIVE_DOUBLE;
        case NC_CHAR:
            return H5T_NATIVE_CHAR;
        default:
            printf ("Error at line %d in %s: Unknown type %d\n", __LINE__, __FILE__, nctype);
    }

    return -1;
}

hid_t mpi_type_to_hdf5_type (MPI_Datatype mpitype) {
    switch (mpitype) {
        case MPI_INT:
            return H5T_NATIVE_INT;
        case MPI_FLOAT:
            return H5T_NATIVE_FLOAT;
        case MPI_DOUBLE:
            return H5T_NATIVE_DOUBLE;
        case MPI_CHAR:
            return H5T_NATIVE_CHAR;
        default:
            printf ("Error at line %d in %s: Unknown type %d\n", __LINE__, __FILE__, mpitype);
    }

    return -1;
}

double tsel, twrite, tread, text;
int hdf5_wrap_init () {
    herr_t herr = 0;
    int i;

    dxplid_coll = H5Pcreate (H5P_DATASET_XFER);
    CHECK_HID (dxplid_coll)
    herr = H5Pset_dxpl_mpio (dxplid_coll, H5FD_MPIO_COLLECTIVE);
    CHECK_HERR
    dxplid_coll_nb = H5Pcreate (H5P_DATASET_XFER);
    CHECK_HID (dxplid_coll_nb)
    herr = H5Pset_dxpl_mpio (dxplid_coll_nb, H5FD_MPIO_COLLECTIVE);
    CHECK_HERR

    dxplid_indep = H5Pcreate (H5P_DATASET_XFER);
    CHECK_HID (dxplid_indep)
    dxplid_indep_nb = H5Pcreate (H5P_DATASET_XFER);
    CHECK_HID (dxplid_indep_nb)
    // dxplid_nb = H5Pcreate (H5P_DATASET_XFER);
    // CHECK_HID (dxplid_nb)

#ifdef ENABLE_LOGVOL
    herr = H5Pset_nonblocking (dxplid_coll, H5VL_LOG_REQ_BLOCKING);
    CHECK_HERR
    herr = H5Pset_nonblocking (dxplid_indep, H5VL_LOG_REQ_BLOCKING);
    CHECK_HERR
    herr = H5Pset_nonblocking (dxplid_coll_nb, H5VL_LOG_REQ_NONBLOCKING);
    CHECK_HERR
    herr = H5Pset_nonblocking (dxplid_indep_nb, H5VL_LOG_REQ_NONBLOCKING);
    CHECK_HERR

    // Register LOG VOL plugin
    log_vlid = H5VLregister_connector (&H5VL_log_g, H5P_DEFAULT);
    CHECK_HID (log_vlid)
#endif

    for (i = 0; i < H5S_MAX_RANK; i++) {
        one[i]  = 1;
        mone[i] = 1;
    }
    dataset_size = 0;
    dataset_size_limit = 0;
    hyperslab_count = 0;
    hyperslab_time = .0;

    memspace_recycle_size = 0;
    memspace_recycle_size_limit = 0;

    dataspace_recycle_size = 0;
    dataspace_recycle_size_limit = 0;

    f_ndim = 0;
    f_nd   = 0;

    tsel = twrite = tread = text = 0;

fn_exit:;
    return (int)herr;
}

void hdf5_wrap_finalize () {
    int rank;
    double tsel_all, twrite_all, text_all;

    if (dxplid_coll >= 0) H5Pclose (dxplid_coll);
    if (dxplid_indep >= 0) H5Pclose (dxplid_indep);
    if (dxplid_coll_nb >= 0) H5Pclose (dxplid_coll_nb);
    if (dxplid_indep_nb >= 0) H5Pclose (dxplid_indep_nb);
    // if (dxplid_nb >= 0) H5Pclose (dxplid_nb);

    MPI_Allreduce (&twrite, &twrite_all, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
    MPI_Allreduce (&tsel, &tsel_all, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);
    MPI_Allreduce (&text, &text_all, 1, MPI_DOUBLE, MPI_MAX, MPI_COMM_WORLD);

    MPI_Comm_rank (MPI_COMM_WORLD, &rank);
    if (rank == 0) {
        printf ("H5Dwrite_time_max: %lf\n", twrite_all);
        printf ("H5Sselect_hyperslab_time_max: %lf\n", tsel_all);
        printf ("H5Dset_extent_time_max: %lf\n", text_all);
    }
}

int register_dataspace_recycle(hid_t dsid) {
    if (dataspace_recycle_size == dataspace_recycle_size_limit) {
        if ( dataspace_recycle_size_limit > 0 ) {
            dataspace_recycle_size_limit *= 2;
            hid_t *temp = (hid_t*) malloc(dataspace_recycle_size_limit*sizeof(hid_t));
            memcpy(temp, dataspace_recycle, sizeof(hid_t) * dataspace_recycle_size);
            free(dataspace_recycle);
            dataspace_recycle = temp;
        } else {
            dataspace_recycle_size_limit = 512;
            dataspace_recycle = (hid_t*) malloc(dataspace_recycle_size_limit*sizeof(hid_t));
        }
    }
    dataspace_recycle[dataspace_recycle_size] = dsid;
    dataspace_recycle_size++;
    return 0;
}

int register_memspace_recycle(hid_t msid) {
    if (memspace_recycle_size == memspace_recycle_size_limit) {
        if ( memspace_recycle_size_limit > 0 ) {
            memspace_recycle_size_limit *= 2;
            hid_t *temp = (hid_t*) malloc(memspace_recycle_size_limit*sizeof(hid_t));
            memcpy(temp, memspace_recycle, sizeof(hid_t) * memspace_recycle_size);
            free(memspace_recycle);
            memspace_recycle = temp;
        } else {
            memspace_recycle_size_limit = 512;
            memspace_recycle = (hid_t*) malloc(memspace_recycle_size_limit*sizeof(hid_t));
        }
    }
    memspace_recycle[memspace_recycle_size] = msid;
    memspace_recycle_size++;
    return 0;
}

int register_multidataset(void *buf, hid_t did, hid_t dsid, hid_t msid, hid_t mtype) {
    int ndim;
    if (dataset_size == dataset_size_limit) {
        if ( dataset_size_limit > 0 ) {
            dataset_size_limit *= 2;
            H5D_rw_multi_t *temp = (H5D_rw_multi_t*) malloc(dataset_size_limit*sizeof(H5D_rw_multi_t));
            memcpy(temp, multi_datasets, sizeof(H5D_rw_multi_t) * dataset_size);
            free(multi_datasets);
            multi_datasets = temp;
        } else {
            dataset_size_limit = 512;
            multi_datasets = (H5D_rw_multi_t*) malloc(dataset_size_limit*sizeof(H5D_rw_multi_t));
        }
    }

    multi_datasets[dataset_size].mem_space_id = msid;
    multi_datasets[dataset_size].dset_id = did;
    multi_datasets[dataset_size].dset_space_id = dsid;
    multi_datasets[dataset_size].mem_type_id = mtype;
    multi_datasets[dataset_size].u.wbuf = buf;
    dataset_size++;
    return 0;
}

int print_no_collective_cause(uint32_t local_no_collective_cause,uint32_t global_no_collective_cause) {
    switch (local_no_collective_cause) {
    case H5D_MPIO_COLLECTIVE: {
        printf("MPI-IO collective successful\n");
        break;
    }
    case H5D_MPIO_SET_INDEPENDENT: {
        printf("local flag: MPI-IO independent flag is on\n");
        break;
    }
    case H5D_MPIO_DATATYPE_CONVERSION  : {
        printf("local flag: MPI-IO datatype conversion needed\n");
        break;
    }
    case H5D_MPIO_DATA_TRANSFORMS: {
        printf("local flag: MPI-IO H5D_MPIO_DATA_TRANSFORMS.\n");
        break;
    }
/*
    case H5D_MPIO_SET_MPIPOSIX: {
        printf("local flag: MPI-IO H5D_MPIO_SET_MPIPOSIX \n");
    }
*/
    case H5D_MPIO_NOT_SIMPLE_OR_SCALAR_DATASPACES: {
        printf("local flag: MPI-IO NOT_SIMPLE_OR_SCALAR_DATASPACES\n");
        break;
    }
/*
    case H5D_MPIO_POINT_SELECTIONS: {
        printf("local flag: MPI-IO H5D_MPIO_POINT_SELECTIONS\n");
    }
*/
    case H5D_MPIO_NOT_CONTIGUOUS_OR_CHUNKED_DATASET: {
        printf("local flag: MPI-IO H5D_MPIO_NOT_CONTIGUOUS_OR_CHUNKED_DATASET\n");
        break;
    }
    case H5D_MPIO_FILTERS: {
        printf("local flag: MPI-IO H5D_MPIO_FILTERS\n");
        break;
    }
    default: {
        printf("undefined label for collective cause\n");
        break;
    }
    }

    switch (global_no_collective_cause) {
    case H5D_MPIO_COLLECTIVE: {
        printf("MPI-IO collective successful\n");
        break;
    }
    case H5D_MPIO_SET_INDEPENDENT: {
        printf("global flag: MPI-IO independent flag is on\n");
        break;
    }
    case H5D_MPIO_DATATYPE_CONVERSION  : {
        printf("global flag: MPI-IO datatype conversion needed\n");
        break;
    }
    case H5D_MPIO_DATA_TRANSFORMS: {
        printf("global flag: MPI-IO H5D_MPIO_DATA_TRANSFORMS.\n");
        break;
    }
/*
    case H5D_MPIO_SET_MPIPOSIX: {
        printf("global flag: MPI-IO H5D_MPIO_SET_MPIPOSIX \n");
    }
*/
    case H5D_MPIO_NOT_SIMPLE_OR_SCALAR_DATASPACES: {
        printf("global flag: MPI-IO NOT_SIMPLE_OR_SCALAR_DATASPACES\n");
        break;
    }
/*
    case H5D_MPIO_POINT_SELECTIONS: {
        printf("global flag: MPI-IO H5D_MPIO_POINT_SELECTIONS\n");
    }
*/
    case H5D_MPIO_NOT_CONTIGUOUS_OR_CHUNKED_DATASET: {
        printf("global flag: MPI-IO H5D_MPIO_NOT_CONTIGUOUS_OR_CHUNKED_DATASET\n");
        break;
    }
    case H5D_MPIO_FILTERS: {
        printf("global flag: MPI-IO H5D_MPIO_FILTERS\n");
        break;
    }
    default: {
        printf("undefined label for collective cause\n");
        break;
    }
    }
    return 0;
}

int flush_multidatasets() {
    int i;
    uint32_t local_no_collective_cause, global_no_collective_cause;
    //printf("Rank %d number of datasets to be written %d\n", rank, dataset_size);
#if ENABLE_MULTIDATASET==1
    hid_t plist_id = H5Pcreate(H5P_DATASET_XFER);
    H5Pset_dxpl_mpio(plist_id, H5FD_MPIO_COLLECTIVE);

    H5Dwrite_multi(plist_id, dataset_size, multi_datasets);

    H5Pclose(plist_id);
#else

    int rank;
    MPI_Comm_rank (MPI_COMM_WORLD, &rank);

    //printf("rank %d has dataset_size %lld\n", rank, (long long int) dataset_size);
    for ( i = 0; i < dataset_size; ++i ) {
        //MPI_Barrier(MPI_COMM_WORLD);
        if (rank == 0 ) {
            //printf("collective write at i = %d\n", i);
            switch(multi_datasets[i].mem_type_id) {
            case H5T_NATIVE_INT: {
                printf("getting int type\n");
                break;
            }
            case H5T_NATIVE_FLOAT: {
                printf("getting float type\n");
                break;
            }
            case H5T_NATIVE_DOUBLE: {
                printf("getting double type\n");
                break;
            }
            case H5T_NATIVE_CHAR: {
                printf("getting char type\n");
                break;
            }
            default: { printf("bad type\n");break;}
            }
        }

        H5Dwrite (multi_datasets[i].dset_id, multi_datasets[i].mem_type_id, multi_datasets[i].mem_space_id, multi_datasets[i].dset_space_id, dxplid_coll, multi_datasets[i].u.wbuf);
        if (!rank) {
        H5Pget_mpio_no_collective_cause( dxplid_coll, &local_no_collective_cause, &global_no_collective_cause);
        print_no_collective_cause(local_no_collective_cause, global_no_collective_cause);
        }
    }
#endif
    //printf("number of hyperslab called %d\n", hyperslab_count);

    if (dataset_size) {
        free(multi_datasets);
    }
    dataset_size = 0;
    dataset_size_limit = 0;
    return 0;
}

int dataspace_recycle_all() {
    int i;
    //printf("recycle %d dataspace\n", dataspace_recycle_size);
    for ( i = 0; i < dataspace_recycle_size; ++i ) {
        if ( dataspace_recycle[i] >= 0 ) {
            H5Sclose(dataspace_recycle[i]);
        }
    }
    if (dataspace_recycle_size) {
        free(dataspace_recycle);
    }
    dataspace_recycle_size = 0;
    return 0;
}

int memspace_recycle_all() {
    int i;
    //printf("recycle %d memspace\n", memspace_recycle_size);
    for ( i = 0; i < memspace_recycle_size; ++i ) {
        if ( memspace_recycle[i] >= 0 ){
            H5Sclose(memspace_recycle[i]);
        }
    }
    if (memspace_recycle_size) {
        free(memspace_recycle);
    }
    memspace_recycle_size = 0;
    return 0;
}

int hdf5_put_vara (
    int vid, hid_t mtype, hid_t dxplid, MPI_Offset *mstart, MPI_Offset *mcount, void *buf) {
    herr_t herr = 0;
    int i;
    int ndim = -1;
    double ts, te;
    hid_t dsid = -1, msid = -1;
    hid_t did;
    hsize_t start[H5S_MAX_RANK], block[H5S_MAX_RANK];
    hsize_t dims[H5S_MAX_RANK];

    did = f_dids[vid];

    dsid = H5Dget_space (did);
    CHECK_HID (dsid)

    ndim = H5Sget_simple_extent_dims (dsid, dims, NULL);
    CHECK_HID (ndim)
/*
    for ( i = 0; i < ndim; ++i ) {
        printf("ndim = %d, dims[%d] = %lld\n", ndim, i,(long long int) dims[i]);
    }
*/
    for (i = 0; i < ndim; i++) {
        start[i] = (hsize_t)mstart[i];
        block[i] = (hsize_t)mcount[i];
    }

    // Extend rec dim
    ts = MPI_Wtime ();
    if (dims[0] < start[0] + block[0]) {
        dims[0] = start[0] + block[0];

        H5Sclose (dsid);
        //printf("put vara checkpoint 0, ndim = %d, dims[0] = %lld, sizeof(dims) = %d, vid = %d\n", ndim, (long long int) dims[0], H5S_MAX_RANK, vid);
        herr = H5Dset_extent (did, dims);
        //printf("put vara checkpoint 1\n");
        CHECK_HERR
        dsid = H5Dget_space (did);
        CHECK_HID (dsid)
    }
    text = MPI_Wtime () - ts;
#ifndef ENABLE_LOGVOL
    msid = H5Screate_simple (ndim, block, block);
    CHECK_HID (msid)
#endif

    ts   = MPI_Wtime ();
    herr = H5Sselect_hyperslab (dsid, H5S_SELECT_SET, start, NULL, one, block);
    CHECK_HERR
    te = MPI_Wtime ();
    tsel += te - ts;
#ifdef ENABLE_LOGVOL
    herr = H5Dwrite (did, mtype, H5S_CONTIG, dsid, dxplid, buf);
    CHECK_HERR
#else
    herr = H5Dwrite (did, mtype, msid, dsid, dxplid, buf);
    CHECK_HERR
#endif
    twrite += MPI_Wtime () - te;

fn_exit:;

    if (dsid >= 0) H5Sclose (dsid);
#ifndef ENABLE_LOGVOL
    if (msid >= 0) H5Sclose (msid);
#endif
    return (int)herr;
}

int hdf5_put_vara_mpi (
    int vid, hid_t mtype, hid_t dxplid, MPI_Offset *mstart, MPI_Offset *mcount, void *buf, int rank) {
    herr_t herr = 0;
    int i;
    int ndim = -1;
    double ts, te;
    hid_t dsid = -1, msid = -1;
    hid_t did;
    hsize_t start[H5S_MAX_RANK], block[H5S_MAX_RANK];
    hsize_t dims[H5S_MAX_RANK];

    did = f_dids[vid];

    dsid = H5Dget_space (did);
    CHECK_HID (dsid)

    ndim = H5Sget_simple_extent_dims (dsid, dims, NULL);
    CHECK_HID (ndim)
/*
    for ( i = 0; i < ndim; ++i ) {
        printf("ndim = %d, dims[%d] = %lld\n", ndim, i,(long long int) dims[i]);
    }
*/
    for (i = 0; i < ndim; i++) {
        start[i] = (hsize_t)mstart[i];
        block[i] = (hsize_t)mcount[i];
    }

    // Extend rec dim
    ts = MPI_Wtime ();
    if (dims[0] < start[0] + block[0]) {
        dims[0] = start[0] + block[0];
        H5Sclose (dsid);
        //printf("rank %d put vara checkpoint 0, ndim = %d, dims[0] = %lld, sizeof(dims) = %d, vid = %d\n", rank, ndim, (long long int) dims[0], H5S_MAX_RANK, vid);
        herr = H5Dset_extent (did, dims);
        //printf("put vara checkpoint 1\n");
        CHECK_HERR
        dsid = H5Dget_space (did);
        CHECK_HID (dsid)
    }
    text = MPI_Wtime () - ts;
#ifndef ENABLE_LOGVOL
    msid = H5Screate_simple (ndim, dims, dims);
    if ( rank != 0 ) {
        for (i = 0; i < ndim; i++) {
            block[i] = 0;
        }
    }
    herr = H5Sselect_hyperslab (msid, H5S_SELECT_SET, start, NULL, one, block);
    herr = H5Sselect_hyperslab (dsid, H5S_SELECT_SET, start, NULL, one, block);
    CHECK_HID (msid)
#endif

    ts   = MPI_Wtime ();
#ifndef ENABLE_LOGVOL
    herr = H5Sselect_hyperslab (dsid, H5S_SELECT_SET, start, NULL, one, block);
#endif
    CHECK_HERR
    te = MPI_Wtime ();
    tsel += te - ts;

#ifdef ENABLE_LOGVOL
    herr = H5Dwrite (did, mtype, H5S_CONTIG, dsid, dxplid, buf);
    CHECK_HERR
#else
    //herr = H5Dwrite (did, mtype, msid, dsid, dxplid, buf);
    //herr = H5Dwrite (did, mtype, msid, dsid, dxplid_coll, buf);
    //CHECK_HERR
    register_dataspace_recycle(dsid);
    register_memspace_recycle(msid);
    register_multidataset(buf, did, dsid, msid, mtype);
#endif
    twrite += MPI_Wtime () - te;

fn_exit:;
/*
    if (dsid >= 0) H5Sclose (dsid);
#ifndef ENABLE_LOGVOL
    if (msid >= 0) H5Sclose (msid);
#endif
*/
    return (int)herr;
}

int hdf5_put_vars (int vid,
                   hid_t mtype,
                   hid_t dxplid,
                   MPI_Offset *mstart,
                   MPI_Offset *mcount,
                   MPI_Offset *mstride,
                   void *buf) {
    herr_t herr = 0;
    int i;
    int ndim = -1;
    double ts, te;
    hid_t dsid = -1, msid = -1;
    hid_t did;
    hsize_t start[H5S_MAX_RANK], block[H5S_MAX_RANK], stride[H5S_MAX_RANK];
    hsize_t dims[H5S_MAX_RANK];

    did = f_dids[vid];

    dsid = H5Dget_space (did);
    CHECK_HID (dsid)

    ndim = H5Sget_simple_extent_dims (dsid, dims, NULL);
    CHECK_HID (ndim)

    for (i = 0; i < ndim; i++) {
        start[i]  = (hsize_t)mstart[i];
        block[i]  = (hsize_t)mcount[i];
        stride[i] = (hsize_t)mstride[i];
    }

    // Extend rec dim
    ts = MPI_Wtime ();
    if (dims[0] < start[0] + block[0]) {
        dims[0] = start[0] + (block[0] - 1) * stride[0] + 1;

        H5Sclose (dsid);
        herr = H5Dset_extent (did, dims);
        CHECK_HERR
        dsid = H5Dget_space (did);
        CHECK_HID (dsid)
    }
    text = MPI_Wtime () - ts;

#ifndef ENABLE_LOGVOL
    msid = H5Screate_simple (ndim, block, block);
    CHECK_HID (msid)
#endif

    ts   = MPI_Wtime ();
    herr = H5Sselect_hyperslab (dsid, H5S_SELECT_SET, start, stride, one, block);
    CHECK_HERR
    te = MPI_Wtime ();
    tsel += te - ts;
#ifdef ENABLE_LOGVOL
    herr = H5Dwrite (did, mtype, H5S_CONTIG, dsid, dxplid, buf);
    CHECK_HERR
#else
    herr = H5Dwrite (did, mtype, msid, dsid, dxplid, buf);
    CHECK_HERR
#endif
    twrite += MPI_Wtime () - te;

fn_exit:;
    if (dsid >= 0) H5Sclose (dsid);
#ifndef ENABLE_LOGVOL
    if (msid >= 0) H5Sclose (msid);
#endif
    return (int)herr;
}

int hdf5_put_var1 (int vid, hid_t mtype, hid_t dxplid, MPI_Offset *mstart, void *buf) {
    return hdf5_put_vara (vid, mtype, dxplid, mstart, mone, buf);
}

int hdf5_put_var1_mpi (int vid, hid_t mtype, hid_t dxplid, MPI_Offset *mstart, void *buf, int rank) {
    return hdf5_put_vara_mpi (vid, mtype, dxplid, mstart, mone, buf, rank);
}

#ifdef ENABLE_LOGVOL
#define USE_LOGVOL_VARN
#endif

#ifdef USE_LOGVOL_VARN
int hdf5_put_varn (int vid,
                   MPI_Datatype mpitype,
                   hid_t dxplid,
                   int cnt,
                   MPI_Offset **mstarts,
                   MPI_Offset **mcounts,
                   void *buf) {
    int err;
    herr_t herr = 0;
    int i, j;
    double ts, te;
    hsize_t esize, rsize, rsize_old = 0;
    int ndim;
    hid_t dsid = -1, msid = -1;
    hid_t mtype;
    char *bufp = buf;
    hid_t did;
    hsize_t start[H5S_MAX_RANK], block[H5S_MAX_RANK];
    hsize_t dims[H5S_MAX_RANK], mdims[H5S_MAX_RANK];

    did = f_dids[vid];

    mtype = mpi_type_to_hdf5_type (mpitype);
    CHECK_HID (mtype)

    dsid = H5Dget_space (did);
    CHECK_HID (dsid)

    ndim = H5Sget_simple_extent_dims (dsid, dims, mdims);
    CHECK_HID (ndim)

    // Extend rec dim if needed
    ts = MPI_Wtime ();
    if (ndim && mdims[0] == H5S_UNLIMITED) {
        MPI_Offset max_rec = 0;
        for (i = 0; i < cnt; i++) {
            if (max_rec < mstarts[i][0] + mcounts[i][0]) {
                max_rec = mstarts[i][0] + mcounts[i][0];
            }
        }
        if (dims[0] < (hsize_t)max_rec) {
            dims[0] = (hsize_t)max_rec;

            H5Sclose (dsid);
            herr = H5Dset_extent (did, dims);
            CHECK_HERR
            dsid = H5Dget_space (did);
            CHECK_HID (dsid)
        }
    }
    text += MPI_Wtime () - ts;

    // Call H5Dwriten
    te   = MPI_Wtime ();
    //herr = H5Dwrite (did, mtype, H5S_CONTIG, dsid, dxplid, bufp);
    //CHECK_HERR
    herr = H5Dwriten (did, mtype, cnt, mstarts, mcounts, dxplid, buf);
    CHECK_HERR
    twrite += MPI_Wtime () - te;

fn_exit:;
    if (dsid >= 0) H5Sclose (dsid);

    return (int)herr;
}
#else
int hdf5_put_varn (int vid,
                   MPI_Datatype mpitype,
                   hid_t dxplid,
                   int cnt,
                   MPI_Offset **mstarts,
                   MPI_Offset **mcounts,
                   void *buf) {
    int err;
    herr_t herr = 0;
    int i, j;
    double ts, te;
    hsize_t esize, rsize, rsize_old = 0, memspace_size;
    int ndim;
    hid_t dsid = -1, msid = -1;
    hid_t mtype;
    char *bufp = buf;
    hid_t did;
    hsize_t start[H5S_MAX_RANK], block[H5S_MAX_RANK];
    hsize_t dims[H5S_MAX_RANK], mdims[H5S_MAX_RANK];

    did = f_dids[vid];

    mtype = mpi_type_to_hdf5_type (mpitype);
    esize = (hsize_t)H5Tget_size (mtype);
    CHECK_HID (esize)

    dsid = H5Dget_space (did);
    CHECK_HID (dsid)

    ndim = H5Sget_simple_extent_dims (dsid, dims, mdims);
    CHECK_HID (ndim)

    // Extend rec dim if needed
    ts = MPI_Wtime ();
    if (ndim && mdims[0] == H5S_UNLIMITED) {
        MPI_Offset max_rec = 0;
        for (i = 0; i < cnt; i++) {
            if (max_rec < mstarts[i][0] + mcounts[i][0]) {
                max_rec = mstarts[i][0] + mcounts[i][0];
            }
        }
        if (dims[0] < (hsize_t)max_rec) {
            dims[0] = (hsize_t)max_rec;

            H5Sclose (dsid);
            herr = H5Dset_extent (did, dims);
            CHECK_HERR
            dsid = H5Dget_space (did);
            CHECK_HID (dsid)
        }
    }
    text += MPI_Wtime () - ts;

    // Call H5DWrite
    int rank;
    MPI_Comm_rank (MPI_COMM_WORLD, &rank);
    printf("rank %d has cnt = %d\n", rank, cnt);
    for (i = 0; i < cnt; i++) {
        rsize = esize;
        for (j = 0; j < ndim; j++) { rsize *= mcounts[i][j]; }
        memspace_size = rsize / esize;

        if (rsize) {
            // err = hdf5_put_vara (vid, mtype, dxplid, mstarts[i], mcounts[i], bufp);
            // CHECK_ERR

            for (j = 0; j < ndim; j++) {
                start[j] = (hsize_t)mstarts[i][j];
                block[j] = (hsize_t)mcounts[i][j];
            }

#ifndef ENABLE_LOGVOL
            // Recreate only when size mismatch
            if (rsize != rsize_old) {
                if (msid >= 0) H5Sclose (msid);
                msid = H5Screate_simple (1, &memspace_size, &memspace_size);
                CHECK_HID (msid)

                rsize_old = rsize;
            }
#endif

            ts = MPI_Wtime ();
            herr = H5Sselect_hyperslab (dsid, H5S_SELECT_SET, start, NULL, one, block);
            CHECK_HERR
            te = MPI_Wtime ();
            tsel += te - ts;
#ifdef ENABLE_LOGVOL
            herr = H5Dwrite (did, mtype, H5S_CONTIG, dsid, dxplid, bufp);
            CHECK_HERR
#else
            //herr = H5Dwrite (did, mtype, msid, dsid, dxplid, bufp);
            herr = H5Dwrite (did, mtype, msid, dsid, dxplid_coll, bufp);

            CHECK_HERR
#endif
            twrite += MPI_Wtime () - te;
            bufp += rsize;
        }
    }
fn_exit:;
    if (dsid >= 0) H5Sclose (dsid);
#ifndef ENABLE_LOGVOL
    if (msid >= 0) H5Sclose (msid);
#endif
    return (int)herr;
}

typedef struct Index_order{
    hsize_t index;
    hsize_t coverage;
    char *data;
} Index_order;

int index_order_cmp (const void *a, const void *b) {
    return ( ((Index_order *)a)->index - ((Index_order *)b)->index);
}

int count_data(int cnt, int ndim, MPI_Offset **blocks, int *index) {
    int j, k;
    hsize_t rsize;
    index[0] = 0;
    for ( k = 0; k < cnt; ++k ) {
        rsize = 1;
        for (j = 0; j < ndim; j++) { rsize *= blocks[k][j]; }
        if (rsize) {
            if (ndim == 1) {
                index[0]++;
            } else if (ndim == 2) {
                index[0] += blocks[k][0];
            } else if (ndim == 3) {
                index[0] += blocks[k][0] * blocks[k][1];
            } else {
                printf("critical error, dimension is greater than 3.\n");
                return -1;
            }
        }
    }
    return 0;
}

int pack_data(Index_order *index_order, int *index, char* src, hsize_t esize, int ndim, hsize_t *dims, hsize_t *start, hsize_t *block) {
    int i, j;
    hsize_t size_copied = 0;
    if ( ndim == 1 ) {
        index_order[index[0]].index = start[0];
        index_order[index[0]].coverage = esize * block[0];
        index_order[index[0]].data = src;
        index[0]++;
    } else if (ndim == 2) {
        for ( i = 0; i < block[0]; ++i ) {
            index_order[index[0]].index = start[1] + start[0] * dims[1];
            index_order[index[0]].coverage = esize * block[1];
            index_order[index[0]].data = src;
            src += index_order[index[0]].coverage;
            index[0]++;
        }
    } else if (ndim == 3) {
        for ( i = 0; i < block[0]; ++i ) {
            for ( j = 0; j < block[1]; ++j ) {
                index_order[index[0]].index = start[0] * dims[1] * dims[2] + start[1] * dims[2] + start[2];
                index_order[index[0]].coverage = esize * block[2];
                index_order[index[0]].data = src;
                src += index_order[index[0]].coverage;
                index[0]++;
            }
        }
    } else {
        printf("critical error, dimension is greater than 3.\n");
        return -1;
    }
}

int copy_index_buf(Index_order *index_order, int total_blocks, char *out_buf) {
    hsize_t displs = 0;
    int i;
    for ( i = 0; i < total_blocks; ++i ) {
        memcpy(out_buf + displs, index_order[i].data, index_order[i].coverage);
        displs += index_order[i].coverage;
    }
}


int hdf5_put_varn_mpi (int vid,
                   MPI_Datatype mpitype,
                   hid_t dxplid,
                   int cnt,
                   int max_cnt,
                   MPI_Offset **mstarts,
                   MPI_Offset **mcounts,
                   void *buf) {
    int err;
    herr_t herr = 0;
    int i, j;
    double ts, te;
    hsize_t esize, rsize, rsize_old = 0, memspace_size, total_memspace_size, hyperslab_set;
    int ndim;
    hid_t dsid = -1, msid = -1;
    hid_t mtype;
    char *bufp = buf;
    hid_t did;
    hsize_t start[H5S_MAX_RANK], block[H5S_MAX_RANK];
    hsize_t dims[H5S_MAX_RANK], mdims[H5S_MAX_RANK];

    char *buf2;
    int index;
    Index_order *index_order;
    int total_blocks;

    did = f_dids[vid];
/*
    if (cnt == 0) {
        memspace_size = 0;
        msid = H5Screate_simple (1, &memspace_size, &memspace_size);
        CHECK_HID (msid)
        register_dataspace_recycle(msid);
        register_multidataset(NULL, did, msid, msid, mtype);
        return 0;
    }
*/

    mtype = mpi_type_to_hdf5_type (mpitype);
    esize = (hsize_t)H5Tget_size (mtype);
    CHECK_HID (esize)

    dsid = H5Dget_space (did);
    CHECK_HID (dsid)

    ndim = H5Sget_simple_extent_dims (dsid, dims, mdims);
    CHECK_HID (ndim)
    // Extend rec dim if needed
    ts = MPI_Wtime ();
    if (ndim && mdims[0] == H5S_UNLIMITED) {
        MPI_Offset max_rec = 0;
        for (i = 0; i < cnt; i++) {
            if (max_rec < mstarts[i][0] + mcounts[i][0]) {
                max_rec = mstarts[i][0] + mcounts[i][0];
            }
        }
        if (dims[0] < (hsize_t)max_rec) {
            dims[0] = (hsize_t)max_rec;

            H5Sclose (dsid);
            herr = H5Dset_extent (did, dims);
            CHECK_HERR
            dsid = H5Dget_space (did);
            CHECK_HID (dsid)
        }

    }
    text += MPI_Wtime () - ts;

    ndim = H5Sget_simple_extent_dims (dsid, dims, mdims);

    count_data(cnt, ndim, mcounts, &total_blocks);
    index_order = (Index_order*) malloc(sizeof(Index_order) * total_blocks);
    index = 0;

    register_dataspace_recycle(dsid);
    herr = H5Sselect_hyperslab (dsid, H5S_SELECT_SET, start, NULL, one, block);
    // Call H5DWrite
    int rank;
    MPI_Comm_rank (MPI_COMM_WORLD, &rank);
    hyperslab_set = 0;
    total_memspace_size = 0;
    for (i = 0; i < cnt; i++) {
        rsize = esize;
        for (j = 0; j < ndim; j++) { rsize *= mcounts[i][j]; }
        memspace_size = rsize / esize;
        total_memspace_size += memspace_size;
        if (rsize) {
            // err = hdf5_put_vara (vid, mtype, dxplid, mstarts[i], mcounts[i], bufp);
            // CHECK_ERR

            for (j = 0; j < ndim; j++) {
                start[j] = (hsize_t)mstarts[i][j];
                block[j] = (hsize_t)mcounts[i][j];
            }

#ifndef ENABLE_LOGVOL
            // Recreate only when size mismatch
            if (rsize != rsize_old) {
                //if (msid >= 0) H5Sclose (msid);
                //msid = H5Screate_simple (1, &memspace_size, &memspace_size);
                //CHECK_HID (msid)
                //register_memspace_recycle(msid);
                rsize_old = rsize;
            }
#endif
            ts = MPI_Wtime ();
            if (!hyperslab_set) {
                herr = H5Sselect_hyperslab (dsid, H5S_SELECT_SET, start, NULL, one, block);
                hyperslab_set = 1;
            } else {
                herr = H5Sselect_hyperslab (dsid, H5S_SELECT_OR, start, NULL, one, block);
            }
            CHECK_HERR
            te = MPI_Wtime ();
            tsel += te - ts;
#ifdef ENABLE_LOGVOL
            herr = H5Dwrite (did, mtype, H5S_CONTIG, dsid, dxplid, bufp);
            CHECK_HERR
#else
            //herr = H5Dwrite (did, mtype, msid, dsid, dxplid, bufp);
            //herr = H5Dwrite (did, mtype, msid, dsid, dxplid_indep, bufp);
            //CHECK_HERR

            pack_data(index_order, &index, bufp, esize, ndim, dims, start, block);
            hyperslab_count++;

#endif
            twrite += MPI_Wtime () - te;
            bufp += rsize;
        }
    }

    qsort(index_order, total_blocks, sizeof(Index_order), index_order_cmp);
    buf2 = (char*) malloc(esize * total_memspace_size);
    copy_index_buf(index_order, total_blocks, buf2);
    memcpy(buf, buf2, esize * total_memspace_size);
    free(index_order);
    free(buf2);

    msid = H5Screate_simple (1, &total_memspace_size, &total_memspace_size);
    CHECK_HID (msid)
    register_memspace_recycle(msid);
    register_multidataset(buf, did, dsid, msid, mtype);
    /* The folowing code is to place dummy H5Dwrite for collective call.*/

    //if (msid >= 0) H5Sclose (msid);

fn_exit:;
/*
    if (dsid >= 0) H5Sclose (dsid);
#ifndef ENABLE_LOGVOL
    if (msid >= 0) H5Sclose (msid);
#endif
*/
    return (int)herr;
}
#endif

int hdf5_get_vara (
    int vid, hid_t mtype, hid_t dxplid, MPI_Offset *mstart, MPI_Offset *mcount, void *buf) {
    herr_t herr = 0;
    int i;
    double ts, te;
    int ndim   = -1;
    hid_t dsid = -1, msid = -1;
    hid_t did;
    hsize_t start[H5S_MAX_RANK], block[H5S_MAX_RANK];

    did = f_dids[vid];

    dsid = H5Dget_space (did);
    CHECK_HID (dsid)

    ndim = H5Sget_simple_extent_ndims (dsid);
    CHECK_HID (ndim)

    for (i = 0; i < ndim; i++) {
        start[i] = (hsize_t)mstart[i];
        block[i] = (hsize_t)mcount[i];
    }

#ifndef ENABLE_LOGVOL
    msid = H5Screate_simple (ndim, block, block);
    CHECK_HID (msid)
#endif

    ts   = MPI_Wtime ();
    herr = H5Sselect_hyperslab (dsid, H5S_SELECT_SET, start, NULL, one, block);
    CHECK_HERR
    te = MPI_Wtime ();
    tsel += te - ts;
#ifdef ENABLE_LOGVOL
    herr = H5Dread (did, mtype, H5S_CONTIG, dsid, dxplid, buf);
    CHECK_HERR
#else
    herr = H5Dread (did, mtype, msid, dsid, dxplid, buf);
    CHECK_HERR
#endif
    tread += MPI_Wtime () - te;

fn_exit:;
    if (dsid >= 0) H5Sclose (dsid);
#ifndef ENABLE_LOGVOL
    if (msid >= 0) H5Sclose (msid);
#endif
    return (int)herr;
}

int hdf5_get_vars (int vid,
                   hid_t mtype,
                   hid_t dxplid,
                   MPI_Offset *mstart,
                   MPI_Offset *mcount,
                   MPI_Offset *mstride,
                   void *buf) {
    herr_t herr = 0;
    int i;
    double ts, te;
    int ndim   = -1;
    hid_t dsid = -1, msid = -1;
    hid_t did;
    hsize_t start[H5S_MAX_RANK], block[H5S_MAX_RANK], stride[H5S_MAX_RANK];

    did = f_dids[vid];

    dsid = H5Dget_space (did);
    CHECK_HID (dsid)

    ndim = H5Sget_simple_extent_ndims (dsid);
    CHECK_HID (ndim)

    for (i = 0; i < ndim; i++) {
        start[i]  = (hsize_t)mstart[i];
        block[i]  = (hsize_t)mcount[i];
        stride[i] = (hsize_t)mstride[i];
    }

#ifndef ENABLE_LOGVOL
    msid = H5Screate_simple (ndim, block, block);
    CHECK_HID (msid)
#endif

    ts   = MPI_Wtime ();
    herr = H5Sselect_hyperslab (dsid, H5S_SELECT_SET, start, stride, one, block);
    CHECK_HERR
    te = MPI_Wtime ();
    tsel += te - ts;
#ifdef ENABLE_LOGVOL
    herr = H5Dread (did, mtype, H5S_CONTIG, dsid, dxplid, buf);
    CHECK_HERR
#else
    herr = H5Dread (did, mtype, msid, dsid, dxplid, buf);
    CHECK_HERR
#endif
    tread += MPI_Wtime () - te;

fn_exit:;
    if (dsid >= 0) H5Sclose (dsid);
#ifndef ENABLE_LOGVOL
    if (msid >= 0) H5Sclose (msid);
#endif
    return (int)herr;
}

int hdf5_get_var1 (int vid, hid_t mtype, hid_t dxplid, MPI_Offset *mstart, void *buf) {
    return hdf5_get_vara (vid, mtype, dxplid, mstart, mone, buf);
}

int hdf5_get_varn (int vid,
                   MPI_Datatype mpitype,
                   hid_t dxplid,
                   int cnt,
                   MPI_Offset **mstarts,
                   MPI_Offset **mcounts,
                   void *buf) {
    int err;
    herr_t herr = 0;
    int i, j;
    size_t esize, rsize;
    int ndim;
    hid_t dsid = -1;
    hid_t mtype;
    hid_t did;
    char *bufp = buf;

    did = f_dids[vid];

    mtype = mpi_type_to_hdf5_type (mpitype);

    dsid = H5Dget_space (did);
    CHECK_HID (dsid)

    ndim = H5Sget_simple_extent_ndims (dsid);
    CHECK_HID (ndim)

    esize = H5Tget_size (mtype);
    CHECK_HID (esize)
    for (i = 0; i < cnt; i++) {
        err = hdf5_get_vara (vid, mtype, dxplid, mstarts[i], mcounts[i], bufp);
        CHECK_ERR
        rsize = esize;
        for (j = 0; j < ndim; j++) { rsize *= mcounts[i][j]; }
        bufp += rsize;
    }

fn_exit:;
    if (dsid >= 0) H5Sclose (dsid);

    return (int)herr;
}

int hdf5_put_att (
    hid_t fid, int vid, const char *name, hid_t atype, MPI_Offset nelems, const void *buf) {
    herr_t herr = 0;
    hid_t asid = -1, aid = -1;
    hid_t did;
    hsize_t asize;
    htri_t exists;

    asize = (size_t)nelems;
    asid  = H5Screate_simple (1, &asize, &asize);
    CHECK_HID (asid)

    if (vid == NC_GLOBAL)
        did = fid;
    else
        did = f_dids[vid];

    exists = H5Aexists (did, name);
    CHECK_HID (exists)
    if (!exists) {
        aid = H5Acreate2 (did, name, atype, asid, H5P_DEFAULT, H5P_DEFAULT);
    } else {
        aid = H5Aopen (did, name, H5P_DEFAULT);
    }
    CHECK_HID (aid)

    herr = H5Awrite (aid, atype, buf);
    CHECK_HERR

fn_exit:;
    if (asid >= 0) H5Sclose (asid);
    if (aid >= 0) H5Aclose (aid);
    return (int)herr;
}

int hdf5_get_att (hid_t fid, int vid, const char *name, hid_t atype, void *buf) {
    herr_t herr = 0;
    hid_t aid   = -1;
    hid_t did;

    if (vid == NC_GLOBAL)
        did = fid;
    else
        did = f_dids[vid];

    aid = H5Aopen (did, name, H5P_DEFAULT);
    CHECK_HID (aid)
    herr = H5Aread (aid, atype, buf);
    CHECK_HERR

fn_exit:;
    if (aid >= 0) H5Aclose (aid);
    return (int)herr;
}

int hdf5_def_var (hid_t fid, const char *name, nc_type nctype, int ndim, int *dimids, int *vid) {
    herr_t herr = 0;
    int i;
    hid_t did;
    hid_t sid    = -1;
    hid_t dcplid = -1;
    hsize_t dims[H5S_MAX_RANK], mdims[H5S_MAX_RANK];

    dcplid = H5Pcreate (H5P_DATASET_CREATE);
    CHECK_HID (dcplid)

    for (i = 0; i < ndim; i++) { dims[i] = mdims[i] = f_dims[dimids[i]]; }
    if (ndim) {
        if (dims[0] == H5S_UNLIMITED) {
            dims[0] = 1;

            herr = H5Pset_chunk (dcplid, ndim, dims);
            CHECK_HERR
            dims[0] = 0;
        }
    }

    //sid = H5Screate_simple (ndim, dims, mdims);
    sid = H5Screate_simple (ndim, dims, mdims);
    CHECK_HID (sid);
/*
    printf("------------------ %d\n", f_nd);
        for ( i = 0; i < ndim; ++i ){
            printf("creating dataset with dims[%d]=%lld, mdims[%d]=%lld , f_dims[dimids[%d]] = %lld\n",i,(long long int)dims[i], i, (long long int)mdims[i], i, (long long int) f_dims[dimids[i]]);
        }
*/
    did = H5Dcreate2 (fid, name, nc_type_to_hdf5_type (nctype), sid, H5P_DEFAULT, dcplid,
                      H5P_DEFAULT);
    CHECK_HID (did)

    f_dids[f_nd] = did;
    *vid         = f_nd++;

fn_exit:;
    if (sid != -1) H5Sclose (sid);
    if (dcplid != -1) H5Pclose (dcplid);
    return (int)herr;
}

int hdf5_inq_varid (hid_t fid, const char *name, int *vid) {
    herr_t herr = 0;
    hid_t did;

    did = H5Dopen2 (fid, name, H5P_DEFAULT);
    CHECK_HID (did)

    f_dids[f_nd] = did;
    *vid         = f_nd++;

fn_exit:;
    return (int)herr;
}

int hdf5_def_dim (hid_t fid, const char *name, MPI_Offset msize, int *did) {
    herr_t herr = 0;
    int i;
    hid_t sid = -1;
    hid_t aid = -1;
    hsize_t size;
    char aname[128];

    size = (hsize_t)msize;
    if (size == NC_UNLIMITED) size = H5S_UNLIMITED;

    sid = H5Screate (H5S_SCALAR);
    CHECK_HID (sid)

    sprintf (aname, "_NCDIM_%s", name);
    aid = H5Acreate2 (fid, aname, H5T_NATIVE_HSIZE, sid, H5P_DEFAULT, H5P_DEFAULT);
    CHECK_HID (aid)

    herr = H5Awrite (aid, H5T_NATIVE_HSIZE, &size);

    f_dims[f_ndim] = size;
    *did           = f_ndim++;

fn_exit:;
    if (aid != -1) H5Aclose (aid);
    if (sid != -1) H5Sclose (sid);
    return (int)herr;
}

int hdf5_inq_dimid (hid_t fid, const char *name, int *did) {
    herr_t herr = 0;
    int i;
    hid_t sid = -1;
    hid_t aid;
    hsize_t size;
    char aname[128];

    sprintf (aname, "_NCDIM_%s", name);
    aid = H5Aopen (fid, aname, H5P_DEFAULT);
    CHECK_HID (aid)

    herr = H5Aread (aid, H5T_NATIVE_HSIZE, &size);
    CHECK_HERR

    f_dims[f_ndim] = size;
    *did           = f_ndim++;

fn_exit:;
    if (aid != -1) H5Aclose (aid);
    return (int)herr;
}

int hdf5_inq_dimlen (hid_t fid, int did, MPI_Offset *msize) {
    *msize = f_dims[did];
    return 0;
}

int hdf5_inq_file_info (hid_t fid, MPI_Info *info) {
    herr_t herr = 0;
    hid_t pid;

    pid = H5Fget_access_plist (fid);
    CHECK_HID (pid);
    herr = H5Pget_fapl_mpio (pid, NULL, info);
    CHECK_HERR

fn_exit:;
    if (pid != -1) H5Pclose (pid);
    return (int)herr;
}

int hdf5_inq_put_size (hid_t fid, size_t *size) {
    herr_t herr = 0;

    *size = 0;

fn_exit:;

    return (int)herr;
}

int hdf5_inq_get_size (hid_t fid, size_t *size) {
    herr_t herr = 0;

    *size = 0;

fn_exit:;

    return (int)herr;
}

int hdf5_close_vars (hid_t fid) {
    herr_t herr = 0;
    int i;

    for (i = 0; i < f_nd; i++) {
        herr = H5Dclose (f_dids[i]);
        CHECK_HERR
    }

fn_exit:;
    return (int)herr;
}
