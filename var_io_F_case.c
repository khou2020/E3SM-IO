/*********************************************************************
 *
 * Copyright (C) 2018, Northwestern University
 * See COPYRIGHT notice in top-level directory.
 *
 * This program uses the E3SM I/O patterns recorded by the PIO library to
 * evaluate the performance of two PnetCDF APIs: nc_vard_all(), and
 * nc_put_varn(). The E3SM I/O patterns consist of a large number of small,
 * noncontiguous requests on each MPI process, which presents a challenge for
 * achieving a good performance.
 *
 * See README.md for compile and run instructions.
 *
 *********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> /* unlink() */

#include <e3sm_io.h>

static int nc_put_varn(int ncid, int vid, int nreq, MPI_Offset* const *starts, MPI_Offset* const *counts, void *buf, MPI_Offset bufcount, MPI_Datatype buftype){
    int err;
    int i;
    int nreq_all;
    int esize, rsize;
    int ndim;
    MPI_Offset dummycount[16] = {0}; 
    MPI_Offset dummystart[16] = {0}; 
    char *bufp = (char*)buf;

    MPI_Allreduce(&nreq, &nreq_all, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    MPI_Type_size(buftype, &esize);

    err = nc_inq_varndims(ncid, vid, &ndim);

    for(i = 0; i < nreq_all; i++){
        if (i < nreq){
            rsize = esize;
            for(j = 0; j < ndim; j++){
                rsize *= count[j];
            }
            
            if (buftype == MPI_FLOAT){
                err = nc_put_vara_float(ncid, vid, dummystart, dummycount, (float*)bufp);
            }
            else{
                err = nc_put_vara_double(ncid, vid, dummystart, dummycount, (double*)bufp);  
            }
            ERR

            bufp += rsize;
        }
        else{
            if (buftype == MPI_FLOAT){
                err = nc_put_vara_float(ncid, vid, dummystart, dummycount, (float*)buf);
            }
            else{
                err = nc_put_vara_double(ncid, vid, dummystart, dummycount, (double*)buf);  
            }
            ERR
        }
    }
}

static int nc_get_varn(int ncid, int vid, int nreq, MPI_Offset* const *starts, MPI_Offset* const *counts, void *buf, MPI_Offset bufcount, MPI_Datatype buftype){
    int err;
    int i;
    int nreq_all;
    int esize, rsize;
    int ndim;
    MPI_Offset dummycount[16] = {0}; 
    MPI_Offset dummystart[16] = {0}; 
    char *bufp = (char*)buf;

    MPI_Allreduce(&nreq, &nreq_all, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);

    MPI_Type_size(buftype, &esize);

    err = nc_inq_varndims(ncid, vid, &ndim);

    for(i = 0; i < nreq_all; i++){
        if (i < nreq){
            rsize = esize;
            for(j = 0; j < ndim; j++){
                rsize *= count[j];
            }
            
            if (buftype == MPI_FLOAT){
                err = nc_put_vara_float(ncid, vid, dummystart, dummycount, (float*)bufp);
            }
            else{
                err = nc_put_vara_double(ncid, vid, dummystart, dummycount, (double*)bufp);  
            }
            ERR

            bufp += rsize;
        }
        else{
            if (buftype == MPI_FLOAT){
                err = nc_put_vara_float(ncid, vid, dummystart, dummycount, (float*)buf);
            }
            else{
                err = nc_put_vara_double(ncid, vid, dummystart, dummycount, (double*)buf);  
            }
            ERR
        }
    }
}

/*----< write_small_vars_F_case() >------------------------------------------*/
static int
write_small_vars_F_case(int          ncid,
                        int          vid,    /* starting variable ID */
                        int         *varids,
                        int          rec_no,
                        int          gap,
                        MPI_Offset   lev,
                        MPI_Offset   ilev,
                        MPI_Offset   nbnd,
                        MPI_Offset   nchars,
                        int        **int_buf,
                        char       **txt_buf,
                        double     **dbl_buf)
{
    int i, err, nerrs=0;
    MPI_Offset start[2], count[2];

    /* scalar and small variables are written by rank 0 only */
    i = vid;

    if (rec_no == 0) {
        /* lev */
        err = nc_put_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += lev + gap;
        /* hyam */
        err = nc_put_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += lev + gap;
        /* hybm */
        err = nc_put_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += lev + gap;
        /* P0 */
        err = nc_put_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += 1 + gap;
        /* ilev */
        err = nc_put_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += ilev + gap;
        /* hyai */
        err = nc_put_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += ilev + gap;
        /* hybi */
        err = nc_put_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += ilev + gap;
    }
    else
        i += 7;

    /* time */
    start[0] = rec_no;
    err = nc_put_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* date */
    start[0] = rec_no;
    err = nc_put_var1_int(ncid, varids[i++], start, *int_buf); ERR
    *int_buf += 1;
    /* datesec */
    start[0] = rec_no;
    err = nc_put_var1_int(ncid, varids[i++], start, *int_buf); ERR
    *int_buf += 1;
    /* time_bnds */
    start[0] = rec_no; start[1] = 0;
    count[0] = 1;      count[1] = nbnd;
    err = nc_put_vara_double(ncid, varids[i++], start, count, *dbl_buf); ERR
    *dbl_buf += nbnd + gap;
    /* date_written */
    start[0] = rec_no; start[1] = 0;
    count[0] = 1;      count[1] = nchars;
    err = nc_put_vara_text(ncid, varids[i++], start, count, *txt_buf); ERR
    *txt_buf += nchars;
    /* time_written */
    start[0] = rec_no; start[1] = 0;
    count[0] = 1;      count[1] = nchars;
    err = nc_put_vara_text(ncid, varids[i++], start, count, *txt_buf); ERR
    *txt_buf += nchars;

    if (rec_no == 0) {
        /* ndbase */
        err = nc_put_var_int(ncid, varids[i++], *int_buf); ERR
        *int_buf += 1;
        /* nsbase */
        err = nc_put_var_int(ncid, varids[i++], *int_buf); ERR
        *int_buf += 1;
        /* nbdate */
        err = nc_put_var_int(ncid, varids[i++], *int_buf); ERR
        *int_buf += 1;
        /* nbsec */
        err = nc_put_var_int(ncid, varids[i++], *int_buf); ERR
        *int_buf += 1;
        /* mdt */
        err = nc_put_var_int(ncid, varids[i++], *int_buf); ERR
        *int_buf += 1;
    }
    else
        i += 5;

    /* ndcur */
    start[0] = rec_no;
    err = nc_put_var1_int(ncid, varids[i++], start, *int_buf); ERR
    *int_buf += 1;
    /* nscur */
    start[0] = rec_no;
    err = nc_put_var1_int(ncid, varids[i++], start, *int_buf); ERR
    *int_buf += 1;
    /* co2vmr */
    start[0] = rec_no;
    err = nc_put_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* ch4vmr */
    start[0] = rec_no;
    err = nc_put_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* n2ovmr */
    start[0] = rec_no;
    err = nc_put_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* f11vmr */
    start[0] = rec_no;
    err = nc_put_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* f12vmr */
    start[0] = rec_no;
    err = nc_put_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* sol_tsi */
    start[0] = rec_no;
    err = nc_put_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* nsteph */
    start[0] = rec_no;
    err = nc_put_var1_int(ncid, varids[i++], start, *int_buf); ERR
    *int_buf += 1;
fn_exit:
    return err;
}

/*----< read_small_vars_F_case() >------------------------------------------*/
static int
read_small_vars_F_case(int          ncid,
                        int          vid,    /* starting variable ID */
                        int         *varids,
                        int          rec_no,
                        int          gap,
                        MPI_Offset   lev,
                        MPI_Offset   ilev,
                        MPI_Offset   nbnd,
                        MPI_Offset   nchars,
                        int        **int_buf,
                        char       **txt_buf,
                        double     **dbl_buf)
{
    int i, err, nerrs=0;
    MPI_Offset start[2], count[2];

    /* scalar and small variables are read by rank 0 only */
    i = vid;

    if (rec_no == 0) {
        /* lev */
        err = nc_get_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += lev + gap;
        /* hyam */
        err = nc_get_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += lev + gap;
        /* hybm */
        err = nc_get_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += lev + gap;
        /* P0 */
        err = nc_get_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += 1 + gap;
        /* ilev */
        err = nc_get_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += ilev + gap;
        /* hyai */
        err = nc_get_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += ilev + gap;
        /* hybi */
        err = nc_get_var_double(ncid, varids[i++], *dbl_buf); ERR
        *dbl_buf += ilev + gap;
    }
    else
        i += 7;

    /* time */
    start[0] = rec_no;
    err = nc_get_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* date */
    start[0] = rec_no;
    err = nc_get_var1_int(ncid, varids[i++], start, *int_buf); ERR
    *int_buf += 1;
    /* datesec */
    start[0] = rec_no;
    err = nc_get_var1_int(ncid, varids[i++], start, *int_buf); ERR
    *int_buf += 1;
    /* time_bnds */
    start[0] = rec_no; start[1] = 0;
    count[0] = 1;      count[1] = nbnd;
    err = nc_get_vara_double(ncid, varids[i++], start, count, *dbl_buf); ERR
    *dbl_buf += nbnd + gap;
    /* date_written */
    start[0] = rec_no; start[1] = 0;
    count[0] = 1;      count[1] = nchars;
    err = nc_get_vara_text(ncid, varids[i++], start, count, *txt_buf); ERR
    *txt_buf += nchars;
    /* time_written */
    start[0] = rec_no; start[1] = 0;
    count[0] = 1;      count[1] = nchars;
    err = nc_get_vara_text(ncid, varids[i++], start, count, *txt_buf); ERR
    *txt_buf += nchars;

    if (rec_no == 0) {
        /* ndbase */
        err = nc_get_var_int(ncid, varids[i++], *int_buf); ERR
        *int_buf += 1;
        /* nsbase */
        err = nc_get_var_int(ncid, varids[i++], *int_buf); ERR
        *int_buf += 1;
        /* nbdate */
        err = nc_get_var_int(ncid, varids[i++], *int_buf); ERR
        *int_buf += 1;
        /* nbsec */
        err = nc_get_var_int(ncid, varids[i++], *int_buf); ERR
        *int_buf += 1;
        /* mdt */
        err = nc_get_var_int(ncid, varids[i++], *int_buf); ERR
        *int_buf += 1;
    }
    else
        i += 5;

    /* ndcur */
    start[0] = rec_no;
    err = nc_get_var1_int(ncid, varids[i++], start, *int_buf); ERR
    *int_buf += 1;
    /* nscur */
    start[0] = rec_no;
    err = nc_get_var1_int(ncid, varids[i++], start, *int_buf); ERR
    *int_buf += 1;
    /* co2vmr */
    start[0] = rec_no;
    err = nc_get_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* ch4vmr */
    start[0] = rec_no;
    err = nc_get_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* n2ovmr */
    start[0] = rec_no;
    err = nc_get_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* f11vmr */
    start[0] = rec_no;
    err = nc_get_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* f12vmr */
    start[0] = rec_no;
    err = nc_get_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* sol_tsi */
    start[0] = rec_no;
    err = nc_get_var1_double(ncid, varids[i++], start, *dbl_buf); ERR
    *dbl_buf += 1 + gap;
    /* nsteph */
    start[0] = rec_no;
    err = nc_get_var1_int(ncid, varids[i++], start, *int_buf); ERR
    *int_buf += 1;
fn_exit:
    return err;
}

#define FIX_1D_VAR_STARTS_COUNTS(starts, counts, nreqs, disps, blocklens) { \
    starts = (MPI_Offset**) malloc(2 * nreqs * sizeof(MPI_Offset*)); \
    counts = starts + nreqs; \
    starts[0] = (MPI_Offset*) malloc(2 * nreqs * sizeof(MPI_Offset)); \
    counts[0] = starts[0] + nreqs; \
    \
    for (j=1; j<nreqs; j++) { \
        starts[j] = starts[j-1] + 1; \
        counts[j] = counts[j-1] + 1; \
    } \
    \
    for (j=0; j<nreqs; j++) { \
        starts[j][0] = disps[j]; \
        counts[j][0] = blocklens[j]; \
    } \
}

#define FIX_2D_VAR_STARTS_COUNTS(starts, counts, nreqs, disps, blocklens, last_dimlen) { \
    starts = (MPI_Offset**) malloc(2 * nreqs * sizeof(MPI_Offset*)); \
    counts = starts + nreqs; \
    starts[0] = (MPI_Offset*) malloc(2 * nreqs * 2 * sizeof(MPI_Offset)); \
    counts[0] = starts[0] + nreqs * 2; \
    \
    for (j=1; j<nreqs; j++) { \
        starts[j] = starts[j-1] + 2; \
        counts[j] = counts[j-1] + 2; \
    } \
    \
    k = 0; \
    starts[0][0] = disps[0] / last_dimlen; \
    starts[0][1] = disps[0] % last_dimlen; /* decomposition is 2D */ \
    counts[0][0] = 1; \
    counts[0][1] = blocklens[0]; /* each blocklens[j] is no bigger than last_dimlen */ \
    for (j=1; j<nreqs; j++) { \
        MPI_Offset _start[2]; \
        _start[0] = disps[j] / last_dimlen; \
        _start[1] = disps[j] % last_dimlen; \
        if (_start[0] == starts[k][0] + counts[k][0] && \
            _start[1] == starts[k][1] && blocklens[j] == counts[k][1]) \
            counts[k][0]++; \
        else { \
            k++; \
            starts[k][0] = _start[0]; \
            starts[k][1] = _start[1]; \
            counts[k][0] = 1; \
            counts[k][1] = blocklens[j]; /* each blocklens[j] is no bigger than last_dimlen */ \
        } \
    } \
    nreqs = k + 1; \
}

#define REC_2D_VAR_STARTS_COUNTS(rec, starts, counts, nreqs, disps, blocklens) { \
    starts = (MPI_Offset**) malloc(2 * nreqs * sizeof(MPI_Offset*)); \
    counts = starts + nreqs; \
    starts[0] = (MPI_Offset*) malloc(2 * nreqs * 2 * sizeof(MPI_Offset)); \
    counts[0] = starts[0] + nreqs * 2; \
    \
    for (j=1; j<nreqs; j++) { \
        starts[j] = starts[j-1] + 2; \
        counts[j] = counts[j-1] + 2; \
    } \
    \
    for (j=0; j<nreqs; j++) { \
        starts[j][1] = disps[j]; /* decomposition is 1D */ \
        counts[j][1] = blocklens[j]; \
        \
        starts[j][0] = rec; /* record ID */ \
        counts[j][0] = 1;   /* one record only */ \
    } \
}

#define REC_3D_VAR_STARTS_COUNTS(rec, starts, counts, nreqs, disps, blocklens, last_dimlen) { \
    starts = (MPI_Offset**) malloc(2 * nreqs * sizeof(MPI_Offset*)); \
    counts = starts + nreqs; \
    starts[0] = (MPI_Offset*) malloc(2 * nreqs * 3 * sizeof(MPI_Offset)); \
    counts[0] = starts[0] + nreqs * 3; \
    \
    for (j=1; j<nreqs; j++) { \
        starts[j] = starts[j-1] + 3; \
        counts[j] = counts[j-1] + 3; \
    } \
    \
    k = 0; \
    starts[0][0] = rec; /* record ID */ \
    starts[0][1] = disps[0] / last_dimlen; \
    starts[0][2] = disps[0] % last_dimlen; /* decomposition is 2D */ \
    counts[0][0] = 1;   /* one record only */ \
    counts[0][1] = 1; \
    counts[0][2] = blocklens[0]; /* each blocklens[j] is no bigger than last_dimlen */ \
    for (j=1; j<nreqs; j++) { \
        MPI_Offset _start[2]; \
        _start[0] = disps[j] / last_dimlen; \
        _start[1] = disps[j] % last_dimlen; \
        if (starts[k][0] == rec && _start[0] == starts[k][1] + counts[k][1] && \
            _start[1] == starts[k][2] && blocklens[j] == counts[k][2]) \
            counts[k][1]++; \
        else { \
            k++; \
            starts[k][0] = rec; \
            starts[k][1] = _start[0]; \
            starts[k][2] = _start[1]; \
            counts[k][0] = 1; \
            counts[k][1] = 1; \
            counts[k][2] = blocklens[j]; /* each blocklens[j] is no bigger than last_dimlen */ \
        } \
    } \
    nreqs = k+1; \
}

#define POST_VARN(k, num, vid) \
    for (j=0; j<num; j++) { \
        err = nc_put_varn(ncid, vid+j, xnreqs[k-1], starts_D##k, \
                              counts_D##k, rec_buf_ptr, -1, REC_ITYPE); \
        ERR \
        rec_buf_ptr += nelems[k-1] + gap; \
        my_nreqs += xnreqs[k-1]; \
    }

#define POST_VARN_RD(k, num, vid) \
    for (j=0; j<num; j++) { \
        err = nc_get_varn(ncid, vid+j, xnreqs[k-1], starts_D##k, \
                              counts_D##k, rec_buf_ptr, -1, REC_ITYPE); \
        ERR \
        rec_buf_ptr += nelems[k-1] + gap; \
        my_nreqs += xnreqs[k-1]; \
    }


/*----< run_varn_F_case() >--------------------------------------------------*/
int
run_varn_F_case(MPI_Comm io_comm,         /* MPI communicator that includes all the tasks involved in IO */
                const char *out_prefix,   /* output file prefix */
                const char *out_postfix,  /* output file postfix */
                int         nvars,        /* number of variables 408 or 51 */
                int         num_recs,     /* number of records */
                int         noncontig_buf,/* whether to us noncontiguous buffer */
                MPI_Info    info,
                MPI_Offset  dims[3][2],   /* dimension lengths */
                const int   nreqs[3],     /* no. request in decompositions 1,2,3 */
                int* const  disps[3],     /* request's displacements */
                int* const  blocklens[3], /* request's block lengths */
                int         format,
                double     *dbl_bufp,
                itype      *rec_bufp,
                char       *txt_buf,
                int        *int_buf)
{
    char outfname[512],  *txt_buf_ptr;
    int i, j, k, err, nerrs=0, rank, ncid, cmode, *varids;
    int rec_no, gap=0, my_nreqs, *int_buf_ptr, xnreqs[3];
    size_t dbl_buflen, rec_buflen, nelems[3];
    itype *rec_buf=NULL, *rec_buf_ptr;
    double *dbl_buf=NULL, *dbl_buf_ptr;
    double pre_timing, open_timing, post_timing, wait_timing, close_timing;
    double timing, total_timing,  max_timing;
    MPI_Offset tmp, metadata_size, put_size, total_size, max_nreqs, total_nreqs;
    MPI_Offset **starts_D2=NULL, **counts_D2=NULL;
    MPI_Offset **starts_D3=NULL, **counts_D3=NULL;
    MPI_Info info_used=MPI_INFO_NULL;

    MPI_Barrier(io_comm); /*-----------------------------------------*/
    total_timing = pre_timing = MPI_Wtime();

    open_timing = 0.0;
    post_timing = 0.0;
    wait_timing = 0.0;
    close_timing = 0.0;

    MPI_Comm_rank(io_comm, &rank);

    if (noncontig_buf) gap = 10;

    varids = (int*) malloc(nvars * sizeof(int));

    for (i=0; i<3; i++) xnreqs[i] = nreqs[i];

    /* calculate number of variable elements from 3 decompositions */
    my_nreqs = 0;
    for (i=0; i<3; i++) {
        for (nelems[i]=0, k=0; k<xnreqs[i]; k++)
            nelems[i] += blocklens[i][k];
    }
    if (verbose && rank == 0)
        printf("nelems=%zd %zd %zd\n", nelems[0],nelems[1],nelems[2]);

    /* allocate and initialize write buffer for small variables */
    dbl_buflen = nelems[1] * 2 + nelems[0]
               + 3 * dims[2][0] + 3 * (dims[2][0]+1) + 8 + 2
               + 20 * gap;
    if (dbl_bufp != NULL){
        dbl_buf = dbl_bufp;
    }
    else{
        dbl_buf = (double*) malloc(dbl_buflen * sizeof(double));
        for (i=0; i<dbl_buflen; i++) dbl_buf[i] = rank;
    }

    /* allocate and initialize write buffer for large variables */
    if (nvars == 408)
        rec_buflen = nelems[1] * 315 + nelems[2] * 63 + (315+63) * gap;
    else
        rec_buflen = nelems[1] * 20 + nelems[2] + (20+1) * gap;

    if (rec_bufp != NULL){
        rec_buf = rec_bufp;
    }
    else{
        rec_buf = (itype*) malloc(rec_buflen * sizeof(itype));

        for (i=0; i<rec_buflen; i++) rec_buf[i] = rank;
        for (i=0; i<10; i++) int_buf[i] = rank;
        for (i=0; i<16; i++) txt_buf[i] = 'a' + rank;
    }

    pre_timing = MPI_Wtime() - pre_timing;

    MPI_Barrier(io_comm); /*-----------------------------------------*/
    timing = MPI_Wtime();

    /* set output file name */
    sprintf(outfname, "%s%s",out_prefix, out_postfix);

    /* create a new CDF-5 file for writing */
    cmode = NC_CLOBBER | NC_NETCDF4 | NC_MPIIO;
    err = nc_create_par(outfname, cmode, io_comm, info, &ncid); ERR

    /* define dimensions, variables, and attributes */
    if (nvars == 408) {
        /* for h0 file */
        err = def_F_case_h0(ncid, dims[2], nvars, varids); ERR
    }
    else {
        /* for h1 file */
        err = def_F_case_h1(ncid, dims[2], nvars, varids); ERR
    }

    /* exit define mode and enter data mode */
    err = nc_enddef(ncid); ERR

    /* I/O amount so far */
    err = nc_inq_put_size(ncid, &metadata_size); ERR
    err = nc_inq_file_info(ncid, &info_used); ERR
    open_timing += MPI_Wtime() - timing;

    MPI_Barrier(io_comm); /*-----------------------------------------*/
    timing = MPI_Wtime();

    i = 0;
    dbl_buf_ptr = dbl_buf;

    if (xnreqs[1] > 0) {
        /* lat */
        MPI_Offset **fix_starts_D2, **fix_counts_D2;

        /* construct varn API arguments starts[][] and counts[][] */
        int num = xnreqs[1];
        FIX_1D_VAR_STARTS_COUNTS(fix_starts_D2, fix_counts_D2, num, disps[1], blocklens[1])

        REC_2D_VAR_STARTS_COUNTS(0, starts_D2, counts_D2, xnreqs[1], disps[1], blocklens[1])

        err = nc_put_varn(ncid, varids[i++], xnreqs[1], fix_starts_D2, fix_counts_D2,
                              dbl_buf_ptr, nelems[1], MPI_DOUBLE); ERR
        dbl_buf_ptr += nelems[1] + gap;
        my_nreqs += xnreqs[1];

        /* lon */
        err = nc_put_varn(ncid, varids[i++], xnreqs[1], fix_starts_D2, fix_counts_D2,
                              dbl_buf_ptr, nelems[1], MPI_DOUBLE); ERR
        dbl_buf_ptr += nelems[1] + gap;
        my_nreqs += xnreqs[1];

        free(fix_starts_D2[0]);
        free(fix_starts_D2);
    }
    else i += 2;

    /* area */
    if (xnreqs[0] > 0) {
        MPI_Offset **fix_starts_D1, **fix_counts_D1;

        /* construct varn API arguments starts[][] and counts[][] */
        FIX_1D_VAR_STARTS_COUNTS(fix_starts_D1, fix_counts_D1, xnreqs[0], disps[0], blocklens[0])

        err = nc_put_varn(ncid, varids[i++], xnreqs[0], fix_starts_D1, fix_counts_D1,
                              dbl_buf_ptr, nelems[0], MPI_DOUBLE); ERR
        dbl_buf_ptr += nelems[0] + gap;
        my_nreqs += xnreqs[0];

        free(fix_starts_D1[0]);
        free(fix_starts_D1);
    }
    else i++;

    /* construct varn API arguments starts[][] and counts[][] */
    if (xnreqs[2] > 0)
        REC_3D_VAR_STARTS_COUNTS(0, starts_D3, counts_D3, xnreqs[2], disps[2], blocklens[2], dims[2][1])

    post_timing += MPI_Wtime() - timing;

    for (rec_no=0; rec_no<num_recs; rec_no++) {
        MPI_Barrier(io_comm); /*-----------------------------------------*/
        timing = MPI_Wtime();

        i=3;
        dbl_buf_ptr = dbl_buf + nelems[1]*2 + nelems[0] + gap*3;
        int_buf_ptr = int_buf;
        txt_buf_ptr = txt_buf;

        /* next 27 small variables are written by rank 0 only */
        if (rank == 0) {
            my_nreqs += 27;
            /* post nonblocking requests using nc_put_varn() */
            err = write_small_vars_F_case(ncid, i, varids, rec_no, gap,
                                          dims[2][0], dims[2][0]+1, 2, 8,
                                          &int_buf_ptr, &txt_buf_ptr,
                                          &dbl_buf_ptr);
            ERR
        }
        i += 27;

        post_timing += MPI_Wtime() - timing;

        MPI_Barrier(io_comm); /*-----------------------------------------*/
        timing = MPI_Wtime();

        /* flush fixed-size and small variables */
        err = nc_wait_all(ncid, NC_PUT_REQ_ALL, NULL); ERR

        wait_timing += MPI_Wtime() - timing;

        MPI_Barrier(io_comm); /*-----------------------------------------*/
        timing = MPI_Wtime();

        rec_buf_ptr = rec_buf;

        for (j=0; j<xnreqs[1]; j++) starts_D2[j][0] = rec_no;
        for (j=0; j<xnreqs[2]; j++) starts_D3[j][0] = rec_no;

        if (nvars == 408) {
            if (two_buf) {
                /* write 2D variables */
                POST_VARN(2,   1,  30)   /* AEROD_v */
                POST_VARN(2,  18,  33)   /* AODABS ... ANSNOW */
                POST_VARN(2,   6,  53)   /* AQ_DMS ... AQ_SOAG */
                POST_VARN(2,   2,  64)   /* CDNUMC and CLDHGH */
                POST_VARN(2,   3,  68)   /* CLDLOW ... CLDTOT */
                POST_VARN(2,  11,  75)   /* DF_DMS ... DSTSFMBL */
                POST_VARN(2,   2,  87)   /* DTENDTH and DTENDTQ */
                POST_VARN(2,   7,  91)   /* FLDS ... FLUTC */
                POST_VARN(2,  15, 102)   /* FSDS ... ICEFRAC */
                POST_VARN(2,   2, 120)   /* LANDFRAC and LHFLX */
                POST_VARN(2,   3, 127)   /* LINOZ_SZA ... LWCF */
                POST_VARN(2,   2, 142)   /* O3_SRF and OCNFRAC */
                POST_VARN(2,   1, 145)   /* OMEGA500 */
                POST_VARN(2,   8, 147)   /* PBLH ... PSL */
                POST_VARN(2,   2, 156)   /* QFLX and QREFHT */
                POST_VARN(2,   1, 161)   /* RAM1 */
                POST_VARN(2,  37, 163)   /* SFDMS ... SNOWHLND */
                POST_VARN(2,  10, 202)   /* SO2_CLXF ... SWCF */
                POST_VARN(2,  19, 213)   /* TAUGWX ... TVQ */
                POST_VARN(2,   1, 233)   /* U10 */
                POST_VARN(2,   3, 240)   /* WD_H2O2 ... WD_SO2 */
                POST_VARN(2,  32, 246)   /* airFV ... dst_c3SFWET */
                POST_VARN(2, 129, 279)   /* mlip ... soa_c3SFWET */
                /* write 3D variables */
                POST_VARN(3,   2,  31)   /* ANRAIN and ANSNOW */
                POST_VARN(3,   2,  51)   /* AQRAIN and AQSNOW */
                POST_VARN(3,   5,  59)   /* AREI ... CCN3 */
                POST_VARN(3,   2,  66)   /* CLDICE and CLDLIQ */
                POST_VARN(3,   4,  71)   /* CLOUD ... DCQ */
                POST_VARN(3,   1,  86)   /* DTCOND */
                POST_VARN(3,   2,  89)   /* EXTINCT and FICE */
                POST_VARN(3,   4,  98)   /* FREQI ... FREQS */
                POST_VARN(3,   3, 117)   /* ICIMR ... IWC */
                POST_VARN(3,   5, 122)   /* LINOZ_DO3 ... LINOZ_SSO3 */
                POST_VARN(3,  12, 130)   /* Mass_bc ... O3 */
                POST_VARN(3,   1, 144)   /* OMEGA */
                POST_VARN(3,   1, 146)   /* OMEGAT */
                POST_VARN(3,   1, 155)   /* Q */
                POST_VARN(3,   3, 158)   /* QRL ... RAINQM */
                POST_VARN(3,   1, 162)   /* RELHUM */
                POST_VARN(3,   2, 200)   /* SNOWQM and SO2 */
                POST_VARN(3,   1, 212)   /* T */
                POST_VARN(3,   1, 232)   /* U */
                POST_VARN(3,   6, 234)   /* UU ... VV */
                POST_VARN(3,   3, 243)   /* WSUB ... aero_water */
                POST_VARN(3,   1, 278)   /* hstobie_linoz */
            } else {
                /* write variables in the same order as they defined */
                POST_VARN(2,   1,  30)   /* AEROD_v */
                POST_VARN(3,   2,  31)   /* ANRAIN and ANSNOW */
                POST_VARN(2,  18,  33)   /* AODABS ... ANSNOW */
                POST_VARN(3,   2,  51)   /* AQRAIN and AQSNOW */
                POST_VARN(2,   6,  53)   /* AQ_DMS ... AQ_SOAG */
                POST_VARN(3,   5,  59)   /* AREI ... CCN3 */
                POST_VARN(2,   2,  64)   /* CDNUMC and CLDHGH */
                POST_VARN(3,   2,  66)   /* CLDICE and CLDLIQ */
                POST_VARN(2,   3,  68)   /* CLDLOW ... CLDTOT */
                POST_VARN(3,   4,  71)   /* CLOUD ... DCQ */
                POST_VARN(2,  11,  75)   /* DF_DMS ... DSTSFMBL */
                POST_VARN(3,   1,  86)   /* DTCOND */
                POST_VARN(2,   2,  87)   /* DTENDTH and DTENDTQ */
                POST_VARN(3,   2,  89)   /* EXTINCT and FICE */
                POST_VARN(2,   7,  91)   /* FLDS ... FLUTC */
                POST_VARN(3,   4,  98)   /* FREQI ... FREQS */
                POST_VARN(2,  15, 102)   /* FSDS ... ICEFRAC */
                POST_VARN(3,   3, 117)   /* ICIMR ... IWC */
                POST_VARN(2,   2, 120)   /* LANDFRAC and LHFLX */
                POST_VARN(3,   5, 122)   /* LINOZ_DO3 ... LINOZ_SSO3 */
                POST_VARN(2,   3, 127)   /* LINOZ_SZA ... LWCF */
                POST_VARN(3,  12, 130)   /* Mass_bc ... O3 */
                POST_VARN(2,   2, 142)   /* O3_SRF and OCNFRAC */
                POST_VARN(3,   1, 144)   /* OMEGA */
                POST_VARN(2,   1, 145)   /* OMEGA500 */
                POST_VARN(3,   1, 146)   /* OMEGAT */
                POST_VARN(2,   8, 147)   /* PBLH ... PSL */
                POST_VARN(3,   1, 155)   /* Q */
                POST_VARN(2,   2, 156)   /* QFLX and QREFHT */
                POST_VARN(3,   3, 158)   /* QRL ... RAINQM */
                POST_VARN(2,   1, 161)   /* RAM1 */
                POST_VARN(3,   1, 162)   /* RELHUM */
                POST_VARN(2,  37, 163)   /* SFDMS ... SNOWHLND */
                POST_VARN(3,   2, 200)   /* SNOWQM and SO2 */
                POST_VARN(2,  10, 202)   /* SO2_CLXF ... SWCF */
                POST_VARN(3,   1, 212)   /* T */
                POST_VARN(2,  19, 213)   /* TAUGWX ... TVQ */
                POST_VARN(3,   1, 232)   /* U */
                POST_VARN(2,   1, 233)   /* U10 */
                POST_VARN(3,   6, 234)   /* UU ... VV */
                POST_VARN(2,   3, 240)   /* WD_H2O2 ... WD_SO2 */
                POST_VARN(3,   3, 243)   /* WSUB ... aero_water */
                POST_VARN(2,  32, 246)   /* airFV ... dst_c3SFWET */
                POST_VARN(3,   1, 278)   /* hstobie_linoz */
                POST_VARN(2, 129, 279)   /* mlip ... soa_c3SFWET */
            }
        }
        else {
            if (two_buf) {
                /* write 2D variables followed by 3D variables */
                POST_VARN(2, 13, 30)   /* CLDHGH ... T5 */
                POST_VARN(2,  7, 44)   /* U250 ... Z500 */
                POST_VARN(3,  1, 43)   /* U */
            } else {
                /* write variables in the same order as they defined */
                POST_VARN(2, 13, 30)   /* CLDHGH ... T5 */
                POST_VARN(3,  1, 43)   /* U */
                POST_VARN(2,  7, 44)   /* U250 ... Z500 */
            }
        }

        post_timing += MPI_Wtime() - timing;

        MPI_Barrier(io_comm); /*-----------------------------------------*/
        timing = MPI_Wtime();

        err = nc_wait_all(ncid, NC_PUT_REQ_ALL, NULL); ERR

        wait_timing += MPI_Wtime() - timing;
    }

    MPI_Barrier(io_comm); /*-----------------------------------------*/
    timing = MPI_Wtime();

    err = nc_inq_put_size(ncid, &total_size); ERR
    put_size = total_size - metadata_size;
    err = nc_close(ncid); ERR
    close_timing += MPI_Wtime() - timing;

    if (starts_D3 != NULL) {
        free(starts_D3[0]);
        free(starts_D3);
    }
    if (starts_D2 != NULL) {
        free(starts_D2[0]);
        free(starts_D2);
    }
    if (rec_bufp == NULL && rec_buf != NULL) free(rec_buf);
    if (dbl_bufp == NULL && dbl_buf != NULL) free(dbl_buf);
    free(varids);

    total_timing = MPI_Wtime() - total_timing;

    tmp = my_nreqs;
    MPI_Reduce(&tmp,           &max_nreqs,  1, MPI_OFFSET, MPI_MAX, 0, io_comm);
    MPI_Reduce(&tmp,           &total_nreqs,1, MPI_OFFSET, MPI_SUM, 0, io_comm);
    MPI_Reduce(&put_size,      &tmp,        1, MPI_OFFSET, MPI_SUM, 0, io_comm);
    put_size = tmp;
    MPI_Reduce(&total_size,    &tmp,        1, MPI_OFFSET, MPI_SUM, 0, io_comm);
    total_size = tmp;
    MPI_Reduce(&open_timing,   &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    open_timing = max_timing;
    MPI_Reduce(&pre_timing,    &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    pre_timing = max_timing;
    MPI_Reduce(&post_timing,   &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    post_timing = max_timing;
    MPI_Reduce(&wait_timing,   &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    wait_timing = max_timing;
    MPI_Reduce(&close_timing,  &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    close_timing = max_timing;
    MPI_Reduce(&total_timing,  &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    total_timing = max_timing;

    /* check if there is any PnetCDF internal malloc residue */
    MPI_Offset malloc_size, sum_size;
    err = nc_inq_malloc_size(&malloc_size);
    if (err == NC_NOERR) {
        MPI_Reduce(&malloc_size, &sum_size, 1, MPI_OFFSET, MPI_SUM, 0, io_comm);
        if (rank == 0 && sum_size > 0) {
            printf("-----------------------------------------------------------\n");
            printf("heap memory allocated by PnetCDF internally has %lld bytes yet to be freed\n",
                   sum_size);
        }
    }
    MPI_Offset m_alloc=0, max_alloc;
    nc_inq_malloc_max_size(&m_alloc);
    MPI_Reduce(&m_alloc, &max_alloc, 1, MPI_OFFSET, MPI_MAX, 0, io_comm);
    if (rank == 0) {
        printf("History output file postfix        = %s\n", out_postfix);
        printf("#%%$: Write_max_heap_mib: %.2f\n", (float)max_alloc/1048576);
        printf("#%%$: Write_nvar: %d\n", nvars);
        printf("#%%$: Write_size_mib: %.2f\n", (double)total_size/1048576);
        printf("#%%$: Write_size_gib: %.2f\n", (double)total_size/1073741824);
        printf("#%%$: Write_nreq_sum: %lld\n", total_nreqs);
        printf("#%%$: Write_nreq_max: %lld\n", max_nreqs);
        printf("#%%$: Write_open_metadata_time: %.4f\n", open_timing);
        printf("#%%$: Write_io_prepare_time: %.4f\n", pre_timing);
        printf("#%%$: Write_req_post_time: %.4f\n", post_timing);
        printf("#%%$: Write_req_wait_time: %.4f\n", wait_timing);
        printf("#%%$: Write_close_time: %.4f\n", close_timing);
        printf("#%%$: Write_total_time: %.4f\n", total_timing);
        printf("#%%$: Write_bandwidth_open_to_close_mib: %.4f\n", (double)total_size/1048576.0/total_timing);
        printf("#%%$: Write_bandwidth_wait_mib: %.4f\n", (double)put_size/1048576.0/wait_timing);
        if (verbose) print_info(&info_used);
        printf("-----------------------------------------------------------\n");
    }

fn_exit:
    if (info_used != MPI_INFO_NULL) MPI_Info_free(&info_used);
    if (!keep_outfile) unlink(outfname);
    fflush(stdout);
    MPI_Barrier(io_comm);
    return nerrs;
}

/*----< run_varn_F_case() >--------------------------------------------------*/
int
run_varn_F_case_rd( MPI_Comm io_comm,         /* MPI communicator that includes all the tasks involved in IO */
                    const char *in_prefix,   /* input file prefix */
                    const char *in_postfix,  /* input file postfix */
                    int         nvars,        /* number of variables 408 or 51 */
                    int         num_recs,     /* number of records */
                    int         noncontig_buf,/* whether to us noncontiguous buffer */
                    MPI_Info    info,
                    MPI_Offset  dims[3][2],   /* dimension lengths */
                    const int   nreqs[3],     /* no. request in decompositions 1,2,3 */
                    int* const  disps[3],     /* request's displacements */
                    int* const  blocklens[3], /* request's block lengths */
                    int         format,
                    double     **dbl_bufp,
                    itype      **rec_bufp,
                    char       *txt_buf,
                    int        *int_buf)
{
    char infname[512], *txt_buf_ptr;
    int i, j, k, err, nerrs=0, rank, ncid, cmode, *varids;
    int rec_no, gap=0, my_nreqs, *int_buf_ptr, xnreqs[3];
    size_t dbl_buflen, rec_buflen, nelems[3];
    itype *rec_buf=NULL, *rec_buf_ptr;
    double *dbl_buf=NULL, *dbl_buf_ptr;
    double pre_timing, open_timing, post_timing, wait_timing, close_timing;
    double timing, total_timing,  max_timing;
    MPI_Offset tmp, metadata_size, get_size, total_size, max_nreqs, total_nreqs;
    MPI_Offset **starts_D2=NULL, **counts_D2=NULL;
    MPI_Offset **starts_D3=NULL, **counts_D3=NULL;
    MPI_Info info_used=MPI_INFO_NULL;

    MPI_Barrier(io_comm); /*-----------------------------------------*/
    total_timing = pre_timing = MPI_Wtime();

    open_timing = 0.0;
    post_timing = 0.0;
    wait_timing = 0.0;
    close_timing = 0.0;

    MPI_Comm_rank(io_comm, &rank);

    if (noncontig_buf) gap = 10;

    varids = (int*) malloc(nvars * sizeof(int));

    for (i=0; i<3; i++) xnreqs[i] = nreqs[i];

    /* calculate number of variable elements from 3 decompositions */
    my_nreqs = 0;
    for (i=0; i<3; i++) {
        for (nelems[i]=0, k=0; k<xnreqs[i]; k++)
            nelems[i] += blocklens[i][k];
    }
    if (verbose && rank == 0)
        printf("nelems=%zd %zd %zd\n", nelems[0],nelems[1],nelems[2]);

    /* allocate and initialize read buffer for small variables */
    dbl_buflen = nelems[1] * 2 + nelems[0]
               + 3 * dims[2][0] + 3 * (dims[2][0]+1) + 8 + 2
               + 20 * gap;
    
    dbl_buf = (double*) malloc(dbl_buflen * sizeof(double));
    if (dbl_bufp != NULL){
        *dbl_bufp = dbl_buf;
    }

    for (i=0; i<dbl_buflen; i++) dbl_buf[i] = rank + i;

    /* allocate and initialize read buffer for large variables */
    if (nvars == 408)
        rec_buflen = nelems[1] * 315 + nelems[2] * 63 + (315+63) * gap;
    else
        rec_buflen = nelems[1] * 20 + nelems[2] + (20+1) * gap;

    rec_buf = (itype*) malloc(rec_buflen * sizeof(itype));
    if (rec_bufp != NULL){
        *rec_bufp = rec_buf;
    }

    for (i=0; i<rec_buflen; i++) rec_buf[i] = rank + i;

    for (i=0; i<10; i++) int_buf[i] = rank + i;
    for (i=0; i<16; i++) txt_buf[i] = 'a' + rank + i;

    pre_timing = MPI_Wtime() - pre_timing;

    MPI_Barrier(io_comm); /*-----------------------------------------*/
    timing = MPI_Wtime();

    /* set input file name */
    sprintf(infname, "%s%s",in_prefix, in_postfix);

    /* open a new CDF-5 file for reading */
    cmode = NC_NETCDF4 | NC_MPIIO;
    err = nc_open_par(infname, cmode, io_comm, info, &ncid); ERR

    /* inquery dimensions, variables, and attributes */
    if (nvars == 408) {
        /* for h0 file */
        err = inq_F_case_h0(ncid, dims[2], nvars, varids); ERR
    }
    else {
        /* for h1 file */
        err = inq_F_case_h1(ncid, dims[2], nvars, varids); ERR
    }

    /* I/O amount so far */
    err = nc_inq_get_size(ncid, &metadata_size); ERR
    err = nc_inq_file_info(ncid, &info_used); ERR
    open_timing += MPI_Wtime() - timing;

    MPI_Barrier(io_comm); /*-----------------------------------------*/
    timing = MPI_Wtime();

    i = 0;
    dbl_buf_ptr = dbl_buf;

    if (xnreqs[1] > 0) {
        /* lat */
        MPI_Offset **fix_starts_D2, **fix_counts_D2;

        /* construct varn API arguments starts[][] and counts[][] */
        int num = xnreqs[1];
        FIX_1D_VAR_STARTS_COUNTS(fix_starts_D2, fix_counts_D2, num, disps[1], blocklens[1])

        REC_2D_VAR_STARTS_COUNTS(0, starts_D2, counts_D2, xnreqs[1], disps[1], blocklens[1])

        err = nc_get_varn(ncid, varids[i++], xnreqs[1], fix_starts_D2, fix_counts_D2,
                              dbl_buf_ptr, nelems[1], MPI_DOUBLE); ERR
        dbl_buf_ptr += nelems[1] + gap;
        my_nreqs += xnreqs[1];

        /* lon */
        err = nc_get_varn(ncid, varids[i++], xnreqs[1], fix_starts_D2, fix_counts_D2,
                              dbl_buf_ptr, nelems[1], MPI_DOUBLE); ERR
        dbl_buf_ptr += nelems[1] + gap;
        my_nreqs += xnreqs[1];

        free(fix_starts_D2[0]);
        free(fix_starts_D2);
    }
    else i += 2;

    /* area */
    if (xnreqs[0] > 0) {
        MPI_Offset **fix_starts_D1, **fix_counts_D1;

        /* construct varn API arguments starts[][] and counts[][] */
        FIX_1D_VAR_STARTS_COUNTS(fix_starts_D1, fix_counts_D1, xnreqs[0], disps[0], blocklens[0])

        err = nc_get_varn(ncid, varids[i++], xnreqs[0], fix_starts_D1, fix_counts_D1,
                              dbl_buf_ptr, nelems[0], MPI_DOUBLE); ERR
        dbl_buf_ptr += nelems[0] + gap;
        my_nreqs += xnreqs[0];

        free(fix_starts_D1[0]);
        free(fix_starts_D1);
    }
    else i++;

    /* construct varn API arguments starts[][] and counts[][] */
    if (xnreqs[2] > 0)
        REC_3D_VAR_STARTS_COUNTS(0, starts_D3, counts_D3, xnreqs[2], disps[2], blocklens[2], dims[2][1])

    post_timing += MPI_Wtime() - timing;

    for (rec_no=0; rec_no<num_recs; rec_no++) {
        MPI_Barrier(io_comm); /*-----------------------------------------*/
        timing = MPI_Wtime();

        i=3;
        dbl_buf_ptr = dbl_buf + nelems[1]*2 + nelems[0] + gap*3;
        int_buf_ptr = int_buf;
        txt_buf_ptr = txt_buf;

        /* next 27 small variables are read by rank 0 only */
        if (rank == 0) {
            my_nreqs += 27;
            /* post nonblocking requests using nc_get_varn() */
            err = read_small_vars_F_case(ncid, i, varids, rec_no, gap,
                                          dims[2][0], dims[2][0]+1, 2, 8,
                                          &int_buf_ptr, &txt_buf_ptr,
                                          &dbl_buf_ptr);
            ERR
        }
        i += 27;

        post_timing += MPI_Wtime() - timing;

        MPI_Barrier(io_comm); /*-----------------------------------------*/
        timing = MPI_Wtime();

        /* flush fixed-size and small variables */
        err = nc_wait_all(ncid, NC_GET_REQ_ALL, NULL); ERR

        wait_timing += MPI_Wtime() - timing;

        MPI_Barrier(io_comm); /*-----------------------------------------*/
        timing = MPI_Wtime();

        rec_buf_ptr = rec_buf;

        for (j=0; j<xnreqs[1]; j++) starts_D2[j][0] = rec_no;
        for (j=0; j<xnreqs[2]; j++) starts_D3[j][0] = rec_no;

        if (nvars == 408) {
            if (two_buf) {
                /* read 2D variables */
                POST_VARN_RD(2,   1,  30)   /* AEROD_v */
                POST_VARN_RD(2,  18,  33)   /* AODABS ... ANSNOW */
                POST_VARN_RD(2,   6,  53)   /* AQ_DMS ... AQ_SOAG */
                POST_VARN_RD(2,   2,  64)   /* CDNUMC and CLDHGH */
                POST_VARN_RD(2,   3,  68)   /* CLDLOW ... CLDTOT */
                POST_VARN_RD(2,  11,  75)   /* DF_DMS ... DSTSFMBL */
                POST_VARN_RD(2,   2,  87)   /* DTENDTH and DTENDTQ */
                POST_VARN_RD(2,   7,  91)   /* FLDS ... FLUTC */
                POST_VARN_RD(2,  15, 102)   /* FSDS ... ICEFRAC */
                POST_VARN_RD(2,   2, 120)   /* LANDFRAC and LHFLX */
                POST_VARN_RD(2,   3, 127)   /* LINOZ_SZA ... LWCF */
                POST_VARN_RD(2,   2, 142)   /* O3_SRF and OCNFRAC */
                POST_VARN_RD(2,   1, 145)   /* OMEGA500 */
                POST_VARN_RD(2,   8, 147)   /* PBLH ... PSL */
                POST_VARN_RD(2,   2, 156)   /* QFLX and QREFHT */
                POST_VARN_RD(2,   1, 161)   /* RAM1 */
                POST_VARN_RD(2,  37, 163)   /* SFDMS ... SNOWHLND */
                POST_VARN_RD(2,  10, 202)   /* SO2_CLXF ... SWCF */
                POST_VARN_RD(2,  19, 213)   /* TAUGWX ... TVQ */
                POST_VARN_RD(2,   1, 233)   /* U10 */
                POST_VARN_RD(2,   3, 240)   /* WD_H2O2 ... WD_SO2 */
                POST_VARN_RD(2,  32, 246)   /* airFV ... dst_c3SFWET */
                POST_VARN_RD(2, 129, 279)   /* mlip ... soa_c3SFWET */
                /* read 3D variables */
                POST_VARN_RD(3,   2,  31)   /* ANRAIN and ANSNOW */
                POST_VARN_RD(3,   2,  51)   /* AQRAIN and AQSNOW */
                POST_VARN_RD(3,   5,  59)   /* AREI ... CCN3 */
                POST_VARN_RD(3,   2,  66)   /* CLDICE and CLDLIQ */
                POST_VARN_RD(3,   4,  71)   /* CLOUD ... DCQ */
                POST_VARN_RD(3,   1,  86)   /* DTCOND */
                POST_VARN_RD(3,   2,  89)   /* EXTINCT and FICE */
                POST_VARN_RD(3,   4,  98)   /* FREQI ... FREQS */
                POST_VARN_RD(3,   3, 117)   /* ICIMR ... IWC */
                POST_VARN_RD(3,   5, 122)   /* LINOZ_DO3 ... LINOZ_SSO3 */
                POST_VARN_RD(3,  12, 130)   /* Mass_bc ... O3 */
                POST_VARN_RD(3,   1, 144)   /* OMEGA */
                POST_VARN_RD(3,   1, 146)   /* OMEGAT */
                POST_VARN_RD(3,   1, 155)   /* Q */
                POST_VARN_RD(3,   3, 158)   /* QRL ... RAINQM */
                POST_VARN_RD(3,   1, 162)   /* RELHUM */
                POST_VARN_RD(3,   2, 200)   /* SNOWQM and SO2 */
                POST_VARN_RD(3,   1, 212)   /* T */
                POST_VARN_RD(3,   1, 232)   /* U */
                POST_VARN_RD(3,   6, 234)   /* UU ... VV */
                POST_VARN_RD(3,   3, 243)   /* WSUB ... aero_water */
                POST_VARN_RD(3,   1, 278)   /* hstobie_linoz */
            } else {
                /* read variables in the same order as they inqueryd */
                POST_VARN_RD(2,   1,  30)   /* AEROD_v */
                POST_VARN_RD(3,   2,  31)   /* ANRAIN and ANSNOW */
                POST_VARN_RD(2,  18,  33)   /* AODABS ... ANSNOW */
                POST_VARN_RD(3,   2,  51)   /* AQRAIN and AQSNOW */
                POST_VARN_RD(2,   6,  53)   /* AQ_DMS ... AQ_SOAG */
                POST_VARN_RD(3,   5,  59)   /* AREI ... CCN3 */
                POST_VARN_RD(2,   2,  64)   /* CDNUMC and CLDHGH */
                POST_VARN_RD(3,   2,  66)   /* CLDICE and CLDLIQ */
                POST_VARN_RD(2,   3,  68)   /* CLDLOW ... CLDTOT */
                POST_VARN_RD(3,   4,  71)   /* CLOUD ... DCQ */
                POST_VARN_RD(2,  11,  75)   /* DF_DMS ... DSTSFMBL */
                POST_VARN_RD(3,   1,  86)   /* DTCOND */
                POST_VARN_RD(2,   2,  87)   /* DTENDTH and DTENDTQ */
                POST_VARN_RD(3,   2,  89)   /* EXTINCT and FICE */
                POST_VARN_RD(2,   7,  91)   /* FLDS ... FLUTC */
                POST_VARN_RD(3,   4,  98)   /* FREQI ... FREQS */
                POST_VARN_RD(2,  15, 102)   /* FSDS ... ICEFRAC */
                POST_VARN_RD(3,   3, 117)   /* ICIMR ... IWC */
                POST_VARN_RD(2,   2, 120)   /* LANDFRAC and LHFLX */
                POST_VARN_RD(3,   5, 122)   /* LINOZ_DO3 ... LINOZ_SSO3 */
                POST_VARN_RD(2,   3, 127)   /* LINOZ_SZA ... LWCF */
                POST_VARN_RD(3,  12, 130)   /* Mass_bc ... O3 */
                POST_VARN_RD(2,   2, 142)   /* O3_SRF and OCNFRAC */
                POST_VARN_RD(3,   1, 144)   /* OMEGA */
                POST_VARN_RD(2,   1, 145)   /* OMEGA500 */
                POST_VARN_RD(3,   1, 146)   /* OMEGAT */
                POST_VARN_RD(2,   8, 147)   /* PBLH ... PSL */
                POST_VARN_RD(3,   1, 155)   /* Q */
                POST_VARN_RD(2,   2, 156)   /* QFLX and QREFHT */
                POST_VARN_RD(3,   3, 158)   /* QRL ... RAINQM */
                POST_VARN_RD(2,   1, 161)   /* RAM1 */
                POST_VARN_RD(3,   1, 162)   /* RELHUM */
                POST_VARN_RD(2,  37, 163)   /* SFDMS ... SNOWHLND */
                POST_VARN_RD(3,   2, 200)   /* SNOWQM and SO2 */
                POST_VARN_RD(2,  10, 202)   /* SO2_CLXF ... SWCF */
                POST_VARN_RD(3,   1, 212)   /* T */
                POST_VARN_RD(2,  19, 213)   /* TAUGWX ... TVQ */
                POST_VARN_RD(3,   1, 232)   /* U */
                POST_VARN_RD(2,   1, 233)   /* U10 */
                POST_VARN_RD(3,   6, 234)   /* UU ... VV */
                POST_VARN_RD(2,   3, 240)   /* WD_H2O2 ... WD_SO2 */
                POST_VARN_RD(3,   3, 243)   /* WSUB ... aero_water */
                POST_VARN_RD(2,  32, 246)   /* airFV ... dst_c3SFWET */
                POST_VARN_RD(3,   1, 278)   /* hstobie_linoz */
                POST_VARN_RD(2, 129, 279)   /* mlip ... soa_c3SFWET */
            }
        }
        else {
            if (two_buf) {
                /* read 2D variables followed by 3D variables */
                POST_VARN_RD(2, 13, 30)   /* CLDHGH ... T5 */
                POST_VARN_RD(2,  7, 44)   /* U250 ... Z500 */
                POST_VARN_RD(3,  1, 43)   /* U */
            } else {
                /* read variables in the same order as they inqueryd */
                POST_VARN_RD(2, 13, 30)   /* CLDHGH ... T5 */
                POST_VARN_RD(3,  1, 43)   /* U */
                POST_VARN_RD(2,  7, 44)   /* U250 ... Z500 */
            }
        }

        post_timing += MPI_Wtime() - timing;

        MPI_Barrier(io_comm); /*-----------------------------------------*/
        timing = MPI_Wtime();

        err = nc_wait_all(ncid, NC_GET_REQ_ALL, NULL); ERR

        wait_timing += MPI_Wtime() - timing;
    }

    MPI_Barrier(io_comm); /*-----------------------------------------*/
    timing = MPI_Wtime();

    err = nc_inq_get_size(ncid, &total_size); ERR
    get_size = total_size - metadata_size;
    err = nc_close(ncid); ERR
    close_timing += MPI_Wtime() - timing;

    if (starts_D3 != NULL) {
        free(starts_D3[0]);
        free(starts_D3);
    }
    if (starts_D2 != NULL) {
        free(starts_D2[0]);
        free(starts_D2);
    }
    if (rec_bufp == NULL && rec_buf != NULL) free(rec_buf);
    if (dbl_bufp == NULL && dbl_buf != NULL) free(dbl_buf);
    free(varids);

    total_timing = MPI_Wtime() - total_timing;

    tmp = my_nreqs;
    MPI_Reduce(&tmp,           &max_nreqs,  1, MPI_OFFSET, MPI_MAX, 0, io_comm);
    MPI_Reduce(&tmp,           &total_nreqs,1, MPI_OFFSET, MPI_SUM, 0, io_comm);
    MPI_Reduce(&get_size,      &tmp,        1, MPI_OFFSET, MPI_SUM, 0, io_comm);
    get_size = tmp;
    MPI_Reduce(&total_size,    &tmp,        1, MPI_OFFSET, MPI_SUM, 0, io_comm);
    total_size = tmp;
    MPI_Reduce(&open_timing,   &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    open_timing = max_timing;
    MPI_Reduce(&pre_timing,    &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    pre_timing = max_timing;
    MPI_Reduce(&post_timing,   &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    post_timing = max_timing;
    MPI_Reduce(&wait_timing,   &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    wait_timing = max_timing;
    MPI_Reduce(&close_timing,  &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    close_timing = max_timing;
    MPI_Reduce(&total_timing,  &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    total_timing = max_timing;

    /* check if there is any PnetCDF internal malloc residue */
    MPI_Offset malloc_size, sum_size;
    err = nc_inq_malloc_size(&malloc_size);
    if (err == NC_NOERR) {
        MPI_Reduce(&malloc_size, &sum_size, 1, MPI_OFFSET, MPI_SUM, 0, io_comm);
        if (rank == 0 && sum_size > 0) {
            printf("-----------------------------------------------------------\n");
            printf("heap memory allocated by PnetCDF internally has %lld bytes yet to be freed\n",
                   sum_size);
        }
    }
    MPI_Offset m_alloc=0, max_alloc;
    nc_inq_malloc_max_size(&m_alloc);
    MPI_Reduce(&m_alloc, &max_alloc, 1, MPI_OFFSET, MPI_MAX, 0, io_comm);
    if (rank == 0) {
        printf("History input file postfix        = %s\n", in_postfix);
        printf("#%%$: Read_max_heap_mib: %.2f\n", (float)max_alloc/1048576);
        printf("#%%$: Read_nvar: %d\n", nvars);
        printf("#%%$: Read_size_mib: %.2f\n", (double)total_size/1048576);
        printf("#%%$: Read_size_gib: %.2f\n", (double)total_size/1073741824);
        printf("#%%$: Read_nreq_sum: %lld\n", total_nreqs);
        printf("#%%$: Read_nreq_max: %lld\n", max_nreqs);
        printf("#%%$: Read_open_metadata_time: %.4f\n", open_timing);
        printf("#%%$: Read_io_prepare_time: %.4f\n", pre_timing);
        printf("#%%$: Read_req_post_time: %.4f\n", post_timing);
        printf("#%%$: Read_req_wait_time: %.4f\n", wait_timing);
        printf("#%%$: Read_close_time: %.4f\n", close_timing);
        printf("#%%$: Read_total_time: %.4f\n", total_timing);
        printf("#%%$: Read_bandwidth_open_to_close_mib: %.4f\n", (double)total_size/1048576.0/total_timing);
        printf("#%%$: Read_bandwidth_wait_mib: %.4f\n", (double)get_size/1048576.0/wait_timing);
        if (verbose) print_info(&info_used);
        printf("-----------------------------------------------------------\n");
    }

fn_exit:
    if (info_used != MPI_INFO_NULL) MPI_Info_free(&info_used);
    fflush(stdout);
    MPI_Barrier(io_comm);
    return nerrs;
}