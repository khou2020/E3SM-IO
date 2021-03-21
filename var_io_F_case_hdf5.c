/*********************************************************************
 *
 * Copyright (C) 2018, Northwestern University
 * See COPYRIGHT notice in top-level directory.
 *
 * This program uses the E3SM I/O patterns recorded by the PIO library to
 * evaluate the performance of HDF5 APIs. The E3SM I/O patterns consist
 * of a large number of small, noncontiguous requests on each MPI process,
 * which presents a challenge for achieving a good performance.
 *
 * See README.md for compile and run instructions.
 *
 *********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h> /* unlink() */

#include "e3sm_io_hdf5.h"

#ifdef ENABLE_LOGVOL
#include "H5VL_log.h"
#endif
#include <sys/stat.h>

int hdf5_put_varn_mpi (int vid,
                   MPI_Datatype mpitype,
                   hid_t dxplid,
                   int cnt,
                   int max_cnt,
                   MPI_Offset **mstarts,
                   MPI_Offset **mcounts,
                   void *buf);

int flush_multidatasets();
int dataspace_recycle_all();
int memspace_recycle_all();


/*----< write_small_vars_F_case_hdf5() >------------------------------------------*/
static int write_small_vars_F_case_hdf5 (hid_t ncid,
                                         int rank,
                                         int vid, /* starting variable ID */
                                         int *varids,
                                         hsize_t rec_no,
                                         int gap,
                                         MPI_Offset lev,
                                         MPI_Offset ilev,
                                         MPI_Offset nbnd,
                                         MPI_Offset nchars,
                                         int **int_buf,
                                         char **txt_buf,
                                         double **dbl_buf) {
    int i, err, nerrs = 0;
    hsize_t start[2], count[2];

    /* scalar and small variables are written by rank 0 only */
    i = vid;

    if (rec_no == 0 && rank == 0) {
        /* lev */
        err = HDF5_IPUT_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += lev + gap;
        /* hyam */
        err = HDF5_IPUT_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += lev + gap;
        /* hybm */
        err = HDF5_IPUT_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += lev + gap;
        /* P0 */
        err = HDF5_IPUT_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += 1 + gap;
        /* ilev */
        err = HDF5_IPUT_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += ilev + gap;
        /* hyai */
        err = HDF5_IPUT_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += ilev + gap;
        /* hybi */
        err = HDF5_IPUT_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += ilev + gap;
    } else
        i += 7;

    /* time */
    start[0] = rec_no;
    err      = HDF5_IPUT_VAR1_DOUBLE_MPI (ncid, varids[i++], start, *dbl_buf, rank);
    ERR
    *dbl_buf += 1 + gap;
    /* date */
    start[0] = rec_no;
    err      = HDF5_IPUT_VAR1_INT_MPI (ncid, varids[i++], start, *int_buf, rank);
    ERR
    *int_buf += 1;
    /* datesec */
    start[0] = rec_no;
    err      = HDF5_IPUT_VAR1_INT_MPI (ncid, varids[i++], start, *int_buf, rank);
    ERR
    *int_buf += 1;
    /* time_bnds */
    start[0] = rec_no;
    start[1] = 0;
    count[0] = 1;
    count[1] = nbnd;
    err      = HDF5_IPUT_VARA_DOUBLE_MPI (ncid, varids[i++], start, count, *dbl_buf, rank);
    ERR
    *dbl_buf += nbnd + gap;
    /* date_written */
    start[0] = rec_no;
    start[1] = 0;
    count[0] = 1;
    count[1] = nchars;
    err      = HDF5_IPUT_VARA_TEXT_MPI (ncid, varids[i++], start, count, *txt_buf, rank);
    ERR
    *txt_buf += nchars;
    /* time_written */
    start[0] = rec_no;
    start[1] = 0;
    count[0] = 1;
    count[1] = nchars;
    err      = HDF5_IPUT_VARA_TEXT_MPI (ncid, varids[i++], start, count, *txt_buf, rank);
    ERR
    *txt_buf += nchars;

    if (rec_no == 0 && rank == 0) {
        /* ndbase */
        err = HDF5_IPUT_VAR_INT (ncid, varids[i++], *int_buf, NULL);
        ERR
        *int_buf += 1;
        /* nsbase */
        err = HDF5_IPUT_VAR_INT (ncid, varids[i++], *int_buf, NULL);
        ERR
        *int_buf += 1;
        /* nbdate */
        err = HDF5_IPUT_VAR_INT (ncid, varids[i++], *int_buf, NULL);
        ERR
        *int_buf += 1;
        /* nbsec */
        err = HDF5_IPUT_VAR_INT (ncid, varids[i++], *int_buf, NULL);
        ERR
        *int_buf += 1;
        /* mdt */
        err = HDF5_IPUT_VAR_INT (ncid, varids[i++], *int_buf, NULL);
        ERR
        *int_buf += 1;
    } else
        i += 5;

    /* ndcur */
    start[0] = rec_no;
    err      = HDF5_IPUT_VAR1_INT_MPI (ncid, varids[i++], start, *int_buf, rank);
    ERR
    *int_buf += 1;
    /* nscur */
    start[0] = rec_no;
    err      = HDF5_IPUT_VAR1_INT_MPI (ncid, varids[i++], start, *int_buf, rank);
    ERR
    *int_buf += 1;
    /* co2vmr */
    start[0] = rec_no;
    err      = HDF5_IPUT_VAR1_DOUBLE_MPI (ncid, varids[i++], start, *dbl_buf, rank);
    ERR
    *dbl_buf += 1 + gap;
    /* ch4vmr */
    start[0] = rec_no;
    err      = HDF5_IPUT_VAR1_DOUBLE_MPI (ncid, varids[i++], start, *dbl_buf, rank);
    ERR
    *dbl_buf += 1 + gap;
    /* n2ovmr */
    start[0] = rec_no;
    err      = HDF5_IPUT_VAR1_DOUBLE_MPI (ncid, varids[i++], start, *dbl_buf, rank);
    ERR
    *dbl_buf += 1 + gap;
    /* f11vmr */
    start[0] = rec_no;
    err      = HDF5_IPUT_VAR1_DOUBLE_MPI (ncid, varids[i++], start, *dbl_buf, rank);
    ERR
    *dbl_buf += 1 + gap;
    /* f12vmr */
    start[0] = rec_no;
    err      = HDF5_IPUT_VAR1_DOUBLE_MPI (ncid, varids[i++], start, *dbl_buf, rank);
    ERR
    *dbl_buf += 1 + gap;
    /* sol_tsi */
    start[0] = rec_no;
    err      = HDF5_IPUT_VAR1_DOUBLE_MPI (ncid, varids[i++], start, *dbl_buf, rank);
    ERR
    *dbl_buf += 1 + gap;
    /* nsteph */
    start[0] = rec_no;
    err      = HDF5_IPUT_VAR1_INT_MPI (ncid, varids[i++], start, *int_buf, rank);
    ERR
    *int_buf += 1;
fn_exit:
    return err;
}

/*----< read_small_vars_F_case_hdf5() >------------------------------------------*/
static int read_small_vars_F_case_hdf5 (hid_t ncid,
                                        int vid, /* starting variable ID */
                                        int *varids,
                                        hsize_t rec_no,
                                        int gap,
                                        MPI_Offset lev,
                                        MPI_Offset ilev,
                                        MPI_Offset nbnd,
                                        MPI_Offset nchars,
                                        int **int_buf,
                                        char **txt_buf,
                                        double **dbl_buf) {
    int i, err, nerrs = 0;
    hsize_t start[2], count[2];
    /* scalar and small variables are written by rank 0 only */
    i = vid;

    if (rec_no == 0) {
        /* lev */
        err = HDF5_IGET_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += lev + gap;
        /* hyam */
        err = HDF5_IGET_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += lev + gap;
        /* hybm */
        err = HDF5_IGET_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += lev + gap;
        /* P0 */
        err = HDF5_IGET_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += 1 + gap;
        /* ilev */
        err = HDF5_IGET_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += ilev + gap;
        /* hyai */
        err = HDF5_IGET_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += ilev + gap;
        /* hybi */
        err = HDF5_IGET_VAR_DOUBLE (ncid, varids[i++], *dbl_buf, NULL);
        ERR
        *dbl_buf += ilev + gap;
    } else
        i += 7;

    /* time */
    start[0] = rec_no;
    err      = HDF5_IGET_VAR1_DOUBLE (ncid, varids[i++], start, *dbl_buf, NULL);
    ERR
    *dbl_buf += 1 + gap;
    /* date */
    start[0] = rec_no;
    err      = HDF5_IGET_VAR1_INT (ncid, varids[i++], start, *int_buf, NULL);
    ERR
    *int_buf += 1;
    /* datesec */
    start[0] = rec_no;
    err      = HDF5_IGET_VAR1_INT (ncid, varids[i++], start, *int_buf, NULL);
    ERR
    *int_buf += 1;
    /* time_bnds */
    start[0] = rec_no;
    start[1] = 0;
    count[0] = 1;
    count[1] = nbnd;
    err      = HDF5_IGET_VARA_DOUBLE (ncid, varids[i++], start, count, *dbl_buf, NULL);
    ERR
    *dbl_buf += nbnd + gap;
    /* date_written */
    start[0] = rec_no;
    start[1] = 0;
    count[0] = 1;
    count[1] = nchars;
    err      = HDF5_IGET_VARA_TEXT (ncid, varids[i++], start, count, *txt_buf, NULL);
    ERR
    *txt_buf += nchars;
    /* time_written */
    start[0] = rec_no;
    start[1] = 0;
    count[0] = 1;
    count[1] = nchars;
    err      = HDF5_IGET_VARA_TEXT (ncid, varids[i++], start, count, *txt_buf, NULL);
    ERR
    *txt_buf += nchars;

    if (rec_no == 0) {
        /* ndbase */
        err = HDF5_IGET_VAR_INT (ncid, varids[i++], *int_buf, NULL);
        ERR
        *int_buf += 1;
        /* nsbase */
        err = HDF5_IGET_VAR_INT (ncid, varids[i++], *int_buf, NULL);
        ERR
        *int_buf += 1;
        /* nbdate */
        err = HDF5_IGET_VAR_INT (ncid, varids[i++], *int_buf, NULL);
        ERR
        *int_buf += 1;
        /* nbsec */
        err = HDF5_IGET_VAR_INT (ncid, varids[i++], *int_buf, NULL);
        ERR
        *int_buf += 1;
        /* mdt */
        err = HDF5_IGET_VAR_INT (ncid, varids[i++], *int_buf, NULL);
        ERR
        *int_buf += 1;
    } else
        i += 5;

    /* ndcur */
    start[0] = rec_no;
    err      = HDF5_IGET_VAR1_INT (ncid, varids[i++], start, *int_buf, NULL);
    ERR
    *int_buf += 1;
    /* nscur */
    start[0] = rec_no;
    err      = HDF5_IGET_VAR1_INT (ncid, varids[i++], start, *int_buf, NULL);
    ERR
    *int_buf += 1;
    /* co2vmr */
    start[0] = rec_no;
    err      = HDF5_IGET_VAR1_DOUBLE (ncid, varids[i++], start, *dbl_buf, NULL);
    ERR
    *dbl_buf += 1 + gap;
    /* ch4vmr */
    start[0] = rec_no;
    err      = HDF5_IGET_VAR1_DOUBLE (ncid, varids[i++], start, *dbl_buf, NULL);
    ERR
    *dbl_buf += 1 + gap;
    /* n2ovmr */
    start[0] = rec_no;
    err      = HDF5_IGET_VAR1_DOUBLE (ncid, varids[i++], start, *dbl_buf, NULL);
    ERR
    *dbl_buf += 1 + gap;
    /* f11vmr */
    start[0] = rec_no;
    err      = HDF5_IGET_VAR1_DOUBLE (ncid, varids[i++], start, *dbl_buf, NULL);
    ERR
    *dbl_buf += 1 + gap;
    /* f12vmr */
    start[0] = rec_no;
    err      = HDF5_IGET_VAR1_DOUBLE (ncid, varids[i++], start, *dbl_buf, NULL);
    ERR
    *dbl_buf += 1 + gap;
    /* sol_tsi */
    start[0] = rec_no;
    err      = HDF5_IGET_VAR1_DOUBLE (ncid, varids[i++], start, *dbl_buf, NULL);
    ERR
    *dbl_buf += 1 + gap;
    /* nsteph */
    start[0] = rec_no;
    err      = HDF5_IGET_VAR1_INT (ncid, varids[i++], start, *int_buf, NULL);
    ERR
    *int_buf += 1;
fn_exit:
    return err;
}
#define FIX_1D_VAR_STARTS_COUNTS(starts, counts, nreqs, disps, blocklens)      \
    {                                                                          \
        starts    = (MPI_Offset **)malloc (2 * nreqs * sizeof (MPI_Offset *)); \
        counts    = starts + nreqs;                                            \
        starts[0] = (MPI_Offset *)malloc (2 * nreqs * sizeof (MPI_Offset));    \
        counts[0] = starts[0] + nreqs;                                         \
                                                                               \
        for (j = 1; j < nreqs; j++) {                                          \
            starts[j] = starts[j - 1] + 1;                                     \
            counts[j] = counts[j - 1] + 1;                                     \
        }                                                                      \
                                                                               \
        for (j = 0; j < nreqs; j++) {                                          \
            starts[j][0] = disps[j];                                           \
            counts[j][0] = blocklens[j];                                       \
        }                                                                      \
    }

#define FIX_2D_VAR_STARTS_COUNTS(starts, counts, nreqs, disps, blocklens, last_dimlen)             \
    {                                                                                              \
        starts    = (MPI_Offset **)malloc (2 * nreqs * sizeof (MPI_Offset *));                     \
        counts    = starts + nreqs;                                                                \
        starts[0] = (MPI_Offset *)malloc (2 * nreqs * 2 * sizeof (MPI_Offset));                    \
        counts[0] = starts[0] + nreqs * 2;                                                         \
                                                                                                   \
        for (j = 1; j < nreqs; j++) {                                                              \
            starts[j] = starts[j - 1] + 2;                                                         \
            counts[j] = counts[j - 1] + 2;                                                         \
        }                                                                                          \
                                                                                                   \
        k            = 0;                                                                          \
        starts[0][0] = disps[0] / last_dimlen;                                                     \
        starts[0][1] = disps[0] % last_dimlen; /* decomposition is 2D */                           \
        counts[0][0] = 1;                                                                          \
        counts[0][1] = blocklens[0]; /* each blocklens[j] is no bigger than last_dimlen */         \
        for (j = 1; j < nreqs; j++) {                                                              \
            MPI_Offset _start[2];                                                                  \
            _start[0] = disps[j] / last_dimlen;                                                    \
            _start[1] = disps[j] % last_dimlen;                                                    \
            if (_start[0] == starts[k][0] + counts[k][0] && _start[1] == starts[k][1] &&           \
                blocklens[j] == counts[k][1])                                                      \
                counts[k][0]++;                                                                    \
            else {                                                                                 \
                k++;                                                                               \
                starts[k][0] = _start[0];                                                          \
                starts[k][1] = _start[1];                                                          \
                counts[k][0] = 1;                                                                  \
                counts[k][1] = blocklens[j]; /* each blocklens[j] is no bigger than last_dimlen */ \
            }                                                                                      \
        }                                                                                          \
        nreqs = k + 1;                                                                             \
    }

#define REC_2D_VAR_STARTS_COUNTS(rec, starts, counts, nreqs, disps, blocklens)  \
    {                                                                           \
        starts    = (MPI_Offset **)malloc (2 * nreqs * sizeof (MPI_Offset *));  \
        counts    = starts + nreqs;                                             \
        starts[0] = (MPI_Offset *)malloc (2 * nreqs * 2 * sizeof (MPI_Offset)); \
        counts[0] = starts[0] + nreqs * 2;                                      \
                                                                                \
        for (j = 1; j < nreqs; j++) {                                           \
            starts[j] = starts[j - 1] + 2;                                      \
            counts[j] = counts[j - 1] + 2;                                      \
        }                                                                       \
                                                                                \
        for (j = 0; j < nreqs; j++) {                                           \
            starts[j][1] = disps[j]; /* decomposition is 1D */                  \
            counts[j][1] = blocklens[j];                                        \
                                                                                \
            starts[j][0] = rec; /* record ID */                                 \
            counts[j][0] = 1;   /* one record only */                           \
        }                                                                       \
    }

#define REC_3D_VAR_STARTS_COUNTS(rec, starts, counts, nreqs, disps, blocklens, last_dimlen)        \
    {                                                                                              \
        starts    = (MPI_Offset **)malloc (2 * nreqs * sizeof (MPI_Offset *));                     \
        counts    = starts + nreqs;                                                                \
        starts[0] = (MPI_Offset *)malloc (2 * nreqs * 3 * sizeof (MPI_Offset));                    \
        counts[0] = starts[0] + nreqs * 3;                                                         \
                                                                                                   \
        for (j = 1; j < nreqs; j++) {                                                              \
            starts[j] = starts[j - 1] + 3;                                                         \
            counts[j] = counts[j - 1] + 3;                                                         \
        }                                                                                          \
                                                                                                   \
        k            = 0;                                                                          \
        starts[0][0] = rec; /* record ID */                                                        \
        starts[0][1] = disps[0] / last_dimlen;                                                     \
        starts[0][2] = disps[0] % last_dimlen; /* decomposition is 2D */                           \
        counts[0][0] = 1;                      /* one record only */                               \
        counts[0][1] = 1;                                                                          \
        counts[0][2] = blocklens[0]; /* each blocklens[j] is no bigger than last_dimlen */         \
        for (j = 1; j < nreqs; j++) {                                                              \
            MPI_Offset _start[2];                                                                  \
            _start[0] = disps[j] / last_dimlen;                                                    \
            _start[1] = disps[j] % last_dimlen;                                                    \
            if (starts[k][0] == rec && _start[0] == starts[k][1] + counts[k][1] &&                 \
                _start[1] == starts[k][2] && blocklens[j] == counts[k][2])                         \
                counts[k][1]++;                                                                    \
            else {                                                                                 \
                k++;                                                                               \
                starts[k][0] = rec;                                                                \
                starts[k][1] = _start[0];                                                          \
                starts[k][2] = _start[1];                                                          \
                counts[k][0] = 1;                                                                  \
                counts[k][1] = 1;                                                                  \
                counts[k][2] = blocklens[j]; /* each blocklens[j] is no bigger than last_dimlen */ \
            }                                                                                      \
        }                                                                                          \
        nreqs = k + 1;                                                                             \
    }
/*
        err = HDF5_IPUT_VARN (ncid, vid + j, xnreqs[k - 1], starts_D##k, counts_D##k, rec_buf_ptr, \
                              -1, REC_ITYPE, NULL);                                                \
*/

#define POST_VARN(k, num, vid)                                                            \
    for (j = 0; j < num; j++) {                                                                    \
        hdf5_put_varn_mpi (vid + j, REC_ITYPE, dxplid_coll, xnreqs[k - 1], max_cnt[k - 1], starts_D##k, counts_D##k, rec_buf_ptr);\
        ERR                                                                                        \
        rec_buf_ptr += nelems[k - 1] + gap;                                                        \
        my_nreqs += xnreqs[k - 1];                                                                 \
        if (rec_no == 0) nvars_D[k - 1]++;                                                         \
    }

/*----< run_varn_F_case() >--------------------------------------------------*/
int run_varn_F_case_hdf5 (
    MPI_Comm io_comm,    /* MPI communicator that includes all the tasks involved in IO */
    const char *out_dir, /* output folder name */
    const char *outfile, /* output file name */
    int nvars,           /* number of variables 414 or 51 */
    int num_recs,        /* number of records */
    int noncontig_buf,   /* whether to us noncontiguous buffer */
    MPI_Info info,
    MPI_Offset dims[3][2],   /* dimension lengths */
    const int nreqs[3],      /* no. request in decompositions 1,2,3 */
    int *const disps[3],     /* request's displacements */
    int *const blocklens[3], /* request's block lengths */
    double *dbl_bufp,        /* buffer for fixed size double var */
    itype *rec_bufp,         /* buffer for rec floating point var */
    char *txt_buf,           /* buffer for char var */
    int *int_buf)            /* buffer for int var */
{
    char outfname[512], *txt_buf_ptr;
    herr_t herr;
    hid_t ncid, faplid, faplid_indp, fcplid_indp;
    int i, j, k, err, nerrs = 0, rank, cmode, *varids, nvars_D[3];
    int rec_no, gap = 0, my_nreqs, *int_buf_ptr, xnreqs[3], max_cnt[3];
    size_t dbl_buflen, rec_buflen, nelems[3];
    itype *rec_buf  = NULL, *rec_buf_ptr;
    double *dbl_buf = NULL, *dbl_buf_ptr;
    double pre_timing, open_timing, post_timing, wait_timing, close_timing, small_write_timing;
    double timing, total_timing, max_timing;
    MPI_Offset tmp, metadata_size, put_size, total_size, max_nreqs, total_nreqs;
    MPI_Offset **starts_D2 = NULL, **counts_D2 = NULL;
    MPI_Offset **starts_D3 = NULL, **counts_D3 = NULL;
    MPI_Info info_used = MPI_INFO_NULL;
    struct stat file_stat;

    MPI_Barrier (io_comm); /*-----------------------------------------*/
    total_timing = pre_timing = MPI_Wtime ();

    open_timing  = 0.0;
    post_timing  = 0.0;
    wait_timing  = 0.0;
    close_timing = 0.0;
    small_write_timing = .0;

    MPI_Comm_rank (io_comm, &rank);

    if (noncontig_buf) gap = 10;

    varids = (int *)malloc (nvars * sizeof (int));

    for (i = 0; i < 3; i++) {
        xnreqs[i]  = nreqs[i];
        nvars_D[i] = 0; /* number of variables using decomposition i */
    }

    /* calculate number of variable elements from 3 decompositions */
    my_nreqs = 0;
    for (i = 0; i < 3; i++) {
        for (nelems[i] = 0, k = 0; k < xnreqs[i]; k++) nelems[i] += blocklens[i][k];
    }
    if (verbose && rank == 0) printf ("nelems=%zd %zd %zd\n", nelems[0], nelems[1], nelems[2]);

    /* allocate and initialize write buffer for small variables */
    dbl_buflen =
        nelems[1] * 2 + nelems[0] + 3 * dims[2][0] + 3 * (dims[2][0] + 1) + 8 + 2 + 20 * gap;
    if (dbl_bufp != NULL) {
        dbl_buf = dbl_bufp;
    } else {
        dbl_buf = (double *)malloc (dbl_buflen * sizeof (double));
        for (i = 0; i < dbl_buflen; i++) dbl_buf[i] = rank;
    }

    /* allocate and initialize write buffer for large variables */
    if (nvars == 414)
        rec_buflen = nelems[1] * 321 + nelems[2] * 63 + (321 + 63) * gap;
    else
        rec_buflen = nelems[1] * 20 + nelems[2] + (20 + 1) * gap;

    if (rec_bufp != NULL) {
        rec_buf = rec_bufp;
    } else {
        rec_buf = (itype *)malloc (rec_buflen * sizeof (itype));

        for (i = 0; i < rec_buflen; i++) rec_buf[i] = rank;
        for (i = 0; i < 10; i++) int_buf[i] = rank;
        for (i = 0; i < 16; i++) txt_buf[i] = 'a' + rank;

    }

    pre_timing = MPI_Wtime () - pre_timing;

    MPI_Barrier (io_comm); /*-----------------------------------------*/
    timing = MPI_Wtime ();

    /* set output file name */
    sprintf (outfname, "%s/%s", out_dir, outfile);

    err = hdf5_wrap_init ();
    ERR;


    // We are going to let rank 0 to handle dataset init and attributes alone. This could be faster.
    double start = MPI_Wtime();
    if (!rank) {
        faplid_indp = H5Pcreate (H5P_FILE_ACCESS);
        fcplid_indp = H5Pcreate (H5P_FILE_CREATE);
        ncid = H5Fcreate (outfname, H5F_ACC_TRUNC, fcplid_indp, faplid_indp);
        if (nvars == 414) {
            /* for h0 file */
            err = def_F_case_h0_hdf5 (ncid, dims[2], nvars, varids);
            ERR
        } else {
            /* for h1 file */
            err = def_F_case_h1_hdf5 (ncid, dims[2], nvars, varids);
            ERR
        }
        start = MPI_Wtime() - start;
        printf("dataset creation time %lf\n", start);
        /* exit define mode and enter data mode */
        err = HDF5_NOP1 (ncid);
        ERR
        herr = hdf5_close_vars (ncid);
        H5Pclose(faplid_indp);
        H5Pclose(fcplid_indp);
        H5Fclose(ncid);
    }
    MPI_Barrier(io_comm);
    // Now collectively open the datasets just created
    start = MPI_Wtime();
    faplid = H5Pcreate (H5P_FILE_ACCESS);
    H5Pset_fapl_mpio (faplid, io_comm, info);
    H5Pset_all_coll_metadata_ops (faplid, 1);
    ncid = H5Fopen (outfname, H5F_ACC_RDWR, faplid);
    if (nvars == 414) {
        /* for h0 file */
        err = def_F_case_h0_hdf5_mpi (ncid, dims[2], nvars, varids);
        ERR
    } else {
        /* for h1 file */
        err = def_F_case_h1_hdf5_mpi (ncid, dims[2], nvars, varids);
        ERR
    }
    err = HDF5_NOP1 (ncid);
    ERR
    CHECK_HID (ncid)

    herr = hdf5_close_vars (ncid);
    HERR;
    herr = H5Fclose (ncid);
    HERR;
    herr = H5Pclose (faplid);
    start = MPI_Wtime() - start;
    printf("dataset open time %lf\n", start);

/*
    faplid = H5Pcreate (H5P_FILE_ACCESS);
    // MPI and collective metadata is required by LOG VOL
    H5Pset_fapl_mpio (faplid, io_comm, info);
    H5Pset_all_coll_metadata_ops (faplid, 1);
#ifdef ENABLE_LOGVOL
    H5Pset_vol (faplid, log_vlid, NULL);
#endif
    // Create file
    ncid = H5Fcreate (outfname, H5F_ACC_TRUNC, H5P_DEFAULT, faplid);
    CHECK_HID (ncid)
*/
    /* define dimensions, variables, and attributes */
    return 0;
    /* I/O amount so far */
    // err = HDF5_INQ_PUT_SIZE (ncid, &metadata_size); ERR

    stat (outfname, &file_stat);
    metadata_size = file_stat.st_size;

    err = HDF5_INQ_FILE_INFO (ncid, &info_used);
    ERR
    open_timing += MPI_Wtime () - timing;

    MPI_Barrier (io_comm); /*-----------------------------------------*/
    timing = MPI_Wtime ();

    i           = 0;
    dbl_buf_ptr = dbl_buf;
    MPI_Allreduce(xnreqs, max_cnt, 3, MPI_INT, MPI_MAX, io_comm);
    if (xnreqs[1] > 0) {
        /* lat */
        MPI_Offset **fix_starts_D2, **fix_counts_D2;

        /* construct varn API arguments starts[][] and counts[][] */
        int num = xnreqs[1];
        FIX_1D_VAR_STARTS_COUNTS (fix_starts_D2, fix_counts_D2, num, disps[1], blocklens[1])

        REC_2D_VAR_STARTS_COUNTS (0, starts_D2, counts_D2, xnreqs[1], disps[1], blocklens[1])
/*
        err = HDF5_IPUT_VARN (ncid, varids[i++], xnreqs[1], fix_starts_D2, fix_counts_D2,
                              dbl_buf_ptr, nelems[1], MPI_DOUBLE, NULL);
*/
        hdf5_put_varn_mpi (varids[i++],
                   MPI_DOUBLE,
                   dxplid_coll,
                   xnreqs[1],
                   max_cnt[1],
                   fix_starts_D2,
                   fix_counts_D2,
                   dbl_buf_ptr);
        ERR
        dbl_buf_ptr += nelems[1] + gap;
        my_nreqs += xnreqs[1];
        nvars_D[1]++;

        /* lon */
/*
        err = HDF5_IPUT_VARN (ncid, varids[i++], xnreqs[1], fix_starts_D2, fix_counts_D2,
                              dbl_buf_ptr, nelems[1], MPI_DOUBLE, NULL);
*/
        hdf5_put_varn_mpi (varids[i++],
                   MPI_DOUBLE,
                   dxplid_coll,
                   xnreqs[1],
                   max_cnt[1],
                   fix_starts_D2,
                   fix_counts_D2,
                   dbl_buf_ptr);
        ERR
        dbl_buf_ptr += nelems[1] + gap;
        my_nreqs += xnreqs[1];
        nvars_D[1]++;

        free (fix_starts_D2[0]);
        free (fix_starts_D2);
    } else {
        i += 2;
    }
    flush_multidatasets();
    /* area */
    if (xnreqs[0] > 0) {
        MPI_Offset **fix_starts_D1, **fix_counts_D1;

        /* construct varn API arguments starts[][] and counts[][] */
        FIX_1D_VAR_STARTS_COUNTS (fix_starts_D1, fix_counts_D1, xnreqs[0], disps[0], blocklens[0])
        nvars_D[0]++;
/*
        err = HDF5_IPUT_VARN (ncid, varids[i++], xnreqs[0], fix_starts_D1, fix_counts_D1,
                              dbl_buf_ptr, nelems[0], MPI_DOUBLE, NULL);
*/
        hdf5_put_varn_mpi (varids[i++],
                   MPI_DOUBLE,
                   dxplid_coll,
                   xnreqs[0],
                   max_cnt[0],
                   fix_starts_D1,
                   fix_counts_D1,
                   dbl_buf_ptr);
        ERR
        dbl_buf_ptr += nelems[0] + gap;
        my_nreqs += xnreqs[0];
        nvars_D[0]++;

        free (fix_starts_D1[0]);
        free (fix_starts_D1);
    } else {
        i++;
    }
    flush_multidatasets();
    /* construct varn API arguments starts[][] and counts[][] */
    if (xnreqs[2] > 0)
        REC_3D_VAR_STARTS_COUNTS (0, starts_D3, counts_D3, xnreqs[2], disps[2], blocklens[2],
                                  dims[2][1])

    post_timing += MPI_Wtime () - timing;

    for (rec_no = 0; rec_no < num_recs; rec_no++) {
        MPI_Barrier (io_comm); /*-----------------------------------------*/
        timing = MPI_Wtime ();

        i           = 3;
        dbl_buf_ptr = dbl_buf + nelems[1] * 2 + nelems[0] + gap * 3;
        int_buf_ptr = int_buf;
        txt_buf_ptr = txt_buf;
        /* next 27 small variables are written by rank 0 only */
        //if (rank == 0) {
            my_nreqs += 27;
            /* post nonblocking requests using HDF5_IPUT_VARN() */

            err = write_small_vars_F_case_hdf5 (ncid, rank, i, varids, rec_no, gap, dims[2][0],
                                                dims[2][0] + 1, 2, 8, &int_buf_ptr, &txt_buf_ptr,
                                                &dbl_buf_ptr);

            ERR
        //}
        small_write_timing = MPI_Wtime () - timing;
        flush_multidatasets();
        i += 27;
        post_timing += MPI_Wtime () - timing;

        MPI_Barrier (io_comm); /*-----------------------------------------*/
        timing = MPI_Wtime ();

        /* flush fixed-size and small variables */
        err = HDF5_WAIT_ALL (ncid, NC_REQ_ALL, NULL, NULL);
        ERR

        wait_timing += MPI_Wtime () - timing;

        MPI_Barrier (io_comm); /*-----------------------------------------*/
        timing = MPI_Wtime ();

        rec_buf_ptr = rec_buf;

        for (j = 0; j < xnreqs[1]; j++) starts_D2[j][0] = rec_no;
        for (j = 0; j < xnreqs[2]; j++) starts_D3[j][0] = rec_no;
        if (nvars == 414) {
            if (two_buf) {
                /* write 2D variables */
                POST_VARN (2, 1, 30)    /* AEROD_v */
                POST_VARN (2, 19, 33)   /* AODABS ... AODVIS */
                POST_VARN (2, 6, 54)    /* AQ_DMS ... AQ_SOAG */
                POST_VARN (2, 4, 64)    /* BURDEN1 ... BURDEN4 */
                POST_VARN (2, 2, 69)    /* CDNUMC and CLDHGH */
                POST_VARN (2, 3, 73)    /* CLDLOW ... CLDTOT */
                POST_VARN (2, 11, 80)   /* DF_DMS ... DSTSFMBL */
                POST_VARN (2, 2, 92)    /* DTENDTH and DTENDTQ */
                POST_VARN (2, 7, 96)    /* FLDS ... FLUTC */
                POST_VARN (2, 15, 107)  /* FSDS ... ICEFRAC */
                POST_VARN (2, 2, 125)   /* LANDFRAC and LHFLX */
                POST_VARN (2, 1, 131)   /* LINOZ_SFCSINK */
                POST_VARN (2, 3, 133)   /* LINOZ_SZA ... LWCF */
                POST_VARN (2, 2, 148)   /* O3_SRF and OCNFRAC */
                POST_VARN (2, 1, 151)   /* OMEGA500 */
                POST_VARN (2, 8, 153)   /* PBLH ... PSL */
                POST_VARN (2, 2, 162)   /* QFLX and QREFHT */
                POST_VARN (2, 1, 167)   /* RAM1 */
                POST_VARN (2, 37, 169)  /* SFDMS ... SNOWHLND */
                POST_VARN (2, 10, 208)  /* SO2_CLXF ... SWCF */
                POST_VARN (2, 19, 219)  /* TAUGWX ... TVQ */
                POST_VARN (2, 1, 239)   /* U10 */
                POST_VARN (2, 3, 246)   /* WD_H2O2 ... WD_SO2 */
                POST_VARN (2, 32, 252)  /* airFV ... dst_c3SFWET */
                POST_VARN (2, 129, 285) /* mlip ... soa_c3SFWET */
                /* write 3D variables */
                POST_VARN (3, 2, 31)   /* ANRAIN and ANSNOW */
                POST_VARN (3, 2, 52)   /* AQRAIN and AQSNOW */
                POST_VARN (3, 4, 60)   /* AREI ... AWNI */
                POST_VARN (3, 1, 68)   /* CCN3 */
                POST_VARN (3, 2, 71)   /* CLDICE and CLDLIQ */
                POST_VARN (3, 4, 76)   /* CLOUD ... DCQ */
                POST_VARN (3, 1, 91)   /* DTCOND */
                POST_VARN (3, 2, 94)   /* EXTINCT and FICE */
                POST_VARN (3, 4, 103)  /* FREQI ... FREQS */
                POST_VARN (3, 3, 122)  /* ICIMR ... IWC */
                POST_VARN (3, 4, 127)  /* LINOZ_DO3 ... LINOZ_O3COL */
                POST_VARN (3, 1, 132)  /* LINOZ_SSO3 */
                POST_VARN (3, 12, 136) /* Mass_bc ... O3 */
                POST_VARN (3, 1, 150)  /* OMEGA */
                POST_VARN (3, 1, 152)  /* OMEGAT */
                POST_VARN (3, 1, 161)  /* Q */
                POST_VARN (3, 3, 164)  /* QRL ... RAINQM */
                POST_VARN (3, 1, 168)  /* RELHUM */
                POST_VARN (3, 2, 206)  /* SNOWQM and SO2 */
                POST_VARN (3, 1, 218)  /* T */
                POST_VARN (3, 1, 238)  /* U */
                POST_VARN (3, 6, 240)  /* UU ... VV */
                POST_VARN (3, 3, 249)  /* WSUB ... aero_water */
                POST_VARN (3, 1, 284)  /* hstobie_linoz */
            } else {
                /* write variables in the same order as they defined */
                POST_VARN (2, 1, 30)    /* AEROD_v */
                POST_VARN (3, 2, 31)    /* ANRAIN and ANSNOW */
                POST_VARN (2, 19, 33)   /* AODABS ... AODVIS */
                POST_VARN (3, 2, 52)    /* AQRAIN and AQSNOW */
                POST_VARN (2, 6, 54)    /* AQ_DMS ... AQ_SOAG */
                POST_VARN (3, 4, 60)    /* AREI ... AWNI */
                POST_VARN (2, 4, 64)    /* BURDEN1 ... BURDEN4 */
                POST_VARN (3, 1, 68)    /* CCN3 */
                POST_VARN (2, 2, 69)    /* CDNUMC and CLDHGH */
                POST_VARN (3, 2, 71)    /* CLDICE and CLDLIQ */
                POST_VARN (2, 3, 73)    /* CLDLOW ... CLDTOT */
                POST_VARN (3, 4, 76)    /* CLOUD ... DCQ */
                POST_VARN (2, 11, 80)   /* DF_DMS ... DSTSFMBL */
                POST_VARN (3, 1, 91)    /* DTCOND */
                POST_VARN (2, 2, 92)    /* DTENDTH and DTENDTQ */
                POST_VARN (3, 2, 94)    /* EXTINCT and FICE */
                POST_VARN (2, 7, 96)    /* FLDS ... FLUTC */
                POST_VARN (3, 4, 103)   /* FREQI ... FREQS */
                POST_VARN (2, 15, 107)  /* FSDS ... ICEFRAC */
                POST_VARN (3, 3, 122)   /* ICIMR ... IWC */
                POST_VARN (2, 2, 125)   /* LANDFRAC and LHFLX */
                POST_VARN (3, 4, 127)   /* LINOZ_DO3 ... LINOZ_O3COL */
                POST_VARN (2, 1, 131)   /* LINOZ_SFCSINK */
                POST_VARN (3, 1, 132)   /* LINOZ_SSO3 */
                POST_VARN (2, 3, 133)   /* LINOZ_SZA ... LWCF */
                POST_VARN (3, 12, 136)  /* Mass_bc ... O3 */
                POST_VARN (2, 2, 148)   /* O3_SRF and OCNFRAC */
                POST_VARN (3, 1, 150)   /* OMEGA */
                POST_VARN (2, 1, 151)   /* OMEGA500 */
                POST_VARN (3, 1, 152)   /* OMEGAT */
                POST_VARN (2, 8, 153)   /* PBLH ... PSL */
                POST_VARN (3, 1, 161)   /* Q */
                POST_VARN (2, 2, 162)   /* QFLX and QREFHT */
                POST_VARN (3, 3, 164)   /* QRL ... RAINQM */
                POST_VARN (2, 1, 167)   /* RAM1 */
                POST_VARN (3, 1, 168)   /* RELHUM */
                POST_VARN (2, 37, 169)  /* SFDMS ... SNOWHLND */
                POST_VARN (3, 2, 206)   /* SNOWQM and SO2 */
                POST_VARN (2, 10, 208)  /* SO2_CLXF ... SWCF */
                POST_VARN (3, 1, 218)   /* T */
                POST_VARN (2, 19, 219)  /* TAUGWX ... TVQ */
                POST_VARN (3, 1, 238)   /* U */
                POST_VARN (2, 1, 239)   /* U10 */
                POST_VARN (3, 6, 240)   /* UU ... VV */
                POST_VARN (2, 3, 246)   /* WD_H2O2 ... WD_SO2 */
                POST_VARN (3, 3, 249)   /* WSUB ... aero_water */
                POST_VARN (2, 32, 252)  /* airFV ... dst_c3SFWET */
                POST_VARN (3, 1, 284)   /* hstobie_linoz */
                POST_VARN (2, 129, 285) /* mlip ... soa_c3SFWET */
            }
        } else {
            if (two_buf) {
                /* write 2D variables followed by 3D variables */
                POST_VARN (2, 13, 30) /* CLDHGH ... T5 */
                POST_VARN (2, 7, 44)  /* U250 ... Z500 */
                POST_VARN (3, 1, 43)  /* U */
            } else {
                /* write variables in the same order as they defined */
                POST_VARN (2, 13, 30) /* CLDHGH ... T5 */
                POST_VARN (3, 1, 43)  /* U */
                POST_VARN (2, 7, 44)  /* U250 ... Z500 */
            }
        }
        post_timing += MPI_Wtime () - timing;

        MPI_Barrier (io_comm); /*-----------------------------------------*/

        timing = MPI_Wtime ();

        flush_multidatasets();
        dataspace_recycle_all();
        memspace_recycle_all();

        err = HDF5_WAIT_ALL (ncid, NC_REQ_ALL, NULL, NULL);
        ERR

        wait_timing += MPI_Wtime () - timing;
    }

    MPI_Barrier (io_comm); /*-----------------------------------------*/
    timing = MPI_Wtime ();

    herr = hdf5_close_vars (ncid);
    HERR;
    herr = H5Fclose (ncid);
    HERR;
    herr = H5Pclose (faplid);
    HERR;
    hdf5_wrap_finalize ();
    close_timing += MPI_Wtime () - timing;

    // err = HDF5_INQ_PUT_SIZE (ncid, &total_size); ERR
    // put_size = total_size - metadata_size;
    stat (outfname, &file_stat);
    total_size = file_stat.st_size;
    put_size   = total_size;

    if (starts_D3 != NULL) {
        free (starts_D3[0]);
        free (starts_D3);
    }
    if (starts_D2 != NULL) {
        free (starts_D2[0]);
        free (starts_D2);
    }
    if (rec_buf != NULL) free (rec_buf);
    if (dbl_buf != NULL) free (dbl_buf);
    free (varids);

    total_timing = MPI_Wtime () - total_timing;

    tmp = my_nreqs;
    MPI_Reduce (&tmp, &max_nreqs, 1, MPI_OFFSET, MPI_MAX, 0, io_comm);
    MPI_Reduce (&tmp, &total_nreqs, 1, MPI_OFFSET, MPI_SUM, 0, io_comm);
    // MPI_Reduce(&put_size,      &tmp,        1, MPI_OFFSET, MPI_SUM, 0, io_comm);
    // put_size = tmp;
    // MPI_Reduce(&total_size,    &tmp,        1, MPI_OFFSET, MPI_SUM, 0, io_comm);
    // total_size = tmp;
    MPI_Reduce (&open_timing, &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    open_timing = max_timing;
    MPI_Reduce (&pre_timing, &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    pre_timing = max_timing;
    MPI_Reduce (&post_timing, &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    post_timing = max_timing;
    MPI_Reduce (&small_write_timing, &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    small_write_timing = max_timing;
    MPI_Reduce (&wait_timing, &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    wait_timing = max_timing;
    MPI_Reduce (&close_timing, &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    close_timing = max_timing;
    MPI_Reduce (&total_timing, &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, io_comm);
    total_timing = max_timing;

    /* check if there is any PnetCDF internal malloc residue */
    // MPI_Offset m_alloc=0, max_alloc;
    // HDF5_NOP1 (&m_alloc);
    // MPI_Reduce(&m_alloc, &max_alloc, 1, MPI_OFFSET, MPI_MAX, 0, io_comm);
    if (rank == 0) {
        int nvars_noD = nvars;
        for (i = 0; i < 3; i++) nvars_noD -= nvars_D[i];
        printf ("History output file                = %s\n", outfile);
        printf ("No. variables use no decomposition = %3d\n", nvars_noD);
        printf ("No. variables use decomposition D1 = %3d\n", nvars_D[0]);
        printf ("No. variables use decomposition D2 = %3d\n", nvars_D[1]);
        printf ("No. variables use decomposition D3 = %3d\n", nvars_D[2]);
        printf ("Total number of variables          = %3d\n", nvars);
        // printf ("MAX heap memory allocated by PnetCDF internally is %.2f MiB\n",
        //        (float)max_alloc / 1048576);
        printf ("Total write amount                 = %.2f MiB = %.2f GiB\n",
                (double)total_size / 1048576, (double)total_size / 1073741824);
        printf ("Total number of requests           = %lld\n", total_nreqs);
        printf ("Max number of requests             = %lld\n", max_nreqs);
        printf ("Max Time of open + metadata define = %.4f sec\n", open_timing);
        printf ("Max Time of I/O preparing          = %.4f sec\n", pre_timing);
        printf ("Max Time of HDF5_IPUT_VARN        = %.4f sec\n", post_timing);
        printf ("Max Time of HDF5 small writes        = %.4f sec\n", small_write_timing);
        printf ("Max Time of HDF5_WAIT_ALL         = %.4f sec\n", wait_timing);
        printf ("Max Time of close                  = %.4f sec\n", close_timing);
        printf ("Max Time of TOTAL                  = %.4f sec\n", total_timing);
        printf ("I/O bandwidth (open-to-close)      = %.4f MiB/sec\n",
                (double)total_size / 1048576.0 / total_timing);
        printf ("I/O bandwidth (write-only)         = %.4f MiB/sec\n",
                (double)put_size / 1048576.0 / wait_timing);
        if (verbose) print_info (&info_used);
        printf ("-----------------------------------------------------------\n");
    }

fn_exit:
    if (info_used != MPI_INFO_NULL) MPI_Info_free (&info_used);
    if (!keep_outfile) unlink (outfname);
    fflush (stdout);
    MPI_Barrier (io_comm);
    return nerrs;
}

#define POST_VARN_RD(k, num, vid)                                                                 \
    for (j = 0; j < num; j++) {                                                                   \
        err = HDF5_IGET_VARN (ncid, vid + j, nreqs[k - 1], starts_D##k, counts_D##k, rec_buf_ptr, \
                              -1, REC_ITYPE, NULL);                                               \
        ERR                                                                                       \
        rec_buf_ptr += nelems[k - 1] + gap;                                                       \
        my_nreqs += nreqs[k - 1];                                                                 \
    }

/*----< run_varn_F_case_rd() >--------------------------------------------------*/
int run_varn_F_case_rd_hdf5 (
    MPI_Comm io_comm,  /* MPI communicator that includes all the tasks involved in IO */
    char *out_dir,     /* output folder name */
    char *outfile,     /* output file name */
    int nvars,         /* number of variables 414 or 51 */
    int num_recs,      /* number of records */
    int noncontig_buf, /* whether to us noncontiguous buffer */
    MPI_Info info,
    MPI_Offset dims[3][2], /* dimension lengths */
    int nreqs[3],          /* no. request in decompositions 1,2,3 */
    int *disps[3],         /* request's displacements */
    int *blocklens[3],     /* request's block lengths */
    double **dbl_bufp,     /* buffer for fixed size double var */
    itype **rec_bufp,      /* buffer for rec floating point var */
    char *txt_buf,         /* buffer for char var */
    int *int_buf)          /* buffer for int var */
{
    char outfname[512], *txt_buf_ptr;
    herr_t herr;
    hid_t ncid, faplid;
    int i, j, k, err, nerrs = 0, rank, cmode, *varids, nreqs_D3_merged;
    int rec_no, gap = 0, my_nreqs, *int_buf_ptr;
    size_t dbl_buflen, rec_buflen, nelems[3];
    itype *rec_buf, *rec_buf_ptr;
    double *dbl_buf, *dbl_buf_ptr;
    double pre_timing, open_timing, post_timing, wait_timing, close_timing;
    double timing, total_timing, max_timing;
    MPI_Offset tmp, metadata_size, put_size, total_size, max_nreqs, total_nreqs;
    MPI_Offset **starts_D2 = NULL, **counts_D2 = NULL;
    MPI_Offset **starts_D3 = NULL, **counts_D3 = NULL;
    MPI_Comm comm      = io_comm;
    MPI_Info info_used = MPI_INFO_NULL;

    MPI_Barrier (comm); /*-----------------------------------------*/
    total_timing = pre_timing = MPI_Wtime ();

    open_timing  = 0.0;
    post_timing  = 0.0;
    wait_timing  = 0.0;
    close_timing = 0.0;

    MPI_Comm_rank (comm, &rank);

    if (noncontig_buf) gap = 10;

    /* calculate number of variable elements from 3 decompositions */
    my_nreqs = 0;
    for (i = 0; i < 3; i++) {
        for (nelems[i] = 0, k = 0; k < nreqs[i]; k++) nelems[i] += blocklens[i][k];
    }
    if (verbose && rank == 0) printf ("nelems=%zd %zd %zd\n", nelems[0], nelems[1], nelems[2]);

    /* allocate and initialize write buffer for small variables */
    dbl_buflen =
        nelems[1] * 2 + nelems[0] + 3 * dims[2][0] + 3 * (dims[2][0] + 1) + 8 + 2 + 20 * gap;

    dbl_buf = (double *)malloc (dbl_buflen * sizeof (double));
    if (dbl_bufp != NULL) { *dbl_bufp = dbl_buf; }

    for (i = 0; i < dbl_buflen; i++) dbl_buf[i] = rank;

    /* allocate and initialize write buffer for large variables */
    if (nvars == 414)
        rec_buflen = nelems[1] * 321 + nelems[2] * 63 + (321 + 63) * gap;
    else
        rec_buflen = nelems[1] * 20 + nelems[2] + (20 + 1) * gap;

    rec_buf = (itype *)malloc (rec_buflen * sizeof (itype));
    if (rec_bufp != NULL) { *rec_bufp = rec_buf; }

    for (i = 0; i < rec_buflen; i++) rec_buf[i] = rank;

    for (i = 0; i < 10; i++) int_buf[i] = rank;

    for (i = 0; i < 16; i++) txt_buf[i] = 'a' + rank;

    varids = (int *)malloc (nvars * sizeof (int));

    pre_timing = MPI_Wtime () - pre_timing;

    MPI_Barrier (comm); /*-----------------------------------------*/
    timing = MPI_Wtime ();

    /* set output file name */
    sprintf (outfname, "%s/%s", out_dir, outfile);

    /* create a new HDF5 file for writing */
    err = hdf5_wrap_init ();
    ERR;

    // Register LOG VOL plugin
    faplid = H5Pcreate (H5P_FILE_ACCESS);
    // MPI and collective metadata is required by LOG VOL
    H5Pset_fapl_mpio (faplid, comm, info);
    H5Pset_all_coll_metadata_ops (faplid, 1);
    ncid = H5Fopen (outfname, H5F_ACC_RDONLY, faplid);
    CHECK_HID (ncid)

    /* define dimensions, variables, and attributes */
    if (nvars == 414) {
        /* for h0 file */
        err = inq_F_case_h0_hdf5 (ncid, dims[2], nvars, varids);
        ERR
    } else {
        /* for h1 file */
        err = inq_F_case_h1_hdf5 (ncid, dims[2], nvars, varids);
        ERR
    }

    /* exit define mode and enter data mode */
    // err = ncmpi_enddef(ncid); ERR

    /* I/O amount so far */
    err = HDF5_NOP2 (ncid, &metadata_size);
    ERR
    err = HDF5_INQ_FILE_INFO (ncid, &info_used);
    ERR
    open_timing += MPI_Wtime () - timing;

    MPI_Barrier (comm); /*-----------------------------------------*/
    timing = MPI_Wtime ();

    i           = 0;
    dbl_buf_ptr = dbl_buf;

    if (nreqs[1] > 0) {
        /* lat */
        MPI_Offset **fix_starts_D2, **fix_counts_D2;

        /* construct varn API arguments starts[][] and counts[][] */
        int num = nreqs[1];
        FIX_1D_VAR_STARTS_COUNTS (fix_starts_D2, fix_counts_D2, num, disps[1], blocklens[1])

        REC_2D_VAR_STARTS_COUNTS (0, starts_D2, counts_D2, nreqs[1], disps[1], blocklens[1])

        err = HDF5_IGET_VARN (ncid, varids[i++], nreqs[1], fix_starts_D2, fix_counts_D2,
                              dbl_buf_ptr, nelems[1], MPI_DOUBLE, NULL);
        ERR
        dbl_buf_ptr += nelems[1] + gap;
        my_nreqs += nreqs[1];

        /* lon */
        err = HDF5_IGET_VARN (ncid, varids[i++], nreqs[1], fix_starts_D2, fix_counts_D2,
                              dbl_buf_ptr, nelems[1], MPI_DOUBLE, NULL);
        ERR
        dbl_buf_ptr += nelems[1] + gap;
        my_nreqs += nreqs[1];

        free (fix_starts_D2[0]);
        free (fix_starts_D2);
    } else
        i += 2;

    /* area */
    if (nreqs[0] > 0) {
        MPI_Offset **fix_starts_D1, **fix_counts_D1;

        /* construct varn API arguments starts[][] and counts[][] */
        FIX_1D_VAR_STARTS_COUNTS (fix_starts_D1, fix_counts_D1, nreqs[0], disps[0], blocklens[0])

        err = HDF5_IGET_VARN (ncid, varids[i++], nreqs[0], fix_starts_D1, fix_counts_D1,
                              dbl_buf_ptr, nelems[0], MPI_DOUBLE, NULL);
        ERR
        dbl_buf_ptr += nelems[0] + gap;
        my_nreqs += nreqs[0];

        free (fix_starts_D1[0]);
        free (fix_starts_D1);
    } else
        i++;

    /* construct varn API arguments starts[][] and counts[][] */
    if (nreqs[2] > 0)
        REC_3D_VAR_STARTS_COUNTS (0, starts_D3, counts_D3, nreqs[2], disps[2], blocklens[2],
                                  dims[2][1])

    post_timing += MPI_Wtime () - timing;

    for (rec_no = 0; rec_no < num_recs; rec_no++) {
        MPI_Barrier (comm); /*-----------------------------------------*/
        timing = MPI_Wtime ();

        i           = 3;
        dbl_buf_ptr = dbl_buf + nelems[1] * 2 + nelems[0] + gap * 3;
        int_buf_ptr = int_buf;
        txt_buf_ptr = txt_buf;

        /* next 27 small variables are written by rank 0 only */
        if (rank == 0) {
            my_nreqs += 27;
            /* post nonblocking requests using HDF5_IPUT_VARN() */
            err = read_small_vars_F_case_hdf5 (ncid, i, varids, rec_no, gap, dims[2][0],
                                               dims[2][0] + 1, 2, 8, &int_buf_ptr, &txt_buf_ptr,
                                               &dbl_buf_ptr);
            ERR
        }
        i += 27;

        post_timing += MPI_Wtime () - timing;

        MPI_Barrier (comm); /*-----------------------------------------*/
        timing = MPI_Wtime ();

        /* flush fixed-size and small variables */
        err = HDF5_WAIT_ALL (ncid, NC_REQ_ALL, NULL, NULL);
        ERR

        wait_timing += MPI_Wtime () - timing;

        MPI_Barrier (comm); /*-----------------------------------------*/
        timing = MPI_Wtime ();

        rec_buf_ptr = rec_buf;

        for (j = 0; j < nreqs[1]; j++) starts_D2[j][0] = rec_no;
        for (j = 0; j < nreqs[2]; j++) starts_D3[j][0] = rec_no;

        if (nvars == 414) {
            if (two_buf) {
                /* read 2D variables */
                POST_VARN_RD (2, 1, 30)    /* AEROD_v */
                POST_VARN_RD (2, 19, 33)   /* AODABS ... AODVIS */
                POST_VARN_RD (2, 6, 54)    /* AQ_DMS ... AQ_SOAG */
                POST_VARN_RD (2, 4, 64)    /* BURDEN1 ... BURDEN4 */
                POST_VARN_RD (2, 2, 69)    /* CDNUMC and CLDHGH */
                POST_VARN_RD (2, 3, 73)    /* CLDLOW ... CLDTOT */
                POST_VARN_RD (2, 11, 80)   /* DF_DMS ... DSTSFMBL */
                POST_VARN_RD (2, 2, 92)    /* DTENDTH and DTENDTQ */
                POST_VARN_RD (2, 7, 96)    /* FLDS ... FLUTC */
                POST_VARN_RD (2, 15, 107)  /* FSDS ... ICEFRAC */
                POST_VARN_RD (2, 2, 125)   /* LANDFRAC and LHFLX */
                POST_VARN_RD (2, 1, 131)   /* LINOZ_SFCSINK */
                POST_VARN_RD (2, 3, 133)   /* LINOZ_SZA ... LWCF */
                POST_VARN_RD (2, 2, 148)   /* O3_SRF and OCNFRAC */
                POST_VARN_RD (2, 1, 151)   /* OMEGA500 */
                POST_VARN_RD (2, 8, 153)   /* PBLH ... PSL */
                POST_VARN_RD (2, 2, 162)   /* QFLX and QREFHT */
                POST_VARN_RD (2, 1, 167)   /* RAM1 */
                POST_VARN_RD (2, 37, 169)  /* SFDMS ... SNOWHLND */
                POST_VARN_RD (2, 10, 208)  /* SO2_CLXF ... SWCF */
                POST_VARN_RD (2, 19, 219)  /* TAUGWX ... TVQ */
                POST_VARN_RD (2, 1, 239)   /* U10 */
                POST_VARN_RD (2, 3, 246)   /* WD_H2O2 ... WD_SO2 */
                POST_VARN_RD (2, 32, 252)  /* airFV ... dst_c3SFWET */
                POST_VARN_RD (2, 129, 285) /* mlip ... soa_c3SFWET */
                /* write 3D variables */
                POST_VARN_RD (3, 2, 31)   /* ANRAIN and ANSNOW */
                POST_VARN_RD (3, 2, 52)   /* AQRAIN and AQSNOW */
                POST_VARN_RD (3, 4, 60)   /* AREI ... AWNI */
                POST_VARN_RD (3, 1, 68)   /* CCN3 */
                POST_VARN_RD (3, 2, 71)   /* CLDICE and CLDLIQ */
                POST_VARN_RD (3, 4, 76)   /* CLOUD ... DCQ */
                POST_VARN_RD (3, 1, 91)   /* DTCOND */
                POST_VARN_RD (3, 2, 94)   /* EXTINCT and FICE */
                POST_VARN_RD (3, 4, 103)  /* FREQI ... FREQS */
                POST_VARN_RD (3, 3, 122)  /* ICIMR ... IWC */
                POST_VARN_RD (3, 4, 127)  /* LINOZ_DO3 ... LINOZ_O3COL */
                POST_VARN_RD (3, 1, 132)  /* LINOZ_SSO3 */
                POST_VARN_RD (3, 12, 136) /* Mass_bc ... O3 */
                POST_VARN_RD (3, 1, 150)  /* OMEGA */
                POST_VARN_RD (3, 1, 152)  /* OMEGAT */
                POST_VARN_RD (3, 1, 161)  /* Q */
                POST_VARN_RD (3, 3, 164)  /* QRL ... RAINQM */
                POST_VARN_RD (3, 1, 168)  /* RELHUM */
                POST_VARN_RD (3, 2, 206)  /* SNOWQM and SO2 */
                POST_VARN_RD (3, 1, 218)  /* T */
                POST_VARN_RD (3, 1, 238)  /* U */
                POST_VARN_RD (3, 6, 240)  /* UU ... VV */
                POST_VARN_RD (3, 3, 249)  /* WSUB ... aero_water */
                POST_VARN_RD (3, 1, 284)  /* hstobie_linoz */
            } else {
                /* read variables in the same order as they defined */
                POST_VARN_RD (2, 1, 30)    /* AEROD_v */
                POST_VARN_RD (3, 2, 31)    /* ANRAIN and ANSNOW */
                POST_VARN_RD (2, 19, 33)   /* AODABS ... AODVIS */
                POST_VARN_RD (3, 2, 52)    /* AQRAIN and AQSNOW */
                POST_VARN_RD (2, 6, 54)    /* AQ_DMS ... AQ_SOAG */
                POST_VARN_RD (3, 4, 60)    /* AREI ... AWNI */
                POST_VARN_RD (2, 4, 64)    /* BURDEN1 ... BURDEN4 */
                POST_VARN_RD (3, 1, 68)    /* CCN3 */
                POST_VARN_RD (2, 2, 69)    /* CDNUMC and CLDHGH */
                POST_VARN_RD (3, 2, 71)    /* CLDICE and CLDLIQ */
                POST_VARN_RD (2, 3, 73)    /* CLDLOW ... CLDTOT */
                POST_VARN_RD (3, 4, 76)    /* CLOUD ... DCQ */
                POST_VARN_RD (2, 11, 80)   /* DF_DMS ... DSTSFMBL */
                POST_VARN_RD (3, 1, 91)    /* DTCOND */
                POST_VARN_RD (2, 2, 92)    /* DTENDTH and DTENDTQ */
                POST_VARN_RD (3, 2, 94)    /* EXTINCT and FICE */
                POST_VARN_RD (2, 7, 96)    /* FLDS ... FLUTC */
                POST_VARN_RD (3, 4, 103)   /* FREQI ... FREQS */
                POST_VARN_RD (2, 15, 107)  /* FSDS ... ICEFRAC */
                POST_VARN_RD (3, 3, 122)   /* ICIMR ... IWC */
                POST_VARN_RD (2, 2, 125)   /* LANDFRAC and LHFLX */
                POST_VARN_RD (3, 4, 127)   /* LINOZ_DO3 ... LINOZ_O3COL */
                POST_VARN_RD (2, 1, 131)   /* LINOZ_SFCSINK */
                POST_VARN_RD (3, 1, 132)   /* LINOZ_SSO3 */
                POST_VARN_RD (2, 3, 133)   /* LINOZ_SZA ... LWCF */
                POST_VARN_RD (3, 12, 136)  /* Mass_bc ... O3 */
                POST_VARN_RD (2, 2, 148)   /* O3_SRF and OCNFRAC */
                POST_VARN_RD (3, 1, 150)   /* OMEGA */
                POST_VARN_RD (2, 1, 151)   /* OMEGA500 */
                POST_VARN_RD (3, 1, 152)   /* OMEGAT */
                POST_VARN_RD (2, 8, 153)   /* PBLH ... PSL */
                POST_VARN_RD (3, 1, 161)   /* Q */
                POST_VARN_RD (2, 2, 162)   /* QFLX and QREFHT */
                POST_VARN_RD (3, 3, 164)   /* QRL ... RAINQM */
                POST_VARN_RD (2, 1, 167)   /* RAM1 */
                POST_VARN_RD (3, 1, 168)   /* RELHUM */
                POST_VARN_RD (2, 37, 169)  /* SFDMS ... SNOWHLND */
                POST_VARN_RD (3, 2, 206)   /* SNOWQM and SO2 */
                POST_VARN_RD (2, 10, 208)  /* SO2_CLXF ... SWCF */
                POST_VARN_RD (3, 1, 218)   /* T */
                POST_VARN_RD (2, 19, 219)  /* TAUGWX ... TVQ */
                POST_VARN_RD (3, 1, 238)   /* U */
                POST_VARN_RD (2, 1, 239)   /* U10 */
                POST_VARN_RD (3, 6, 240)   /* UU ... VV */
                POST_VARN_RD (2, 3, 246)   /* WD_H2O2 ... WD_SO2 */
                POST_VARN_RD (3, 3, 249)   /* WSUB ... aero_water */
                POST_VARN_RD (2, 32, 252)  /* airFV ... dst_c3SFWET */
                POST_VARN_RD (3, 1, 284)   /* hstobie_linoz */
                POST_VARN_RD (2, 129, 285) /* mlip ... soa_c3SFWET */
            }
        } else {
            if (two_buf) {
                /* read 2D variables followed by 3D variables */
                POST_VARN_RD (2, 13, 30) /* CLDHGH ... T5 */
                POST_VARN_RD (2, 7, 44)  /* U250 ... Z500 */
                POST_VARN_RD (3, 1, 43)  /* U */
            } else {
                /* read variables in the same order as they defined */
                POST_VARN_RD (2, 13, 30) /* CLDHGH ... T5 */
                POST_VARN_RD (3, 1, 43)  /* U */
                POST_VARN_RD (2, 7, 44)  /* U250 ... Z500 */
            }
        }

        post_timing += MPI_Wtime () - timing;

        MPI_Barrier (comm); /*-----------------------------------------*/
        timing = MPI_Wtime ();

        err = HDF5_WAIT_ALL (ncid, NC_REQ_ALL, NULL, NULL);
        ERR

        wait_timing += MPI_Wtime () - timing;
    }

    MPI_Barrier (comm); /*-----------------------------------------*/
    timing = MPI_Wtime ();

    err = HDF5_NOP2 (ncid, &total_size);
    ERR;
    put_size = total_size - metadata_size;

    herr = hdf5_close_vars (ncid);
    HERR;
    herr = H5Fclose (ncid);
    HERR;
    herr = H5Pclose (faplid);
    HERR;
    hdf5_wrap_finalize ();
    close_timing += MPI_Wtime () - timing;

    if (starts_D3 != NULL) {
        free (starts_D3[0]);
        free (starts_D3);
    }
    if (starts_D2 != NULL) {
        free (starts_D2[0]);
        free (starts_D2);
    }
    if (rec_bufp == NULL) { free (rec_buf); }
    if (dbl_bufp == NULL) { free (dbl_buf); }
    free (varids);

    total_timing = MPI_Wtime () - total_timing;

    tmp = my_nreqs;
    MPI_Reduce (&tmp, &max_nreqs, 1, MPI_OFFSET, MPI_MAX, 0, comm);
    MPI_Reduce (&tmp, &total_nreqs, 1, MPI_OFFSET, MPI_SUM, 0, comm);
    //MPI_Reduce (&put_size, &tmp, 1, MPI_OFFSET, MPI_SUM, 0, comm);
    //put_size = tmp;
    //MPI_Reduce (&total_size, &tmp, 1, MPI_OFFSET, MPI_SUM, 0, comm);
    //total_size = tmp;
    MPI_Reduce (&open_timing, &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, comm);
    open_timing = max_timing;
    MPI_Reduce (&pre_timing, &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, comm);
    pre_timing = max_timing;
    MPI_Reduce (&post_timing, &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, comm);
    post_timing = max_timing;
    MPI_Reduce (&wait_timing, &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, comm);
    wait_timing = max_timing;
    MPI_Reduce (&close_timing, &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, comm);
    close_timing = max_timing;
    MPI_Reduce (&total_timing, &max_timing, 1, MPI_DOUBLE, MPI_MAX, 0, comm);
    total_timing = max_timing;

    /* check if there is any PnetCDF internal malloc residue */
    MPI_Offset malloc_size, sum_size;
    err = ncmpi_inq_malloc_size (&malloc_size);
    if (err == NC_NOERR) {
        MPI_Reduce (&malloc_size, &sum_size, 1, MPI_OFFSET, MPI_SUM, 0, comm);
        if (rank == 0 && sum_size > 0) {
            printf ("-----------------------------------------------------------\n");
            printf ("heap memory allocated by PnetCDF internally has %lld bytes yet to be freed\n",
                    sum_size);
        }
    }
    MPI_Offset m_alloc = 0, max_alloc = 0;
    ncmpi_inq_malloc_max_size (&m_alloc);
    MPI_Reduce (&m_alloc, &max_alloc, 1, MPI_OFFSET, MPI_MAX, 0, comm);
    if (rank == 0) {
        printf ("History output file                = %s\n", outfile);
        // printf ("MAX heap memory allocated by PnetCDF internally is %.2f MiB\n",
        //        (float)max_alloc / 1048576);
        printf ("Total number of variables          = %d\n", nvars);
        printf ("Total read amount                 = %.2f MiB = %.2f GiB\n",
                (double)total_size / 1048576, (double)total_size / 1073741824);
        printf ("Total number of requests           = %lld\n", total_nreqs);
        printf ("Max number of requests             = %lld\n", max_nreqs);
        printf ("Max Time of open + metadata inquery = %.4f sec\n", open_timing);
        printf ("Max Time of I/O preparing          = %.4f sec\n", pre_timing);
        printf ("Max Time of HDF5_IGET_VARN        = %.4f sec\n", post_timing);
        printf ("Max Time of HDF5_WAIT_ALL         = %.4f sec\n", wait_timing);
        printf ("Max Time of close                  = %.4f sec\n", close_timing);
        printf ("Max Time of TOTAL                  = %.4f sec\n", total_timing);
        printf ("I/O bandwidth (open-to-close)      = %.4f MiB/sec\n",
                (double)total_size / 1048576.0 / total_timing);
        printf ("I/O bandwidth (read-only)         = %.4f MiB/sec\n",
                (double)put_size / 1048576.0 / wait_timing);
        if (verbose) print_info (&info_used);
        printf ("-----------------------------------------------------------\n");
    }
fn_exit:
    if (info_used != MPI_INFO_NULL) MPI_Info_free (&info_used);
    if (!keep_outfile) unlink (outfname);
    fflush (stdout);
    MPI_Barrier (comm);
    return nerrs;
}
