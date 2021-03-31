
#include <pnetcdf.h>

#include "config.h"
#include "e3sm_io.h"

size_t nc_type_size (nc_type type) {
    switch (type) {
        case NC_BYTE:
            return 1;
        case NC_CHAR:
            return 1;
        case NC_SHORT:
            return 2;
        case NC_INT:
            return 4;
        case NC_FLOAT:
            return 4;
        case NC_DOUBLE:
            return 8;
        case NC_UBYTE:
            return 1;
        case NC_USHORT:
            return 2;
        case NC_UINT:
            return 4;
        case NC_INT64:
            return 8;
        case NC_UINT64:
            return 8;
        default:
            return -1;
    }
}

int e3sm_pnc_def_var (int ncid, const char *name, nc_type type, int ndim, int *dimids, int *varid) {
    int err, nerrs = 0;
    int i;
    int mdim = 0;
    MPI_Offset dsize[16];
    MPI_Offset esize;
    int cdim[16];

    err = ncmpi_def_var (ncid, name, type, ndim, dimids, varid);
    ERR

    if (ndim > 0) {
        if (layout != LAYOUT_CONTIG) {
            esize = nc_type_size (type);
            for (i = 0; i < ndim; i++) {
                err = ncmpi_inq_dimlen (ncid, dimids[i], dsize + i);
                ERR
                if (dsize[mdim] < dsize[i]) { mdim = i; }
            }
            for (i = 0; i < ndim; i++) {
                if (dsize[i] == 0) { continue; }
                if (i == mdim) { continue; }
                cdim[i] = (int)dsize[i];
                esize *= cdim[i];
            }
            cdim[mdim] = chunk_size / esize + 1;
            if (cdim[mdim] > dsize[mdim]) { cdim[mdim] = dsize[mdim]; }
            if (dsize[0] == NC_UNLIMITED) { cdim[0] = 1; }
            err = ncmpi_put_att_int (ncid, *varid, "_chunkdim", NC_INT, ndim, cdim);
            ERR
        }
    }

fn_exit:;
    return nerrs == 0 ? 0 : -1;
}