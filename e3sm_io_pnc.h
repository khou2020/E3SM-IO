#pragma once

#include <pnetcdf.h>
#include "config.h"
#include "e3sm_io.h"

int e3sm_pnc_def_var (int ncid, const char *name, nc_type type, int ndim, int *dimids, int *varid);

size_t nc_type_size(nc_type type);