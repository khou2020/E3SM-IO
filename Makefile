
MPICC		= cc
CFLAGS		= -O2

PnetCDF_DIR	= /global/homes/q/qkt561/mpich_develop/PnetCDF
H5_DIR          = /global/homes/q/qkt561/mpich_develop/hdf5-hdf5-1_12_0/install

INCLUDES        = -I$(PnetCDF_DIR)/include -I. -I$(H5_DIR)/include
LDFLAGS         = -L$(PnetCDF_DIR)/lib -L$(H5_DIR)/lib
LIBS            = -lpnetcdf -lhdf5 $(shell $(PnetCDF_DIR)/bin/pnetcdf-config --libs)

.c.o:
	$(MPICC) $(CFLAGS) $(INCLUDES) -c $<

all: e3sm_io

dat2nc: dat2nc.o
	$(MPICC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

e3sm_io.o: e3sm_io.c e3sm_io.h
read_decomp.o: read_decomp.c e3sm_io.h

OBJS = read_decomp.o header_io_F_case.o e3sm_io_hdf5.o var_io_F_case.o header_io_G_case.o var_io_G_case.o header_io_F_case_hdf5.o var_io_F_case_hdf5.o header_io_G_case_hdf5.o var_io_G_case_hdf5.o

e3sm_io: e3sm_io.o $(OBJS)
	$(MPICC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

# romio_patch.c contains fix in https://github.com/pmodels/mpich/pull/3089
# ROMIO_PATCH	= -Wl,--wrap=ADIOI_Type_create_hindexed_x -l:libmpich_intel.a
# ROMIO_PATCH	= -Wl,--wrap=ADIOI_Type_create_hindexed_x -l:libmpi.a
ROMIO_PATCH	= -Wl,--wrap=ADIOI_Type_create_hindexed_x

e3sm_io.romio_patch: e3sm_io.o $(OBJS) romio_patch.o
	$(MPICC) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS) $(ROMIO_PATCH)

clean:
	rm -f core.* *.o dat2nc e3sm_io e3sm_io.romio_patch
	rm -f f_case_h0_varn.nc f_case_h1_varn.nc
	rm -f f_case_h0_vard.nc f_case_h1_vard.nc
	rm -f g_case_hist_varn.nc

.PHONY: clean

