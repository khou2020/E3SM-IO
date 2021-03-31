AM_CPPFLAGS = -I$(top_srcdir)

bin_PROGRAMS = e3sm_io

e3sm_io_SOURCES = e3sm_io.c e3sm_io_pnc.c read_decomp.c var_io_F_case.c var_io_G_case.c header_io_F_case.c header_io_G_case.c
e3sm_io_SOURCES += e3sm_io.h e3sm_io_pnc.h 
e3sm_io_LDADD = -lpnetcdf

if ENABLE_HDF5
e3sm_io_SOURCES += e3sm_io_hdf5.c var_io_F_case_hdf5.c header_io_F_case_hdf5.c var_io_G_case_hdf5.c header_io_G_case_hdf5.c
e3sm_io_SOURCES += e3sm_io_hdf5.h 
e3sm_io_LDADD += -lhdf5
endif

if ENABLE_LOGVOL
e3sm_io_SOURCES += dummy.cpp
e3sm_io_LDADD += -lH5VL_log
endif