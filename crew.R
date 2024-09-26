################################################################################
##### make pipeline
.libPaths()
targets::tar_destroy(ask = FALSE)
targets::tar_make()


################################################################################
##### checks
# targets::tar_read(library)
# targets::tar_read(libPaths)
# targets::tar_meta(fields = warnings, complete_only = TRUE)

# source("../hpc/bash_functions.R")
# queue()
# geo()
# job(308240)