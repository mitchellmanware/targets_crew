################################################################################
##### load libraries
library(targets)
library(tarchetypes)
library(crew)
library(crew.cluster)
library(beethoven)
library(dplyr)

Sys.setenv(
  "LD_LIBRARY_PATH" = paste(
    "/ddn/gs1/tools/set/R432/lib64/R/lib",
    Sys.getenv("LD_LIBRARY_PATH"),
    sep = ":"
  )
)

################################################################################
##### crew
# geo_script <- paste0("
# export PATH=/ddn/gs1/tools/set/R432/bin/R:/ddn/gs1/tools/cuda11.8/bin:$PATH
# export LD_LIBRARY_PATH=/ddn/gs1/tools/set/R432/lib64/R/lib:/ddn/gs1/tools/cuda11.8/lib64:$LD_LIBRARY_PATH
# export R_LIBS_USER=/ddn/gs1/tools/set/R432/lib64/R/library:$R_LIBS_USER
# ")
geo_script <- paste0("
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=manwareme@nih.gov

export PATH=/ddn/gs1/tools/set/R432/bin/R:/ddn/gs1/tools/cuda11.8/bin:$PATH
export LD_LIBRARY_PATH=/ddn/gs1/tools/set/R432/lib64/R/lib:/ddn/gs1/tools/cuda11.8/lib64:$LD_LIBRARY_PATH
export R_LIBS_USER=/ddn/gs1/tools/set/R432/lib64/R/library:$R_LIBS_USER

module load /ddn/gs1/tools/set/R432/bin/R
"
)

geo_controller <- crew_controller_slurm(
  name = "geo_controller",
  workers = 16,
  seconds_idle = 30,
  slurm_partition = "geo",
  slurm_memory_gigabytes_per_cpu = 4,
  slurm_cpus_per_task = 2,
  script_lines = geo_script
)

##### targets setup for crew
tar_option_set(
  controller = crew_controller_group(geo_controller),
  resources = tar_resources(
    crew = tar_resources_crew(controller = "geo_controller")
  )
)
tar_option_set(
  packages = c(
    "beethoven", "targets", "tarchetypes", "dplyr",
    "data.table", "sf", "crew", "crew.cluster"
  ),
  library = c("/ddn/gs1/tools/set/R432/lib64/R/library"),
  repository = "local",
  error = "abridge",
  memory = "transient",
  format = "qs",
  storage = "worker",
  deployment = "worker",
  garbage_collection = TRUE,
  seed = 202401L
)

##### test pipeline
list(
  tar_target(
    library,
    command = .Library
  ),
  tar_target(
    libPaths,
    command = .libPaths()
  ),
  tar_target(
    locs,
    command = sf::st_centroid(
      sf::st_as_sf(
        sf::st_read(
          system.file("shape/nc.shp", package = "sf")
        ),
        coords = c("lon", "lat"),
        crs = 4326
      )[, "NAME"]
    ) |> dplyr::rename(site_id = NAME)
  ),
  tar_target(
    dates_s,
    command = beethoven::split_dates(
      dates = c("2020-01-01", "2020-12-31"),
      n = 31
    )
  ),
  tar_target(
    dates_n,
    command = names(dates_s)
  ),
  tar_target(
    geos,
    command = beethoven::inject_geos(
      locs = locs,
      injection = list(
        path = paste0(
          "/ddn/gs1/group/set/Projects/NRT-AP-Model/input/geos/",
          "aqc_tavg_1hr_g1440x721_v1"
        ),
        date = fl_dates(dates_s[[dates_n]]),
        nthreads = 12
      )
    ),
    pattern = map(dates_n),
    iteration = "list",
    resources = tar_resources(
      crew = tar_resources_crew(
        controller = "geo_controller"
      )
    )
  )
)
