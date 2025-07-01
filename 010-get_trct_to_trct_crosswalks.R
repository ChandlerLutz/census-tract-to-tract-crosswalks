## ./010-get_trct_to_trct_crosswalks.R

##Clear the workspace
rm(list = ls()) 
suppressWarnings(CLmisc::detach_all_packages())

##Set wd using the here package
setwd(here::here("./"))

utils <- CLmisc::load_module(here::here("core/utils.R"))

CLmisc::mkdir_p(here::here("crosswalks/"))

cw_tract2000_to_tract2010 <- utils$f_get_tract_to_tract_cw(from_yr = 2000, to_yr = 2020)

nanoparquet::write_parquet(
  cw_tract2000_to_tract2010,
  here::here("crosswalks", "cw_tract2000_to_tract2020.parquet")
)
