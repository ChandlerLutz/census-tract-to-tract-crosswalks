## core/utils.R

ipums_key <- keyring::key_get("ipums_api_key")
ipumsr::set_ipums_api_key(ipums_key, save = FALSE)

f_sum <- function(x) {
  if (sum(is.na(x)) == length(x)) return(NA_real_)
  sum(x, na.rm = TRUE)
}

f_get_tract_to_nhgis_tract_cw <- function() {

  box::use(
    data.table[...], magrittr[`%>%`], CLmisc[mkdir_p, select_by_ref],
    stringi[stri_length, stri_sub]
  )

  save_dir <- here::here("work/f_get_tract_to_nhgis_tract_cw")
  CLmisc::mkdir_p(save_dir)
  save_file_loc <- file.path(save_dir, "tract_to_nhgis_tract_cw.parquet")

  if (file.exists(save_file_loc))
    return(nanoparquet::read_parquet(save_file_loc) %>% setDT())

  dt_trct_gjoin_trct_cw <- CLmisc::get_rnhgis_tst(
    name = "AV0", geog_levels = "tract"
  ) %>%
    setDT() %>%
    .[, names(.SD) := NULL, .SDcols = patterns("^NAME|^AV0")]

  colorder <- names(dt_trct_gjoin_trct_cw)

  dt_trct_shp_1980 <- CLmisc::get_rnhgis_shp("us_tract_1980_tl2008") %>%
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "TRACT")) %>%
    setnames("GISJOIN", "GJOIN1980") %>%
    setnames("TRACT", "TRACT1980") %>%
    .[, TRACT1980 := sprintf("%06d", TRACT1980)]

  dt_trct_shp_1990 <- CLmisc::get_rnhgis_shp("us_tract_1990_tl2008") %>%
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "TRACT")) %>%
    setnames("GISJOIN", "GJOIN1990") %>%
    setnames("TRACT", "TRACT1990") %>%
    .[, TRACT1990 := sprintf("%06d", TRACT1990)]

  stopifnot(dt_trct_shp_1990[stri_length(TRACT1990) != 6] %>% nrow() == 0)

  dt_trct_shp_2000 <- CLmisc::get_rnhgis_shp("us_tract_2000_tl2010") %>%
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "TRACTCE00")) %>%
    setnames("GISJOIN", "GJOIN2000") %>%
    setnames("TRACTCE00", "TRACT2000")

  stopifnot(dt_trct_shp_2000[stri_length(TRACT2000) != 6] %>% nrow() == 0)

  dt_trct_shp_2010 <- CLmisc::get_rnhgis_shp("us_tract_2010_tl2020") %>%
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "TRACTCE10")) %>%
    setnames("GISJOIN", "GJOIN2010") %>%
    setnames("TRACTCE10", "TRACT2010")

  stopifnot(dt_trct_shp_2010[stri_length(TRACT2010) != 6] %>% nrow() == 0)

  dt_trct_shp_2020 <- CLmisc::get_rnhgis_shp("us_tract_2020_tl2020") %>%
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "TRACTCE")) %>%
    setnames("GISJOIN", "GJOIN2020") %>%
    setnames("TRACTCE", "TRACT2020")

  stopifnot(dt_trct_shp_2020[stri_length(TRACT2020) != 6] %>% nrow() == 0)

  dt_trct_gjoin_trct_cw <- dt_trct_gjoin_trct_cw %>%
    merge(dt_trct_shp_1980, by = "GJOIN1980", all.x = TRUE) %>%
    merge(dt_trct_shp_1990, by = "GJOIN1990", all.x = TRUE) %>%
    merge(dt_trct_shp_2000, by = "GJOIN2000", all.x = TRUE) %>%
    merge(dt_trct_shp_2010, by = "GJOIN2010", all.x = TRUE) %>%
    merge(dt_trct_shp_2020, by = "GJOIN2020", all.x = TRUE)

  dt_trct_gjoin_trct_cw <- dt_trct_gjoin_trct_cw %>%
    setcolorder(colorder)

  nanoparquet::write_parquet(
    dt_trct_gjoin_trct_cw, save_file_loc
  )

  return(dt_trct_gjoin_trct_cw)

}

f_get_std_to_blkgrp2010_shp_hu <- function() {

  box::use(
    data.table[...], magrittr[`%>%`], stringi[stri_sub], CLmisc[mkdir_p, select_by_ref]
  )

  save_dir <- here::here("work/f_get_std_blkgrp2010_shp_hu")
  mkdir_p(save_dir)
  save_file_loc <- file.path(save_dir, "std_to_blkgrp2010_shp_hu.rds")

  if (file.exists(save_file_loc))
    return(readRDS(save_file_loc))
  
  ## Use housing units standardized to 2010 block groups
  dt_blkgrp_hu <- CLmisc::get_rnhgis_tst(name = "CM7", geog_levels = "blck_grp") %>%
    setDT() %>%
    select_by_ref(c("GISJOIN", "CM7AA1990", "CM7AA2000", "CM7AA2010",
                    "CM7AA2020")) %>%
    setnames(
      names(.), c("gjoin_blkgrp2010", "hu1990", "hu2000", "hu2010", "hu2020")
    ) %>%
    setkey(gjoin_blkgrp2010)
  
  dt_blkgrp_shp <- CLmisc::get_rnhgis_shp("us_blck_grp_2010_tl2020") %>% 
    as.data.table() %>%
    select_by_ref(c("GISJOIN", "geometry")) %>%
    .[, geometry := sf::st_transform(geometry, crs = 5070)] %>%
    setnames("GISJOIN", "gjoin_blkgrp2010") %>%
    setkey(gjoin_blkgrp2010)

  dt <- dt_blkgrp_hu[dt_blkgrp_shp, on = "gjoin_blkgrp2010"] %>%
    .[stri_sub(gjoin_blkgrp2010, 2, 3) %notin% c("72")]

  saveRDS(dt, save_file_loc)

  return(dt)

}


f_get_tract_to_tract_cw <- function(from_yr, to_yr) {

  box::use(
    data.table[...], magrittr[`%>%`], stringi[stri_sub, stri_replace_first_regex],
    CLmisc[mkdir_p, select_by_ref, get_rnhgis_shp, get_rnhgis_tst]
  )

  stopifnot(
    from_yr %in% c(1990, 2000, 2010, 2020),
    to_yr %in% c(1990, 2000, 2010, 2020)
  )

  save_dir <- here::here("work/f_get_tract_to_tract_cw")
  CLmisc::mkdir_p(save_dir)
  save_file_loc <- file.path(
    save_dir, paste0("tract_to_tract_cw_", from_yr, "_", to_yr, ".parquet")
  )

  if (file.exists(save_file_loc))
    return(nanoparquet::read_parquet(save_file_loc) %>% setDT())

  dt_blkgrp_shp_hu <- f_get_std_to_blkgrp2010_shp_hu() %>%
    setnames(paste0("hu", from_yr), "housing_units") %>%
    select_by_ref(c("gjoin_blkgrp2010", "housing_units", "geometry"))

  dt_trct_shp_from <- switch(as.character(from_yr),
    "2020" = CLmisc::get_rnhgis_shp("us_tract_2020_tl2020"),
    "2010" = CLmisc::get_rnhgis_shp("us_tract_2010_tl2020"),
    "2000" = CLmisc::get_rnhgis_shp("us_tract_2000_tl2010"),
    "1990" = CLmisc::get_rnhgis_shp("us_tract_1990_tl2008")
  ) %>% as.data.table() %>%
    select_by_ref(c("GISJOIN", "geometry")) %>%
    setnames("GISJOIN", "from_gjoin") %>%
    .[substr(from_gjoin, 2, 3) %notin% c("72")] %>%
    .[, geometry := sf::st_transform(geometry, crs = 5070)] 

  dt_trct_shp_to <- switch(as.character(to_yr),
    "2020" = CLmisc::get_rnhgis_shp("us_tract_2020_tl2020"),
    "2010" = CLmisc::get_rnhgis_shp("us_tract_2010_tl2020"),
    "2000" = CLmisc::get_rnhgis_shp("us_tract_2000_tl2010"),
    "1990" = CLmisc::get_rnhgis_shp("us_tract_1990_tl2008")
  ) %>% as.data.table() %>%
    select_by_ref(c("GISJOIN", "geometry")) %>%
    setnames("GISJOIN", "to_gjoin") %>%
    .[substr(to_gjoin, 2, 3) %notin% c("72")] %>%
    .[, geometry := sf::st_transform(geometry, crs = 5070)] 

  dt_trct_hu <- CLmisc::get_rnhgis_tst(name = "A41", geog_levels = "tract") %>%
    setDT() %>%
    .[substr(NHGISCODE, 2, 3) %notin% c("72")] %>%
    select_by_ref(c(paste0("GJOIN", from_yr), paste0("GJOIN", to_yr),
                    paste0("A41AA", from_yr))) %>%
    setnames(paste0("GJOIN", from_yr), "from_gjoin") %>%
    setnames(paste0("GJOIN", to_yr), "to_gjoin") %>%
    setnames(paste0("A41AA", from_yr), "housing_units") %>%
    .[!(is.na(from_gjoin) & is.na(to_gjoin))] %>%
    .[!is.na(from_gjoin)]

  stopifnot(nrow(dt_trct_hu[is.na(housing_units)]) == 0)

  dt_cw_1_to_1 <- dt_trct_hu %>%
    .[!is.na(from_gjoin) & !is.na(to_gjoin)]

  dt_partial_match_intersects <- sf::st_join(
    sf::st_as_sf(dt_trct_shp_from[from_gjoin %notin% dt_cw_1_to_1$from_gjoin]),
    sf::st_as_sf(dt_trct_shp_to),
    join = sf::st_intersects,
    left = TRUE
  ) %>%
    as.data.table() %>%
    .[, geometry := NULL]

  f_wrkr_cnty <- function(from_gjoin_cnty) {

    cnty_save_dir <- file.path(
      save_dir, paste0("from_tract", from_yr, "_to_tract", to_yr, "_by_cnty")
    )
    mkdir_p(cnty_save_dir)
    cnty_save_file_loc <- file.path(
      cnty_save_dir, paste0("tract_to_tract_cw_cnty", from_gjoin_cnty, ".parquet")
    )

    if (file.exists(cnty_save_file_loc)) return()

    dt_partial_matches_wrkr <- dt_partial_match_intersects[
      stri_sub(from_gjoin, 1, 8) == from_gjoin_cnty
    ]

    dt_from_wrkr <- dt_trct_shp_from %>%
      .[from_gjoin %chin% dt_partial_matches_wrkr$from_gjoin] %>%
      setnames("from_gjoin", "from_geoid")

    dt_to_wrkr <- dt_trct_shp_to %>%
      .[to_gjoin %chin% dt_partial_matches_wrkr$to_gjoin] %>%
      setnames("to_gjoin", "to_geoid")

    from_nhgis_state <- stri_sub(dt_from_wrkr$from_geoid, 1, 4) %>% unique()

    dt_blkgrp_shp_hu_wrkr <- sf::st_join(
      sf::st_as_sf(
        dt_blkgrp_shp_hu[stri_sub(gjoin_blkgrp2010, 1, 4) %chin% from_nhgis_state]
      ),
      sf::st_as_sf(dt_from_wrkr),
      join = sf::st_intersects,
      left = FALSE
    ) %>%
      as.data.table() %>%
      .[, from_geoid := NULL] %>%
      .[!duplicated(gjoin_blkgrp2010)] %>% 
      setnames("gjoin_blkgrp2010", "wts_geoid") %>%
      setnames("housing_units", "wt_var")

    dt_cw <- geolinkr::create_cw_worker(
      dt_from = dt_from_wrkr,
      dt_to = dt_to_wrkr,
      dt_wts = dt_blkgrp_shp_hu_wrkr
    ) %>%
      .[, afact := NULL] %>%
      setnames("wt_var", "housing_units") %>%
      setnames("from_geoid", "from_gjoin") %>%
      setnames("to_geoid", "to_gjoin")

    if (nrow(dt_cw) == 0) {
      arrow::write_parquet(
        dt_cw, cnty_save_file_loc
      )
    } else {
      nanoparquet::write_parquet(
        dt_cw, cnty_save_file_loc
      )
    }

    return()

  }

  cntys <- dt_partial_match_intersects %>%
    .[, stri_sub(from_gjoin, 1, 8)] %>%
    unique()

  ## tmp <- f_wrkr_cnty("G0100010")

  options(future.globals.maxSize = 1000 * 15 * 1024^2)
  future::plan(future::multisession(workers = future::availableCores() - 6))
  future.apply::future_lapply(cntys, f_wrkr_cnty, future.seed = TRUE)
  future::plan(future::sequential())

  dt_partial_matches_cw <- list.files(
    path = file.path(
      save_dir, paste0("from_tract", from_yr, "_to_tract", to_yr, "_by_cnty")
    ),
    pattern = paste0("*.parquet"),
    full.names = TRUE
  ) %>%
    lapply(\(x) nanoparquet::read_parquet(x) %>% setDT()) %>%
    rbindlist(., use.names = TRUE)

  dt_cw_out <- rbind(
    dt_cw_1_to_1,
    dt_partial_matches_cw,
    use.names = TRUE
  ) %>%
    .[, .(housing_units = f_sum(housing_units)), by = .(from_gjoin, to_gjoin)] 

  dt_cw_nhgis_to_tract_cw <- f_get_tract_to_nhgis_tract_cw() %>%
    select_by_ref(c(paste0("GJOIN", from_yr), paste0("GJOIN", to_yr),
                    "STATEFP", "COUNTYFP", 
                    paste0("TRACT", from_yr), paste0("TRACT", to_yr))) %>%
    setnames(paste0("GJOIN", from_yr), "from_gjoin") %>%
    setnames(paste0("GJOIN", to_yr), "to_gjoin") %>%
    setnames(paste0("TRACT", from_yr), paste0("TRACT_from")) %>%
    setnames(paste0("TRACT", to_yr), paste0("TRACT_to")) %>%
    .[!(is.na(from_gjoin) & is.na(to_gjoin))] %>%
    .[, from_census_tract := paste0(STATEFP, COUNTYFP, TRACT_from)] %>%
    .[, to_census_tract := paste0(STATEFP, COUNTYFP, TRACT_to)] %>%
    .[, c("STATEFP", "COUNTYFP") := NULL]

  dt_cw_out <- dt_cw_out %>%
    merge(dt_cw_nhgis_to_tract_cw[!is.na(from_gjoin), .(from_gjoin, from_census_tract)],
          by = "from_gjoin", all.x = TRUE) %>%
    merge(dt_cw_nhgis_to_tract_cw[!is.na(to_gjoin), .(to_gjoin, to_census_tract)],
          by = "to_gjoin", all.x = TRUE) %>%
    .[, from_census_yr := from_yr] %>%
    .[, to_census_yr := to_yr] %>%
    setcolorder(c(
      "from_gjoin", "from_census_tract", "from_census_yr",
      "to_gjoin", "to_census_tract", "to_census_yr", "housing_units"
    )) %>%
    setnames("housing_units", paste0("housing_units_", from_yr))

  nanoparquet::write_parquet(dt_cw_out, save_file_loc)
  return(dt_cw_out)

}
