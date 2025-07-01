

<!-- README.md is generated from README.qmd. Please edit that file -->

# Crosswalks to from Census Tracts to Census Tracts Across Census Years

- Crosswalks in `crosswalks`.
- If you use, please cite:

> Lutz, Chandler. “Bridging the Geographic Divide: Crosswalks Across
> Space and Time.” *Working Paper* (2025).
> https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5126949

# Example

``` r
cw_trct2000_to_trct2020 <- nanoparquet::read_parquet(
  "./crosswalks/cw_tract2000_to_tract2020.parquet"
)
print(head(cw_trct2000_to_trct2020))
```

          from_gjoin from_census_tract from_census_yr to_gjoin to_census_tract
    1 G0600370134902       06037134902           2000     <NA>            <NA>
    2 G0600370207300       06037207300           2000     <NA>            <NA>
    3 G0600370209200       06037209200           2000     <NA>            <NA>
    4 G0600650043504       06065043504           2000     <NA>            <NA>
    5 G0600710002007       06071002007           2000     <NA>            <NA>
    6 G1200210011202       12021011202           2000     <NA>            <NA>
      to_census_yr housing_units_2000
    1         2020               1196
    2         2020                975
    3         2020                549
    4         2020               2025
    5         2020                432
    6         2020                951

# References

- Lutz, Chandler. “Bridging the Geographic Divide: Crosswalks Across
  Space and Time.” *Working Paper* (2025).
- Steven Manson, Jonathan Schroeder, David Van Riper, Katherine Knowles,
  Tracy Kugler, Finn Roberts, and Steven Ruggles. IPUMS National
  Historical Geographic Information System: Version 19.0 \[dataset\].
  Minneapolis, MN: IPUMS. 2024. http://doi.org/10.18128/D050.V19.0

# License

- [MIT](https://choosealicense.com/licenses/mit/)
- If you use, please cite:

> Lutz, Chandler. “Bridging the Geographic Divide: Crosswalks Across
> Space and Time.” *Working Paper* (2025).
> https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5126949
