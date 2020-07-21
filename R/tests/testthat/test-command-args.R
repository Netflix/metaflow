#!/usr/bin/env Rscript
library(metaflow)

flags <- metaflow:::parse_arguments()
saveRDS(flags, "flags.RDS")
