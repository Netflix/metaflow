#!/usr/bin/env Rscript
library(metaflow)

run_cmd <- metaflow:::run_cmd("flow.RDS")
saveRDS(run_cmd, "run_cmd.RDS")
