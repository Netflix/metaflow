#!/usr/bin/env Rscript
library(metaflow)

run_argv <- metaflow:::run_argv("flow.RDS")
saveRDS(run_argv, "run_cmd.RDS")
