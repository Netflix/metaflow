configs <- list(
  default = list(
    metaflow_path = expression(reticulate::py_discover_config("metaflow")$required_module_path)
  ),
  batch = list(
    metaflow_path = expression(path.expand(paste0(getwd(), "/metaflow")))
  )
)
