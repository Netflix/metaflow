.onAttach <- function(libname, pkgname){
  tryCatch({
    metaflow_attach()
  }, error = function(e){
    message("  Please install Metaflow python dependencies by running 'metaflow::install()' in R *** ")
  }) 
  invisible(NULL)
} 

.onLoad <- function(libname, pkgname) {
  tryCatch({
    metaflow_load()
  }, error = function(e){
    message("  Please install Metaflow python dependencies by running 'metaflow::install()' in R *** ")
  }) 
  invisible(NULL)
}

