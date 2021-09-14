#' Decorator that configures resources allocated to a step
#'
#' @description 
#' These decorators control the resources allocated to step running either
#' locally or on _AWS Batch_. The `resources` decorator allocates resources for
#' local execution. However, when a flow is executed with the `batch` argument
#' (`run(with = c("batch")`.), it will also control which resources requested
#' from AWS. The `batch` decorator instead _forces_ the step to be run on _AWS
#' Batch_. See \url{https://docs.metaflow.org/v/r/metaflow/scaling} for more
#' information on how to use these decorators.
#'
#' If both `resources` and `batch` decorators are provided, the maximum values
#' from all decorators is used.
#'
#' @param cpu Integer number of CPUs required for this step. Defaults to `1`.
#' @param gpu Integer number of GPUs required for this step. Defaults to `0`.
#' @param memory Integer memory size (in MB) required for this step. Defaults to
#'   `4096`.
#' @param image Character. Specifies the image to use when launching on AWS
#'   Batch. If not specified, an appropriate
#'   \href{https://hub.docker.com/r/rocker/ml}{Rocker Docker image} will be
#'   used.
#' @param queue Character. Specifies the queue to submit the job to. Defaults to
#'   the queue determined by the environment variable "METAFLOW_BATCH_JOB_QUEUE"
#' @param iam_role Character. IAM role that AWS Batch can use to access Amazon
#'   S3. Defaults to the one determined by the environment variable
#'   METAFLOW_ECS_S3_ACCESS_IAM_ROLE
#' @param execution_role Character. IAM role that AWS Batch can use to trigger
#'   AWS Fargate tasks. Defaults to the one determined by the environment
#'   variable METAFLOW_ECS_FARGATE_EXECUTION_ROLE. See the
#'   \href{https://docs.aws.amazon.com/batch/latest/userguide/execution-IAM-role.html}{AWS
#'    Documentation} for more information.
#' @param shared_memory Integer. The value for the size (in MiB) of the
#'   `/dev/shm` volume for this step. This parameter maps to the `--shm-size`
#'   option to `docker run`.
#' @param max_swap Integer. The total amount of swap memory (in MiB) a container
#'   can use for this step. This parameter is translated to the `--memory-swap`
#'   option to docker run where the value is the sum of the container memory
#'   plus the `max_swap` value.
#' @param swappiness This allows you to tune memory swappiness behavior for this
#'   step. A swappiness value of `0` causes swapping not to happen unless
#'   absolutely necessary. A swappiness value of `100` causes pages to be
#'   swapped very aggressively. Accepted values are whole numbers between `0`
#'   and `100`.
#'   
#' @inherit decorator return
#'
#' @export
#' 
#' @examples \dontrun{
#' # This example will generate a large random matrix which takes up roughly 
#' # 48GB of memory, and sums the entries. The `batch` decorator forces this
#' # step to run in an environment with 60000MB of memory.
#' 
#' start <- function(self) {
#'   big_matrix <- matrix(rexp(80000*80000), 80000)
#'   self$sum <- sum(big_matrix)
#' }
#' 
#' end <- function(self) {
#'   message(
#'     "sum is: ", self$sum
#'   )
#' }
#' 
#' metaflow("BigSumFlowR") %>%
#'   step(
#'     batch(memory=60000, cpu=1),
#'     step = "start",
#'     r_function = start,
#'     next_step = "end"
#'   ) %>%
#'   step(
#'     step = "end",
#'     r_function = end
#'   ) %>%
#'   run()
#' }
batch <- function(
  cpu = 1L,
  gpu = 0L,
  memory = 4096L,
  image = NULL,
  queue = NULL,
  iam_role = NULL,
  execution_role = NULL,
  shared_memory = NULL,
  max_swap = NULL,
  swappiness = NULL
) {
  queue = queue %||% pkg.env$mf$metaflow_config$BATCH_JOB_QUEUE
  iam_role = iam_role %||% pkg.env$mf$metaflow_config$ECS_S3_ACCESS_IAM_ROLE
  execution_role = execution_role %||% pkg.env$mf$metaflow_config$ECS_FARGATE_EXECUTION_ROLE
  
  decorator(
    "batch",
    cpu = cpu,
    gpu = gpu,
    memory = memory,
    image = image,
    queue = queue,
    iam_role = iam_role,
    execution_role = execution_role,
    shared_memory = shared_memory,
    max_swap = max_swap,
    swappiness = swappiness
  )
}

#' @rdname batch
#' @export
resources <- function(
  cpu = 1L,
  gpu = 0L,
  memory = 4096L,
  shared_memory = NULL
) {
  decorator(
    "resources",
    cpu = cpu,
    gpu = gpu,
    memory = memory,
    shared_memory = shared_memory
  )
}
