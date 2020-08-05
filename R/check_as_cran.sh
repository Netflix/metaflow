rm -rf cran_check
mkdir -p cran_check;
cp -r inst ./cran_check/ 
cp -r man ./cran_check/ 
cp -r R ./cran_check/
cp -r vignettes ./cran_check/
cp DESCRIPTION ./cran_check/
cp NAMESPACE ./cran_check/
cp LICENSE ./cran_check/
cd cran_check; R CMD build . ; R CMD check --as-cran metaflow_*.tar.gz
