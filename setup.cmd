
pyinstaller --hidden-import=_mssql -F batchman.py -n wbs
copy README.txt dist
copy INSTALL.txt dist
mkdir dist\conf
copy conf\config.ini dist\conf
mkdir dist\examples
mkdir dist\examples\schedules
mkdir dist\examples\logs
mkdir dist\examples\scripts

copy schedules\*.* dist\examples\schedules
mkdir dist\docs
copy docs\*.* dist\docs
copy scripts\runsp.bat dist\examples\scripts
rd /s/q build
ren dist wbs

