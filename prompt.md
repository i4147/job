# task
there are diffrent implementations of multiprocessing in python in this repo
write a benchmark to compare these implementations, save results as json
report top 5 methods
functiom to test is a function that calculates hash of files of a user defined folderpath (default is home folder, if not specified)
the benchmark should have warmup phase, and must be verbose
the report must specifically determine which functions is used in top 5 methods
example: function imap from multiprocessing.Pool or function ProcessPoolExecutor from concurrent.futures
make InterpreterPoolExecutor optional if python version is under 3.14
save benchmark to a folder and wait til i test it here too
add helper scripts you used during session to arena folder in root of connected repo
fix any existing errors in files that may cause problem during benchmark, before running benchmark

