# TrivialKV

A naive key-value Database as the project of Storage Technology Foundations course.

## Build

You can choose CMake or Makefile to build this project. It is recommended to use CMake.

### CMake

```bash
mkdir build && cd build
cmake .. && make
```

### Makefile

```bash
make
cd test && ./build.sh && cd ..
cd bench && ./build.sh && cd ..
```

Or you can use `make TARGET_ENGINE=engine_example` for the example engine.

## Tests and benchmark

### Important notes

The program will open lots of files, make sure that your `ulimit` is sufficient or the it will throw assertion errors.

Run `ulimit -n $(ulimit -Sn)` to set the open file limit to the maximum value allowed by the system.

### Correctness tests

Go to your build output directory (`build` for CMake and `.` for Makefile), then execute:

```bash
cd test
./{single_thread,multi_thread,crash}test # for CMake
./run_tests.sh # for Makefile
```

If the program exits immediately, make sure `data` dir exists.

### Performance benchmark

Also go to your build output directory:

```bash
./bench/bench THREAD_NUM READ_RATIO IS_SKEW
```
