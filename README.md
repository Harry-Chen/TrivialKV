# TrivialKV

A naive key-value database as the project of Storage Technology Foundations course.

## Build

You can choose CMake or Makefile to build this project.

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

Go to your build output directory (`build` for CMake and `.` for Makefile), then execute:

### Correctness tests

```bash
cd test
./{single_thread,multi_thread,crash}test # for CMake
./run_tests.sh # for Makefile
```

### Performance benchmark

```bash
./bench/bench THREAD_NUM READ_RATIO IS_SKEW
```
