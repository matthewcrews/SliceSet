# SliceSet: A Data Structure for Slicing Multi-Dimensional Data

A problem I frequently run into during my work is the need to be able to slice a set of data across arbitrary dimensions. At two dimensions, it is simple to manually create the different permutations of the data. As the dimensions go up, though, this becomes space inefficient. I want a data structure that provides this capability for me automatically while being space-efficient and fast.

This repo is a testbed for different mechanisms for slicing 3-dimensional data down to a single dimension. I set up benchmarks for different amounts of data and different sparsities. The ideal solution has the best performance across all data volumes and sparsities.

To date, the best version is `ArrayPool`. `AltLoop` is better in some scenarios but is not significant enough to provide a meaningful difference. `CustomPool2` appears better in the results below but it is worse in scenarios where there are fewer Products in the data.

I welcome all suggestions and critiques. I am working on getting my head around an SSE/AVX version, but the complexity is beyond me now. An SSE/AVX version will also be more sensitive to the underlying hardware since AMD and Intel performance varies more regarding these instructions.

If you would like to submit an approach, please add unit tests to ensure that your solution gets the same results as the baseline, `NaiveFilter`.

## Results

Below are the most important benchmark case results for 1,600 Products and a Sparsity of 0.1%. This case is the closest to real-world usage. You can change the test cases you want to include by updating the Benchmarks class if you want to see how an approach behaves with a different number of Products and varying Sparsities.


|         Method | Size | Sparsity |         Mean |      Error |     StdDev |     Gen 0 | CacheMisses/Op | BranchMispredictions/Op | BranchInstructions/Op | Code Size |  Gen 1 | Allocated |
|--------------- |----- |--------- |-------------:|-----------:|-----------:|----------:|---------------:|------------------------:|----------------------:|----------:|-------:|----------:|
|    NaiveFilter | 1600 |     0.1% | 16,017.30 us | 303.507 us | 311.679 us | 2750.0000 |        131,965 |                  26,441 |            46,898,290 |      5 KB |      - | 22,715 KB |
| ParallelRanges | 1600 |     0.1% |     84.94 us |   0.957 us |   0.848 us |    1.3428 |          1,675 |                   1,716 |               213,009 |      9 KB |      - |     11 KB |
| ComputedRanges | 1600 |     0.1% |     22.46 us |   0.437 us |   0.552 us |    7.4158 |            305 |                     159 |                57,819 |      3 KB | 0.0305 |     61 KB |
|   CustomSeries | 1600 |     0.1% |     19.53 us |   0.242 us |   0.226 us |    6.1340 |            281 |                     123 |                60,111 |      4 KB | 0.0610 |     50 KB |
|      ArrayPool | 1600 |     0.1% |     16.42 us |   0.272 us |   0.254 us |    1.7090 |            114 |                      74 |                46,907 |      6 KB |      - |     14 KB |
|     Branchless | 1600 |     0.1% |     31.69 us |   0.612 us |   0.629 us |    1.7090 |            173 |                     122 |                49,113 |      6 KB |      - |     14 KB |
|     CustomPool | 1600 |     0.1% |     16.37 us |   0.317 us |   0.281 us |    1.7090 |            129 |                      79 |                44,491 |      4 KB |      - |     14 KB |
|    CustomPool2 | 1600 |     0.1% |     15.03 us |   0.294 us |   0.302 us |    2.0447 |            121 |                      77 |                43,381 |      4 KB |      - |     17 KB |
| GroupIntersect | 1600 |     0.1% |     61.00 us |   1.174 us |   1.257 us |    0.1221 |            207 |                   1,624 |               171,342 |     10 KB |      - |      1 KB |
|        AltLoop | 1600 |     0.1% |     15.87 us |   0.315 us |   0.387 us |    1.7090 |            124 |                      75 |                43,815 |      6 KB |      - |     14 KB |
|   BinarySearch | 1600 |     0.1% |     29.15 us |   0.576 us |   0.591 us |    1.7090 |            183 |                     540 |                79,029 |      6 KB |      - |     14 KB |
|      SkipIndex | 1600 |     0.1% |     24.40 us |   0.479 us |   0.813 us |    2.0142 |            155 |                     106 |                69,133 |      9 KB |      - |     17 KB |
|       AltIndex | 1600 |     0.1% |     18.32 us |   0.213 us |   0.189 us |    1.9531 |            138 |                      95 |                49,873 |      5 KB |      - |     16 KB |
|    Branchless2 | 1600 |     0.1% |     40.73 us |   0.306 us |   0.271 us |    1.7090 |            148 |                      90 |                42,847 |      6 KB |      - |     14 KB |
|            SOA | 1600 |     0.1% |     23.85 us |   0.465 us |   0.535 us |    1.9226 |            231 |                     108 |                72,159 |      6 KB |      - |     16 KB |
|   DoubleBuffer | 1600 |     0.1% |     17.93 us |   0.357 us |   0.334 us |    0.5798 |             90 |                      86 |                53,568 |      7 KB |      - |      5 KB |
