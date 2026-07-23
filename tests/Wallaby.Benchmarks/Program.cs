using BenchmarkDotNet.Running;

BenchmarkSwitcher.FromAssembly(typeof(Wallaby.Benchmarks.RouterBenchmarks).Assembly).Run(args);
