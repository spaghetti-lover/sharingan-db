#!/bin/bash

echo "🔥 Sharingan DB - Comprehensive Benchmark Suite"
echo "================================================"
echo ""

# Clean up old files
echo "🧹 Cleaning up old benchmark files..."
rm -f bench_*.db bench_*.wal test_*.db test_*.wal
echo ""

# Run correctness test first
echo "✅ Step 1: Correctness Test (100k operations)"
echo "---------------------------------------------"
go test -v ./internal/benchmark -run Test100kCorrectnessWithTraversal -timeout 30m
echo ""

# Run insert benchmark
echo "⚡ Step 2: Insert Benchmark (100k sequential)"
echo "---------------------------------------------"
go test -v ./internal/benchmark -bench=Benchmark100kInserts -benchtime=1x -timeout 30m
echo ""

# Run read benchmark
echo "📖 Step 3: Read Benchmark (100k sequential)"
echo "--------------------------------------------"
go test -v ./internal/benchmark -bench=Benchmark100kReads -benchtime=1x -timeout 30m
echo ""

# Run mixed workload
echo "🔀 Step 4: Mixed Workload (70% read, 30% write)"
echo "------------------------------------------------"
go test -v ./internal/benchmark -bench=BenchmarkMixedWorkload -benchtime=1x -timeout 30m
echo ""

# Run traversal benchmark
echo "🌲 Step 5: In-Order Traversal (100k keys)"
echo "------------------------------------------"
go test -v ./internal/benchmark -bench=BenchmarkInOrderTraversal -benchtime=1x -timeout 30m
echo ""

# Run random insert benchmark
echo "🎲 Step 6: Random Insert Benchmark (100k)"
echo "------------------------------------------"
go test -v ./internal/benchmark -bench=BenchmarkRandomInserts -benchtime=1x -timeout 30m
echo ""

# Run buffer pool comparison
echo "📦 Step 7: Buffer Pool Size Comparison"
echo "---------------------------------------"
go test -v ./internal/benchmark -bench=BenchmarkBufferPoolSizes -benchtime=1x -timeout 30m
echo ""

echo "✅ All benchmarks completed!"
echo ""
echo "💡 Tip: Check individual benchmark logs above for detailed metrics"