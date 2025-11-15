.PHONY: build run test clean repl

# Build the REPL
build:
		@echo "🔨 Building Sharingan DB REPL..."
		@go build -o bin/sharingan-db cmd/repl/main.go
		@echo "✅ Build complete: bin/sharingan-db"

# Run the REPL
repl: build
		@./bin/sharingan-db
# Run tests
test:
		@echo "🧪 Running tests..."
		@go test ./... -v
# Run benchmarks
bench:
		@echo "⚡ Running benchmarks..."
		@go test ./internal/query -bench=. -benchmem
# Clean build artifacts and database files
clean:
		@echo "🧹 Cleaning..."
		@rm -f bin/sharingan-db
		@rm -f sharingan.db sharingan.wal sharingan.wal.meta
		@rm -f test_*.db test_*.wal bench_*.db bench_*.wal
		@echo "✅ Clean complete"

# Install as system command (optional)
install: build
		@echo "📦 Installing to /usr/local/bin..."
		@sudo cp bin/sharingan-db /usr/local/bin/
		@echo "✅ Installed! Run with: sharingan-db"

# Show help
help:
		@echo "Sharingan DB - Build Commands"
		@echo ""
		@echo "Available targets:"
		@echo "  make build   - Build the REPL binary"
		@echo "  make repl    - Build and run the REPL"
		@echo "  make test    - Run all tests"
		@echo "  make bench   - Run benchmarks"
		@echo "  make clean   - Remove build artifacts and database files"
		@echo "  make install - Install to /usr/local/bin (requires sudo)"
		@echo "  make help    - Show this help"

# Run comprehensive benchmarks
bench-all:
		@echo "⚡ Running comprehensive benchmark suite..."
		@./scripts/run_benchmarks.sh

# Run 100k correctness test
test-100k:
		@echo "✅ Testing 100k operations..."
		@go test -v ./internal/benchmark -run Test100kCorrectnessWithTraversal -timeout 30m

# Run specific benchmarks
bench-insert:
		@go test -v ./internal/benchmark -bench=Benchmark100kInserts -benchtime=1x

bench-read:
		@go test -v ./internal/benchmark -bench=Benchmark100kReads -benchtime=1x

bench-mixed:
		@go test -v ./internal/benchmark -bench=BenchmarkMixedWorkload -benchtime=1x