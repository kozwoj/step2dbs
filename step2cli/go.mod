module github.com/kozwoj/step2cli

go 1.24.1

require (
	github.com/kozwoj/step2 v0.0.0
	github.com/kozwoj/step2query v0.0.0-00010101000000-000000000000
)

require (
	github.com/RoaringBitmap/roaring v1.9.4 // indirect
	github.com/bits-and-blooms/bitset v1.12.0 // indirect
	github.com/go-chi/chi/v5 v5.2.5 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/kozwoj/indexing v0.0.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

replace github.com/kozwoj/step2 => ../step2

replace github.com/kozwoj/step2query => ../step2query

replace github.com/kozwoj/indexing => ../indexing
