module github.com/focusandinsist/consistent-go/test

go 1.21

require github.com/focusandinsist/consistent-go/consistent v0.0.0

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
)

replace github.com/focusandinsist/consistent-go/consistent => ../consistent
