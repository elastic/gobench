# gobench

Index Go benchmark data into Elasticsearch with ease.

## Usage

Pipe the output of "go test -bench ..." to gobench.

By specifying an Elasticsearch URL via the "-es" flag,
gobench will index the results into Elasticsearch directly.
Without this flag, gobench will output actions suitable
for use with the Elasticsearch bulk API.

```bash
go test -bench . -benchmem ./... | gobench -es http://localhost:9200
```

## License

Apache 2.0.
