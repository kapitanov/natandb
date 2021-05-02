# NatanDB

A key-value (or more exactly key-array) standalone database written just for fun.

## Features

* Supports `List`, `Get`, `Set`, `Add`/`Add(unique)`, `Remove`/`Remove(all)`, `RemoveAll`, `Delete` commands.
* Built-in [GRPC interface](./pkg/proto/natan.proto)
* ACID transactions (which do not work over GRPC so far)
* Write-ahead log compression

## Performance

### Read performance

![](docs/read-perf.png)

| Concurrent clients | Num of operations | Performance |
| ------------------ | ----------------- | ----------- |
| 1                  | 10000             | 4566.9 rps  |
| 2                  | 10000             | 7867.3 rps  |
| 3                  | 10000             | 9270.4 rps  |
| 4                  | 10000             | 11315.0 rps |
| 5                  | 10000             | 12689.9 rps |
| 6                  | 10000             | 13842.8 rps |
| 7                  | 10000             | 14789.2 rps |
| 8                  | 10000             | 15205.0 rps |

Test protocol:

1. Start NatanDB server:

   ```shell
   rm -rf ./data && ./natandb run -d ./data
   ```

2. Run tests (in a separate terminal):

   ```shell
   for i in `seq 1 8`; do ./natandb test read -n 10000 -q -t $i $i; done
   ```

### Write performance

![](docs/write-perf.png)

| Concurrent clients | Num of operations | Performance |
| ------------------ | ----------------- | ----------- |
| 1                  | 10000             | 3101.6 rps  |
| 2                  | 10000             | 6010.6 rps  |
| 3                  | 10000             | 6674.4 rps  |
| 4                  | 10000             | 7130.7 rps  |
| 5                  | 10000             | 7792.5 rps  |
| 6                  | 10000             | 7897.0 rps  |
| 7                  | 10000             | 7399.5 rps  |
| 8                  | 10000             | 8098.5 rps  |

Test protocol:

1. Start NatanDB server:

   ```shell
   rm -rf ./data && ./natandb run -d ./data
   ```

2. Run tests (in a separate terminal):

   ```shell
   for i in `seq 1 8`; do ./natandb test write -n 10000 -q -t $i $i; done
   ```

## License

[MIT](LICENSE)
