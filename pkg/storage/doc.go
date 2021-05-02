package storage

// WAL binary format:
//
// +---+---------+---------+----------------------+
// | # | Length  | Group   | Field                |
// +---+---------+---------+----------------------+
// | 1 | 4 bytes | Header  | WAL header           |
// | 2 | N bytes | Record  | A single WAL record  |
// | 3 | N bytes | Record  | A single WAL record  |
// |   |         |         |                      |
// | M | N bytes | Record  | A single WAL record  |
// +---+---------+---------+----------------------+
//
// Each WAL record here is:
//
// +---+---------+---------+----------------------+
// | # | Length  | Group   | Field                |
// +---+---------+---------+----------------------+
// | 1 | 8 bytes | Header  | Record ID            |
// | 2 | 8 bytes | Header  | Record TX ID         |
// | 3 | 1 byte  | Header  | Record type          |
// | 4 | 4 bytes | Header  | "Key" field length   |
// | 5 | 4 bytes | Header  | "Value" field length |
// | 6 | N bytes | Payload | "Key" field          |
// | 7 | N bytes | Payload | "Value" field        |
// +---+---------+---------+----------------------+
