package postings

// PostingsFormat defines the format used for storing record IDs in postings blocks
type PostingsFormat uint32

const (
	FormatSlice  PostingsFormat = 0 // Store record IDs as slice of uint32
	FormatBitmap PostingsFormat = 1 // Store record IDs as bitmap
)

type PostingsFileHeader struct {
	BlockSize        uint32         // size of each postings block in bytes
	FirstFreeBlock   uint32         // number of the first block in the free block list
	NumberOfPostings uint32         // total number of postings in the file
	Format           PostingsFormat // format used for storing record IDs (slice or bitmap)
}

type PostingBlockHeader struct {
	BlockNumber uint32 // the current block number for integrity maintenance
	DictID      uint32 // corresponding dictionary ID for integrity maintenance
	NextBlock   uint32 // block number of the next block of the list
	Count       uint32 // number of records IDs stored in the block
}

type PostingBlock struct {
	Header    PostingBlockHeader
	RecordIDs []uint32 // slice of record IDs stored in the block
}
