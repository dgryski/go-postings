package postings

import "github.com/dgryski/go-groupvarint"

type compressedBlock struct {
	groups []byte // the compressed data
	docID  DocID  // docID is the first ID in the block
	count  uint16 // how many IDs are in this block
}

type cblockiter struct {
	c *compressedBlock // pointer to compressed posting list

	group   [4]uint32 // the current group
	docID   DocID     // current docID (for tracking deltas)
	current uint16    // index into group[]
	count   uint16    // IDs processed, to match against c.count
	offs    uint16    // offset into c.groups
}

const blockLimitBytes = 4096

func newCompressedBlock(docs Postings) (Postings, compressedBlock) {

	cblock := compressedBlock{
		docID: docs[0],
	}

	prev := docs[0]

	buf := make([]byte, 17)
	deltas := make([]uint32, 4)

	for len(docs) >= 4 {
		deltas[0] = uint32(docs[0] - prev)
		deltas[1] = uint32(docs[1] - docs[0])
		deltas[2] = uint32(docs[2] - docs[1])
		deltas[3] = uint32(docs[3] - docs[2])

		b := groupvarint.Encode4(buf, deltas)

		if len(cblock.groups)+len(b) >= blockLimitBytes {
			return docs, cblock
		}

		cblock.groups = append(cblock.groups, b...)
		cblock.count += 4
		prev = docs[3]
		docs = docs[4:]
	}

	// the remaining
	for _, d := range docs {
		b := groupvarint.Encode1(buf, uint32(d-prev))

		if len(cblock.groups)+len(b) >= blockLimitBytes {
			return docs, cblock
		}

		cblock.groups = append(cblock.groups, b...)
		cblock.count++
		prev = d
		docs = docs[1:]
	}

	return docs, cblock
}

func newCompressedIter(cblock compressedBlock) *cblockiter {

	iter := &cblockiter{
		c:     &cblock,
		docID: cblock.docID,
	}

	// load the first group and set docID so at() is correct
	iter.load()
	iter.current = 1

	return iter
}

func (it *cblockiter) load() {
	rem := it.c.count - it.count
	if rem >= 4 {
		groupvarint.Decode4(it.group[:], it.c.groups[it.offs:])
		it.offs += uint16(groupvarint.BytesUsed[it.c.groups[it.offs]])
	} else {
		for i := uint16(0); i < rem; i++ {
			it.offs += uint16(groupvarint.Decode1(&it.group[i], it.c.groups[it.offs:]))
		}
	}
}

func (it *cblockiter) next() bool {

	it.count++

	// end of this group -- read another
	if it.current == 4 {
		it.load()
		it.current = 0
	}

	// consume next delta in group
	it.docID += DocID(it.group[it.current])
	it.current++

	return !it.end()
}

func (it *cblockiter) advance(d DocID) bool {

	for !it.end() && it.at() < d {
		it.next()
	}

	return !it.end()
}

func (it *cblockiter) at() DocID {
	return it.docID
}

func (it *cblockiter) end() bool {
	return it.count >= it.c.count
}
