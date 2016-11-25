package postings

import "github.com/dgryski/go-groupvarint"

type cPostings []compressedBlock

type CompressedIndex struct {
	p    map[TermID]cPostings
	freq map[TermID]int
}

func NewCompressedIndex(idx *Index) *CompressedIndex {
	cidx := CompressedIndex{
		p:    make(map[TermID]cPostings),
		freq: make(map[TermID]int),
	}

	for k, v := range idx.p {
		cidx.p[k], cidx.freq[k] = newCompressedPostings(v), len(v)
	}

	return &cidx
}

func (idx *CompressedIndex) Postings(t TermID) (Iterator, int) {
	return newCompressedIter(idx.p[t]), idx.freq[t]
}

func newCompressedPostings(p Postings) cPostings {
	var cpostings cPostings

	for len(p) > 0 {
		var cb compressedBlock
		p, cb = newCompressedBlock(p)
		cpostings = append(cpostings, cb)
	}

	return cpostings
}

type cpiter struct {
	c       cPostings
	blockID int
	it      *cblockiter
}

func newCompressedIter(cp cPostings) *cpiter {
	var it *cblockiter

	if len(cp) > 0 {
		it = newCBlockIter(cp[0])
	}

	return &cpiter{
		c:  cp,
		it: it,
	}

}

func (cp *cpiter) next() bool {

	if !cp.it.next() {
		// current iterator is finished
		// grab the next one
		cp.blockID++
		if cp.blockID == len(cp.c) {
			// no more blocks
			return false
		}
		cp.it = newCBlockIter(cp.c[cp.blockID])
	}

	return true
}

func (cp *cpiter) advance(d DocID) bool {

	if cp.blockID+1 < len(cp.c) && cp.c[cp.blockID+1].docID < d || !cp.it.advance(d) {
		// end of current iterator
		// linear scan for next one
		// TODO(dgryski): binary/galloping search?

		bound := 1
		for cp.blockID+bound < len(cp.c) && d > cp.c[cp.blockID+bound].docID {
			bound *= 2
		}

		// inlined binary search between the last two steps
		n := d
		low, high := cp.blockID+bound/2, cp.blockID+bound
		if high > len(cp.c) {
			high = len(cp.c)
		}

		for low < high {
			mid := low + (high-low)/2
			if cp.c[mid].docID >= n {
				high = mid
			} else {
				low = mid + 1
			}
		}

		cp.blockID = low - 1

		cp.it = newCBlockIter(cp.c[cp.blockID])
		if !cp.it.advance(d) {
			// wasn't in that block
			cp.blockID++

			if cp.blockID == len(cp.c) {
				return false
			}

			cp.it = newCBlockIter(cp.c[cp.blockID])
			return cp.it.advance(d)
		}
	}

	return !cp.end()
}

func (cp *cpiter) at() DocID {
	return cp.it.at()
}

func (cp *cpiter) end() bool {
	return cp.blockID == len(cp.c) || (cp.blockID == len(cp.c)-1 && cp.it.end())
}

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

const blockLimitBytes = 1024

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

func newCBlockIter(cblock compressedBlock) *cblockiter {

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
