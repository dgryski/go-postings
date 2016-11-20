package postings

import (
	"sort"
	"strconv"
)

// https://blog.twitter.com/2016/omnisearch-index-formats
// https://www.umiacs.umd.edu/~jimmylin/publications/Busch_etal_ICDE2012.pdf
// http://www.slideshare.net/lucenerevolution/twitter-search-lucenerevolutioneu2013-copy

type DocID uint32

type TermID uint32

type Index struct {
	p         map[TermID][]Posting
	nextDocID DocID
	stop      map[TermID]struct{}
}

func NewIndex(stop []TermID) *Index {
	p := make(map[TermID][]Posting)

	var mstop map[TermID]struct{}

	if len(stop) > 0 {
		mstop = make(map[TermID]struct{}, len(stop))

		for _, s := range stop {
			mstop[s] = struct{}{}
		}
	}

	return &Index{p: p, stop: mstop}
}

func (idx *Index) AddDocumentPositions(terms []TermID) DocID {

	if len(terms) > 256 {
		terms = terms[:256]
	}

	id := idx.nextDocID
	for i, t := range terms {
		if _, ok := idx.stop[t]; ok {
			continue
		}
		idx.p[t] = append(idx.p[t], makePosting(id, byte(i)))
	}

	idx.nextDocID++

	return id
}

func (idx *Index) AddDocument(terms []TermID) DocID {

	id := idx.nextDocID
	for _, t := range terms {
		if _, ok := idx.stop[t]; ok {
			continue
		}

		idxt := idx.p[t]
		if len(idxt) == 0 || idxt[len(idxt)-1].Doc() != id {
			idx.p[t] = append(idx.p[t], makePosting(id, byte(0)))
		}
	}

	idx.nextDocID++

	return id
}

type tfList struct {
	terms []TermID
	freq  []int
}

func (tf tfList) Len() int { return len(tf.terms) }
func (tf tfList) Swap(i, j int) {
	tf.terms[i], tf.terms[j] = tf.terms[j], tf.terms[i]
	tf.freq[i], tf.freq[j] = tf.freq[j], tf.freq[i]
}
func (tf tfList) Less(i, j int) bool { return tf.freq[i] < tf.freq[j] }

// Query returns a list of postings that match the terms
func (idx *Index) Query(ts []TermID) []Posting {

	var freq []int

	for _, t := range ts {
		d := idx.p[t]
		if len(d) == 0 {
			return nil
		}
		freq = append(freq, len(d))
	}

	sort.Sort(tfList{ts, freq})

	docs := idx.p[ts[0]]

	result := make([]Posting, len(docs))

	for _, t := range ts[1:] {
		d := idx.p[t]
		result = intersect(result[:0], docs, d)
		docs = result
	}

	return docs
}

// QueryPhrase returns a list of postings that match the term phrase
func (idx *Index) QueryPhrase(ts []TermID) []Posting {

	for _, t := range ts {
		if len(idx.p[t]) == 0 {
			return nil
		}
	}

	docs := idx.p[ts[0]]

	result := make([]Posting, len(docs))

	for _, t := range ts[1:] {
		d := idx.p[t]
		result = intersectPhrase(result[:0], docs, d)
		docs = result
	}

	return docs
}

//  Posting is a document and character position
type Posting uint32

const (
	// high 24-bits are the document ID
	docMask = 0xffffff00
	// low byte is the position
	posMask = 0xff
)

// doc returns the masked docID
func (p Posting) Doc() DocID {
	return DocID(p) >> 8
}

// pos returns the position of the term in the document
func (p Posting) Pos() uint32 {
	return uint32(p) & posMask
}

func (p Posting) String() string {
	return strconv.Itoa(int(p.Doc())) + "/" + strconv.Itoa(int(p.Pos()))
}

const debug = false

func makePosting(doc DocID, pos uint8) Posting {
	if debug && (doc&0xff000000) != 0 {
		panic("bad document ID")
	}
	return Posting(doc)<<8 | Posting(pos)
}

// using iterators so we can abstract away posting lists once pools are implemented

// piter is a posting list iterator
type piter struct {
	list []Posting
	idx  int
}

func newIter(l []Posting) piter {
	return piter{list: l}
}

func (it *piter) next() bool {
	if it.idx >= len(it.list) {
		return false
	}

	it.idx++
	return it.idx < len(it.list)
}

func (it *piter) advance(d DocID) bool {

	// galloping search
	bound := 1
	for it.idx+bound < len(it.list) && d > it.list[it.idx+bound].Doc() {
		bound *= 2
	}

	// inlined binary search between the last two steps
	n := d
	low, high := it.idx+bound/2, it.idx+bound
	if high > len(it.list) {
		high = len(it.list)
	}

	for low < high {
		mid := low + (high-low)/2
		if it.list[mid].Doc() >= n {
			high = mid
		} else {
			low = mid + 1
		}
	}

	// linear scan back for the start of this document
	if low < len(it.list) {
		n = it.list[low].Doc()
		for low > 0 && n == it.list[low-1].Doc() {
			low--
		}
	}

	it.idx = low

	return !it.end()
}

func (it *piter) end() bool {
	return it.idx >= len(it.list)
}

func (it *piter) at() Posting {
	return it.list[it.idx]
}

// intersectPhrase returns the intersection of two posting lists with the terms in b
// occurring immediate after the terms in a
func intersectPhrase(result, a, b []Posting) []Posting {

	ait := newIter(a)
	bit := newIter(b)

scan:
	for !ait.end() && !bit.end() {
		for ait.at().Doc() == bit.at().Doc() {

			if ait.at().Pos()+1 == bit.at().Pos() {
				result = append(result, bit.at())

				if !ait.next() {
					break scan
				}

				if !bit.next() {
					break scan
				}
				continue
			}

			if ait.at().Pos() < bit.at().Pos() {
				if !ait.next() {
					break scan
				}
				continue
			}

			if ait.at().Pos() >= bit.at().Pos() {
				if !bit.next() {
					break scan
				}
			}
		}

		for ait.at().Doc() < bit.at().Doc() {
			if !ait.advance(bit.at().Doc()) {
				break scan
			}
		}

		for !bit.end() && ait.at().Doc() > bit.at().Doc() {
			if !bit.advance(ait.at().Doc()) {
				break scan
			}
		}
	}

	return result
}

// intersect returns the intersection of two posting lists
// postings are returned deduplicated.
func intersect(result, a, b []Posting) []Posting {

	ait := newIter(a)
	bit := newIter(b)

scan:
	for !ait.end() && !bit.end() {

		for ait.at().Doc() == bit.at().Doc() {

			result = append(result, bit.at())

			var d DocID

			d = ait.at().Doc()
			for ait.at().Doc() == d {
				if !ait.next() {
					break scan
				}
			}

			d = bit.at().Doc()
			for bit.at().Doc() == d {
				if !bit.next() {
					break scan
				}
			}
		}

		for ait.at().Doc() < bit.at().Doc() {
			if !ait.advance(bit.at().Doc()) {
				break scan
			}
		}

		for !bit.end() && ait.at().Doc() > bit.at().Doc() {
			if !bit.advance(ait.at().Doc()) {
				break scan
			}
		}
	}

	return result
}
