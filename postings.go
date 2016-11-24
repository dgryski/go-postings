package postings

import (
	"sort"
)

// https://blog.twitter.com/2016/omnisearch-index-formats
// https://www.umiacs.umd.edu/~jimmylin/publications/Busch_etal_ICDE2012.pdf
// http://www.slideshare.net/lucenerevolution/twitter-search-lucenerevolutioneu2013-copy

type DocID uint32

type TermID uint32

type Index struct {
	p         map[TermID]Postings
	nextDocID DocID
	stop      map[TermID]struct{}
}

func NewIndex(stop []TermID) *Index {
	p := make(map[TermID]Postings)

	var mstop map[TermID]struct{}

	if len(stop) > 0 {
		mstop = make(map[TermID]struct{}, len(stop))

		for _, s := range stop {
			mstop[s] = struct{}{}
		}
	}

	return &Index{p: p, stop: mstop}
}

func (idx *Index) AddDocument(terms []TermID) DocID {

	id := idx.nextDocID
	for _, t := range terms {
		if _, ok := idx.stop[t]; ok {
			continue
		}

		idxt := idx.p[t]
		if len(idxt) == 0 || idxt[len(idxt)-1] != id {
			idx.p[t] = append(idx.p[t], id)
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
func (idx *Index) Query(ts []TermID) Postings {

	freq := make([]int, len(ts))
	terms := make([]TermID, len(ts))
	for i, t := range ts {
		d := idx.p[t]
		if len(d) == 0 {
			return nil
		}
		terms[i] = t
		freq[i] = len(d)
	}

	sort.Sort(tfList{terms, freq})

	docs := idx.p[terms[0]]

	result := make(Postings, len(docs))

	for _, t := range terms[1:] {
		d := idx.p[t]
		result = intersect(result[:0], newIter(docs), newIter(d))
		docs = result
		if len(docs) == 0 {
			return nil
		}
	}

	return docs
}

//  Postings is a list of documents
type Postings []DocID

const debug = false

// piter is a posting list iterator
type piter struct {
	list Postings
	idx  int
}

func newIter(l Postings) *piter {
	return &piter{list: l}
}

func (it *piter) next() bool {
	it.idx++
	return !it.end()
}

func (it *piter) advance(d DocID) bool {

	// galloping search
	bound := 1
	for it.idx+bound < len(it.list) && d > it.list[it.idx+bound] {
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
		if it.list[mid] >= n {
			high = mid
		} else {
			low = mid + 1
		}
	}

	// linear scan back for the start of this document
	if low < len(it.list) {
		n = it.list[low]
		for low > 0 && n == it.list[low-1] {
			low--
		}
	}

	it.idx = low

	return !it.end()
}

func (it *piter) end() bool {
	return it.idx >= len(it.list)
}

func (it *piter) at() DocID {
	return it.list[it.idx]
}

type iterator interface {
	at() DocID
	end() bool
	advance(DocID) bool
	next() bool
}

// intersect returns the intersection of two posting lists
// postings are returned deduplicated.
func intersect(result Postings, ait, bit iterator) Postings {

scan:
	for !ait.end() && !bit.end() {

		for ait.at() == bit.at() {

			result = append(result, bit.at())

			var d DocID

			d = ait.at()
			for ait.at() == d {
				if !ait.next() {
					break scan
				}
			}

			d = bit.at()
			for bit.at() == d {
				if !bit.next() {
					break scan
				}
			}
		}

		for ait.at() < bit.at() {
			if !ait.advance(bit.at()) {
				break scan
			}
		}

		for !bit.end() && ait.at() > bit.at() {
			if !bit.advance(ait.at()) {
				break scan
			}
		}
	}

	return result
}
