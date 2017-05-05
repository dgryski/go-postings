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

func (idx *Index) Postings(t TermID) (Iterator, int) {
	p := idx.p[t]
	return newIter(p), len(p)
}

type tfList struct {
	terms []TermID
	freq  []int
	iters []Iterator
}

func (tf tfList) Len() int { return len(tf.terms) }
func (tf tfList) Swap(i, j int) {
	tf.terms[i], tf.terms[j] = tf.terms[j], tf.terms[i]
	tf.freq[i], tf.freq[j] = tf.freq[j], tf.freq[i]
	tf.iters[i], tf.iters[j] = tf.iters[j], tf.iters[i]
}
func (tf tfList) Less(i, j int) bool { return tf.freq[i] < tf.freq[j] }

type InvertedIndex interface {
	Postings(t TermID) (Iterator, int)
}

// Query returns a list of postings that match the terms
func Query(idx InvertedIndex, ts []TermID) Postings {

	if len(ts) == 1 {
		iter, f := idx.Postings(ts[0])
		result := make(Postings, 0, f)

		for !iter.end() {
			result = append(result, iter.at())
			iter.next()
		}
		return result
	}

	freq := make([]int, len(ts))
	terms := make([]TermID, len(ts))
	iters := make([]Iterator, len(ts))
	for i, t := range ts {
		d, f := idx.Postings(t)
		if d.end() {
			return nil
		}
		terms[i] = t
		freq[i] = f
		iters[i] = d
	}

	tf := tfList{terms, freq, iters}

	sort.Sort(tf)

	var docs Iterator = iters[0]

	result := make(Postings, freq[0])

	for _, t := range iters[1:] {
		result = intersect(result[:0], docs, t)
		if len(result) == 0 {
			return nil
		}
		docs = newIter(result)
	}

	return result
}

//  Postings is a list of documents
type Postings []DocID

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

	it.idx = low

	return !it.end()
}

func (it *piter) end() bool {
	return it.idx >= len(it.list)
}

func (it *piter) at() DocID {
	return it.list[it.idx]
}

type Iterator interface {
	at() DocID
	end() bool
	advance(DocID) bool
	next() bool
}

// intersect returns the intersection of two posting lists
// postings are returned deduplicated.
func intersect(result Postings, ait, bit Iterator) Postings {

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
