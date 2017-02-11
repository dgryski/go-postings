package postings

import (
	"reflect"
	"testing"
)

func TestIntersect(t *testing.T) {

	a := Postings{1, 1, 2, 4, 6}

	b := Postings{1, 1, 3, 3, 4, 6, 10}

	got := intersect(nil, newIter(a), newIter(b))

	want := Postings{1, 4, 6}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("intersect()=%v, want=%v", got, want)
	}
}

func TestQuery(t *testing.T) {
	idx := NewIndex(nil)
	docs := []DocID{}
	docs = append(docs, idx.AddDocument([]TermID{1, 2, 3}))
	docs = append(docs, idx.AddDocument([]TermID{1, 2, 4}))
	docs = append(docs, idx.AddDocument([]TermID{1, 2, 5}))

	q := []TermID{2}
	want := Postings{0, 1, 2}

	got := Query(idx, q)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Query(%v)=%v, want %v", q, got, want)
	}

}
