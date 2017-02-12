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

	tests := []struct {
		q    []TermID
		want Postings
	}{
		{[]TermID{2}, Postings{0, 1, 2}},
		{[]TermID{3}, Postings{0}},
		{[]TermID{1, 4}, Postings{1}},
	}

	for _, tt := range tests {
		got := Query(idx, tt.q)
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("Query(%v)=%v, want %v", tt.q, got, tt.want)
		}
	}

}
