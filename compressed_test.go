package postings

import (
	"reflect"
	"testing"
)

func TestCompressedBlock(t *testing.T) {

	docs := Postings{100, 102, 110, 200, 500, 1000}

	_, cblock := newCompressedBlock(docs)

	t.Logf("% 02x", cblock.groups)

	it := newCompressedIter(cblock)

	var got Postings

	for ; !it.end(); it.next() {
		got = append(got, it.at())
	}

	if !reflect.DeepEqual(docs, got) {
		t.Errorf("roundtrip(%v)=%v failed", docs, got)
	}
}

var _ iterator = (*citer)(nil)

func TestCompressedIntersect(t *testing.T) {

	a := Postings{1, 1, 2, 4, 6}

	b := Postings{1, 1, 3, 3, 4, 6, 10}

	_, za := newCompressedBlock(a)
	_, zb := newCompressedBlock(b)

	got := intersect(nil, newCompressedIter(za), newCompressedIter(zb))

	want := Postings{1, 4, 6}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("intersect()=%v, want=%v", got, want)
	}
}
