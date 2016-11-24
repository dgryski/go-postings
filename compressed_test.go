package postings

import (
	"reflect"
	"testing"
)

func TestCompressedBlock(t *testing.T) {

	docs := []Posting{100, 102, 110, 200, 500, 1000}

	_, cblock := newCompressedBlock(docs)

	t.Logf("% 02x", cblock.groups)

	it := newCompressedIter(cblock)

	var got []Posting

	for ; !it.end(); it.next() {
		got = append(got, Posting(it.at()))
	}

	if !reflect.DeepEqual(docs, got) {
		t.Errorf("roundtrip(%v)=%v failed", docs, got)
	}
}
