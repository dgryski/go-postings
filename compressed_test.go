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
