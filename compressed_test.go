package postings

import (
	"reflect"
	"testing"
)

func TestCompressedBlock(t *testing.T) {

	docs := []uint32{100, 102, 110, 200, 500, 1000}

	_, cblock := newCompressedBlock(docs)

	t.Logf("% 02x", cblock.groups)

	it := newCompressedIter(cblock)

	var got []uint32

	for ; !it.end(); it.next() {
		got = append(got, uint32(it.at()))
	}

	if !reflect.DeepEqual(docs, got) {
		t.Errorf("roundtrip(%v)=%v failed", docs, got)
	}
}
