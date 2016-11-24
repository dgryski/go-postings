package postings

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func TestCompressedBlock(t *testing.T) {

	docs := Postings{100, 102, 110, 200, 500, 1000}

	_, cblock := newCompressedBlock(docs)

	t.Logf("% 02x", cblock.groups)

	it := newCBlockIter(cblock)

	var got Postings

	for ; !it.end(); it.next() {
		got = append(got, it.at())
	}

	if !reflect.DeepEqual(docs, got) {
		t.Errorf("roundtrip(%v)=%v failed", docs, got)
	}
}

var _ Iterator = (*cblockiter)(nil)
var _ Iterator = (*cpiter)(nil)

func TestCompressedIntersect(t *testing.T) {

	a := Postings{1, 1, 2, 4, 6}

	b := Postings{1, 1, 3, 3, 4, 6, 10}

	_, za := newCompressedBlock(a)
	_, zb := newCompressedBlock(b)

	got := intersect(nil, newCBlockIter(za), newCBlockIter(zb))

	want := Postings{1, 4, 6}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("intersect()=%v, want=%v", got, want)
	}
}

func makeInput(n int) Postings {
	rand.Seed(0)

	var input Postings

	var docid DocID

	for i := 0; i < n; i++ {

		for j := 0; j < 4; j++ {

			b := uint32(rand.Int31())

			size := nlz(b)

			delta := DocID(1)

			switch size {
			// case 0: none, because b > 0
			case 1:
				delta = DocID(rand.Intn(1 << 8))
			case 2:
				delta = 1<<8 + DocID(rand.Intn((1<<16)-(1<<8)))
			case 3:
				// delta = 1<<16 + DocID(rand.Intn((1<<24)-(1<<16)))
			default:
				// delta = 1<<24 + DocID(rand.Intn((1<<32)-(1<<24)))
			}

			docid += delta

			input = append(input, docid)
		}
	}

	return input
}

func TestCompressedPosting(t *testing.T) {

	sizes := []int{128, 256, 512, 1024, 2048, 4096, 8192}

	for _, s := range sizes {
		p := makeInput(s)

		cp := newCompressedPostings(p)

		t.Logf("size=%d len(cp)=%d", s, len(cp))

		if err := compareIterators(t.Logf, newIter(p), newCompressedIter(cp)); err != nil {
			t.Fatalf("size: %d: err=%v", s, err)
		}
	}
}

func compareIterators(printf func(string, ...interface{}), ait, bit Iterator) error {

	for !ait.end() && !bit.end() {

		a := ait.at()
		b := bit.at()

		if a != b {
			return fmt.Errorf("mismatch: got=%d want=%d", a, b)
		}

		ait.next()
		bit.next()
	}

	if ait.end() != bit.end() {
		return fmt.Errorf("end length mismatch: a=%v b=%v", ait.end(), bit.end())
	}

	return nil
}
