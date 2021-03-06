package iblt

import (
    "bytes"
    "encoding/binary"
    "errors"
    "fmt"
    "github.com/dchest/siphash"
    "github.com/seiflotfy/cuckoofilter"
)

const (
    key0 = 465
    key1 = 629
)

func sipHash(b []byte) []byte {
    // TODO: key constants
    h := siphash.Hash(key0, key1, b)
    rtn := make([]byte, 8)
    binary.BigEndian.PutUint64(rtn, h)
    return rtn
}

// bounds check before calling, len(dst) <= len(src)
func xor(dst []byte, src []byte) {
    for i, v := range dst {
        dst[i] = v ^ src[i]
    }
}

func empty(b []byte) bool {
    for _, v := range b {
        if v != byte(0) {
            return false
        }
    }
    return true
}

// bounds check before calling, len(a) <= len(b)
func equalPrefix(a, b []byte) bool {
    for i, v := range a {
        if v != b[i] {
            return false
        }
    }
    return true
}

type Bucket struct {
    dataSum []byte
    hashSum []byte
    count   int
}

func NewBucket(dataLen, hashLen int) *Bucket {
    return &Bucket{
        dataSum: make([]byte, dataLen),
        hashSum: make([]byte, hashLen),
        count:   0,
    }
}

func (b *Bucket) xor(a *Bucket) {
    xor(b.dataSum, a.dataSum)
    xor(b.hashSum, a.hashSum)
}

func (b *Bucket) subtract(a *Bucket) {
    b.xor(a)
    b.count = b.count - a.count
}

func (b *Bucket) operate(d []byte, sign bool) {
    xor(b.dataSum, d)
    h := sipHash(d)
    xor(b.hashSum, h)
    if sign {
        b.count++
    } else {
        b.count--
    }
}

func (b Bucket) copy() *Bucket {
    bkt := NewBucket(len(b.dataSum), len(b.hashSum))
    copy(bkt.dataSum, b.dataSum)
    bkt.hashSum = b.hashSum
    bkt.count = b.count
    return bkt
}

func (b Bucket) pure() bool {
    if b.count == 1 || b.count == -1 {
        h := sipHash(b.dataSum)
        if equalPrefix(b.hashSum, h) {
            return true
        }
    }
    return false
}

func (b Bucket) empty() bool {
    return b.count == 0 &&
        empty(b.hashSum) &&
        empty(b.dataSum)
}

func (b Bucket) String() string {
    return fmt.Sprintf("Bucket: dataSum: %v, hashSum: %v, count: %d",
        b.dataSum, b.hashSum, b.count)
}

type byteSet struct {
    Set    [][]byte
    filter *cuckoo.Filter
}

func (s byteSet) slice() [][]byte {
    return s.Set
}

func newByteSet(cap uint) *byteSet {
    return &byteSet{
        Set:    make([][]byte, 0),
        filter: cuckoo.NewFilter(cap),
    }
}

func (s byteSet) len() int {
    return len(s.Set)
}

func (s *byteSet) insert(b []byte) {
    if !s.test(b) {
        s.filter.Insert(b)
        s.Set = append(s.Set, b)
    }
}

func (s byteSet) test(b []byte) bool {
    if s.filter.Lookup(b) {
        // possibly in set, false positive
        for _, ele := range s.Set {
            if bytes.Equal(b, ele) {
                return true
            }
        }
    }
    return false
}

func (s *byteSet) delete(b []byte) {
    s.filter.Delete(b)
    idx := 0
    for i, ele := range s.Set {
        if bytes.Equal(b, ele) {
            idx = i
            break
        }
    }
    s.Set = append(s.Set[:idx], s.Set[idx+1:]...)
}

// each part of symmetric difference
type Diff struct {
    Alpha *byteSet
    Beta  *byteSet
}

// bktNum as a good estimation for cuckoo filter capacity
func NewDiff(bktNum uint) *Diff {
    return &Diff{
        Alpha: newByteSet(bktNum),
        Beta:  newByteSet(bktNum),
    }
}

func (d Diff) AlphaSlice() [][]byte {
    return d.Alpha.slice()
}

func (d Diff) BetaSlice() [][]byte {
    return d.Beta.slice()
}

// assume b is pure bucket
func (d *Diff) encode(b *Bucket) error {
    cpy := make([]byte, len(b.dataSum))
    copy(cpy, b.dataSum)
    if b.count == 1 {
        if d.Beta.test(cpy) {
            d.Beta.delete(cpy)
            return errors.New("repetitive bytes found in beta")
        }
        d.Alpha.insert(cpy)
    }
    if b.count == -1 {
        if d.Alpha.test(cpy) {
            d.Alpha.delete(cpy)
            return errors.New("repetitive bytes found in alpha")
        }
        d.Beta.insert(cpy)
    }
    return nil
}

func (d Diff) AlphaLen() int {
    return d.Alpha.len()
}

func (d Diff) BetaLen() int {
    return d.Beta.len()
}
