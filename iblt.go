package iblt

import (
    "bytes"
    "encoding/binary"
    "errors"
    "github.com/dchest/siphash"
    "github.com/golang-collections/collections/queue"
    "github.com/willf/bitset"
    "math"
)

var DEFAULT_DATA_BYTES = 6
var DEFAULT_HASH_BYTES = 3

type Table struct {
    BktNum  uint
    DataLen int
    HashLen int
    HashNum int
    buckets []*Bucket
    bitsSet *bitset.BitSet
}

func GetIbltParams(numItems uint) IbltParam {
    ibltParam, present := ibltParamMap[numItems]
    if !present {
        ibltParam = IbltParam{NumHashFuncs: 4, ItemOverhead: 1.36}
    }

    return ibltParam
}

func GetCellCount(numItems uint) uint {
    ibltParam := GetIbltParams(numItems)
    numCells := uint(math.Ceil(float64(numItems) * ibltParam.ItemOverhead))

    for uint(ibltParam.NumHashFuncs) * numCells / uint(ibltParam.NumHashFuncs) != numCells {
        numCells += 1
    }

    return numCells
}

func New(numItems uint) *Table {
    ibltParam := GetIbltParams(numItems)
    numCells := GetCellCount(numItems)

    return NewTable(numCells, DEFAULT_DATA_BYTES, DEFAULT_HASH_BYTES, ibltParam.NumHashFuncs)
}

// Specify number of buckets, data field length (in byte), number of hash functions
func NewTable(buckets uint, dataLen int, hashLen int, hashNum int, ) *Table {
    return &Table{
        BktNum:  buckets,
        DataLen: dataLen,
        HashLen: hashLen,
        HashNum: hashNum,
        buckets: make([]*Bucket, buckets),
        bitsSet: bitset.New(buckets),
    }
}

func (t *Table) Insert(d []byte) error {
    if err := t.operate(d, true); err != nil {
        return err
    }

    return nil
}

func (t *Table) Delete(d []byte) error {
    if err := t.operate(d, false); err != nil {
        return err
    }

    return nil
}

func (t *Table) operate(d []byte, sign bool) error {
    cpy := make([]byte, len(d))
    copy(cpy, d)
    err := t.index(cpy)
    if err != nil {
        return err
    }

    for i, e := t.bitsSet.NextSet(0); e; i, e = t.bitsSet.NextSet(i + 1) {
        t.operateBucket(i, cpy, sign)
    }

    return nil
}

func (t *Table) index(d []byte) error {
    if len(d) != t.DataLen {
        return errors.New("insert byte length mismatches base data length")
    }

    if t.bitsSet == nil {
        t.bitsSet = bitset.New(t.BktNum)
    }

    t.bitsSet.ClearAll()
    tries := 1
    for i := 0; i < t.HashNum; {
        // assume we can always find different keys
        // as this is in high probability
        h := siphash.Hash(key0, uint64(key1+tries), d)
        tries++
        // TODO: modulo produces imbalanced uniform distribution
        idx := uint(h) % t.BktNum
        if !t.bitsSet.Test(idx) {
            t.bitsSet.Set(idx)
            i++
        }
    }

    return nil
}

func (t Table) Copy() *Table {
    rtn := NewTable(t.BktNum, t.DataLen, t.HashLen, t.HashNum)
    for i, bkt := range t.buckets {
        if bkt != nil {
            rtn.buckets[i] = bkt.copy()
        }
    }

    return rtn
}

// Modify callee, t = t - a
func (t *Table) Subtract(a *Table) error {
    err := t.check(a)
    if err != nil {
        return err
    }

    for i := range t.buckets {
        if t.buckets[i] != nil && a.buckets[i] != nil {
            t.buckets[i].subtract(a.buckets[i])
        }
        if t.buckets[i] == nil && a.buckets[i] != nil {
            t.buckets[i] = a.buckets[i].copy()
            t.buckets[i].count = -t.buckets[i].count
        }
    }

    return nil
}

// Decode is self-destructive
func (t *Table) Decode() (*Diff, error) {
    diff := NewDiff(t.BktNum)
    if t.empty() {
        return diff, nil
    }

    pure := queue.New()
    err := t.enqueuePure(pure)
    if err != nil {
        return diff, err
    }
    // ensure we have at least one pure bucket in the IBLT
    // this is necessary condition for decoding an IBLT
    if pure.Len() == 0 {
        return diff, errors.New("no pure buckets in table")
    }

    bkt := NewBucket(t.DataLen, t.HashLen)
    for pure.Len() > 0 {
        // clean out pure queue, delete all pure buckets and output the stored data
        // it will create more pure buckets to decode in the next cycle
        for pure.Len() > 0 {
            bkt = pure.Dequeue().(*Bucket)
            if err = diff.encode(bkt); err != nil {
                return diff, nil
            }
            // Insert if count < 0, Delete if count > 0
            if err = t.operate(bkt.dataSum, bkt.count < 0); err != nil {
                return diff, err
            }
        }
        // now pure queue should be empty, enqueue more pure cell
        err = t.enqueuePure(pure)
        if err != nil {
            return diff, err
        }
        // no more bucket is pure either
        // 1) we have successfully decoded all the possible buckets and all the buckets should be empty
        // 2) we have hash collision for more than two items
    }
    // check if every bucket is empty
    if !t.empty() {
        return diff, errors.New("dirty entries remained")
    }

    return diff, nil
}

func (t Table) empty() bool {
    for i := range t.buckets {
        if t.buckets[i] != nil && !t.buckets[i].empty() {
            return false
        }
    }
    return true
}

func (t *Table) enqueuePure(pure *queue.Queue) error {
    // TODO: mark empty bucket and skip early
    pureMask := bitset.New(t.bitsSet.Len())
    for i := range t.buckets {
        // skip the same pure bucket at difference indexes, enqueue the first one
        if t.buckets[i] != nil && !pureMask.Test(uint(i)) && t.buckets[i].pure() {
            if err := t.index(t.buckets[i].dataSum); err != nil {
                return err
            }
            if !t.bitsSet.Test(uint(i)) {
                // current bucket is a false pure
                continue
            }
            pureMask.InPlaceUnion(t.bitsSet)
            pure.Enqueue(t.buckets[i])
        }
    }
    return nil
}

func (t Table) check(a *Table) error {
    if t.BktNum != a.BktNum {
        return errors.New("subtract table mismatches bucket number")
    }

    if t.DataLen != a.DataLen {
        return errors.New("subtract table mismatches data length")
    }

    if t.HashLen != a.HashLen {
        return errors.New("subtract table mismatches hash length")
    }

    if t.HashNum != a.HashNum {
        return errors.New("subtract table mismatches number of hash functions")
    }

    if len(t.buckets) != len(a.buckets) {
        return errors.New("illegally appended buckets")
    }

    return nil
}

func (t *Table) operateBucket(idx uint, d []byte, sign bool) {
    if t.buckets[idx] == nil {
        t.buckets[idx] = NewBucket(t.DataLen, t.HashLen)
    }
    t.buckets[idx].operate(d, sign)
}

func (t Table) Serialize() ([]byte, error) {
    var buffer bytes.Buffer
    twoBytes := make([]byte, 2)

    for _, unsigned := range []uint16{uint16(t.BktNum), uint16(t.DataLen), uint16(t.HashLen), uint16(t.HashNum),} {
        binary.BigEndian.PutUint16(twoBytes, uint16(unsigned))
        buffer.Write(twoBytes)
    }

    for idx, bkt := range t.buckets {
        if bkt != nil && !bkt.empty() {
            binary.BigEndian.PutUint16(twoBytes, uint16(idx))
            buffer.Write(twoBytes)
            binary.BigEndian.PutUint16(twoBytes, uint16(bkt.count))
            buffer.Write(twoBytes)

            buffer.Write(bkt.dataSum)
            buffer.Write(bkt.hashSum)
        }
    }
    return buffer.Bytes(), nil
}

func Deserialize(b []byte) (*Table, error) {
    reader := bytes.NewBuffer(b)

    bktNum := uint(binary.BigEndian.Uint16(reader.Next(2)))
    dataLen := int(binary.BigEndian.Uint16(reader.Next(2)))
    hashLen := int(binary.BigEndian.Uint16(reader.Next(2)))
    hashNum := int(binary.BigEndian.Uint16(reader.Next(2)))

    table := NewTable(bktNum, dataLen, hashLen, hashNum)
    for next := reader.Next(2); len(next) != 0; next = reader.Next(2) {
        idx := binary.BigEndian.Uint16(next)
        table.buckets[idx] = NewBucket(dataLen, hashLen)
        table.buckets[idx].count = int(int16(binary.BigEndian.Uint16(reader.Next(2))))
        copy(table.buckets[idx].dataSum, reader.Next(dataLen))
        copy(table.buckets[idx].hashSum, reader.Next(hashLen))
    }

    return table, nil
}
