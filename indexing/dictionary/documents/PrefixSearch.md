# Design of searching for strings that start with a prefix

The design is based on primary index (see `primindex pacakge`) functionality. The index is used to sort strings in the dictionary based on their first L characters. This is done by introducing PrefixKeyCodec described below.

## PrefixKeyCodec and primary index entry for prefix search

``` Go

// PrefixKey represents a fixed-size sortable key used in the primary index.
// Prefix is exactly L bytes long (padded with 0x00 for strings shorter than L).
type PrefixKey struct {
    Prefix []byte // length = L
    dictID uint32
}

type PrefixKeyCodec struct {
    prefixLength int
}

// Serialize encodes the key into a fixed-size byte slice:
// [ Prefix(L bytes) | dictID (4 bytes, big-endian) ]
func (c PrefixKeyCodec) Serialize(key PrefixKey) ([]byte, error) {
    if len(key.Prefix) != c.prefixLength {
        return nil, errors.New("PrefixKyeCodec: invalid prefix length")
    }
    out := make([]byte, len(c.prefixLength)+4)
    copy(out, key.Prefix)
    binary.BigEndian.PutUint32(out[len(key.Prefix):], key.dictID)
    return out
}

// DeserializePrefixKeyCodec decodes a PrefixKeyCodec from a byte slice.
func (c PrefixKeyCodec) Deserialize(buf []byte) PrefixKey {
    prefix := make([]byte, c.prefixLength)
    copy(prefix, buf[:c.prefixLength])
    dictID := binary.BigEndian.Uint32(buf[c.prefixLength:])
    return PrefixKey{Prefix: prefix, dictID: dictID}
}

// Compare implements lexicographic ordering of PrefixKey keys. This is correct
// for prefix-search indexes where ordering beyond the prefix is not important.
// Hence, final ordering for prefix keys is not the same as lexical ordering of their
// respective strings.
func (c PrefixKeyCodec) Compare(a, b PrefixKey) int {
    if comp := bytes.Compare(a.Prefix, b.Prefix); comp != 0 {
        return comp
    }
    // Tie-breaker: arbitrary but stable ordering
    switch {
    case a.dictID < b.dictID:
        return -1
    case a.dictID > b.dictID:
        return 1
    default:
        return 0
    }
}

// Size returns the fixed serialized length of the key.
func (c PrefixKeyCodec) Size() int {
    return c.prefixLength + 4
}

// BuildPrefixKey constructs a PrefixKey from a string and dictID.
// The prefix is the first L bytes of the UTF-8 string, padded with 0x00.
func BuildPrefixKey(s string, dictID uint32, L int) PrefixKey {
    prefix := make([]byte, L)
    copy(prefix, []byte(s)) // UTF-8 safe: truncates at byte boundary
    return PrefixKey{Prefix: prefix, dictID: dictID}
}

// PrefixUpperBound computes the smallest key strictly greater than all keys
// beginning with the given prefix. Used for prefix-range queries.
func PrefixUpperBound(prefix string, L int) PrefixKey {
    buf := make([]byte, L)
    p := []byte(prefix)
    copy(buf, p)

    // Increment last non-0xFF byte
    for i := len(p) - 1; i >= 0; i-- {
        if buf[i] != 0xFF {
            buf[i]++
            for j := i + 1; j < L; j++ {
                buf[j] = 0x00
            }
            return PrefixKey{Prefix: buf, dictID: math.MaxUint32}
        }
    }

    // Overflow: prefix is all 0xFF or empty
    for i := range buf {
        buf[i] = 0xFF
    }
    return PrefixKey{Prefix: buf, dictID: math.MaxUint32}
}
```

The primindex entry is defined as

``` go
// IndexEntry represents a single key-value entry in a leaf node
type IndexEntry struct {
	Key   interface{} // actual key value (will be serialized using KeyCodec)
	Value []byte      // value data (fixed size defined in header)
}
```

where the Key is deserialized value of the keys codec, in our case the PrefixKey, and the Value is a byte array interpreted by the application. In the case of prefix search, the key itself contains the value we need - the dictID. Hence, we don't need a value, so for simplicity we can use empty 2 bytes and ignore them.

The prefix primary index leaf nodes will contain dictionary IDs in the order of their prefix lexical sequence. All IDs of strings starting with the same prefix will be clustered together, but they will not be in the lexical sequence of their strings. That does not matter in the case of prefix-based search, because we only care about finding all strings starting with a prefix, but not their order.

## How a B‑tree‑style index performs a prefix range search
Assume your primary index is a B‑tree or B+‑tree variant (most on‑disk indexes are B+‑trees).
1. Compute lowerKey and upperKey
You already know this part:
- lowerKey = (prefix padded to L, dictID=0)
- upperKey = PrefixUpperBound(prefix, L)
These define the exact range of keys that could match.

2. Descend the tree to find the first matching leaf node
This is a normal B‑tree search:
- Start at root
- At each internal node, binary‑search the keys
- Follow the child pointer that leads to the first key ≥ lowerKey
- Continue until you reach a leaf node
This gives you the starting leaf.

3. Scan the leaf node for the first key
Inside the leaf:
- Binary‑search the leaf’s keys to find the first key ≥ lowerKey
- Start iterating forward
For each key:
if key < upperKey:
    yield key
else:
    stop

4. Traverse the leaf node and collect the keys smaller than upperKey. If you reach the end of the leaf, follow the leaf‑chain pointer leaf.next → nextLeaf
- Load next leaf block
- Iterate its keys

5. Stop when the upper bound is reached
The moment you see: key >= upperKey

The collected keys have dictIDs of the strings that start with the given prefix p. Below is a sudo-code of the described algorithm, where the index.Range() function is the above steps 2, 3 and 4.

``` go
function prefixSearch(prefix p):

    lowerKey = buildLowerBoundKey(p)
    upperKey = buildUpperBoundKey(p)

    it = index.Range(lowerKey, upperKey)

    results = empty postings accumulator

    for each entry in it:
        dictID = entry.dictID

        (offset, length) = offsets[dictID]
        s = readString(offset, length)

        if s starts with p:
            postings = loadPostings(dictID)
            results = union(results, postings)

    return results
```
