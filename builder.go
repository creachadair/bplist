// Copyright 2020 Michael J. Fromberger. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bplist

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

// A Builder accumulates values to build a binary property list.  The zero
// value is ready for use.  Add elements and collections to the list with Value
// and Open.  When the property list is complete, use WriteTo to encode it.
type Builder struct {
	stk  []entry
	nobj int
	err  error
}

// NewBuilder constructs a new empty property list builder.
// Add items to the property list using the Value, Open, and Close methods.
func NewBuilder() *Builder { return new(Builder) }

// Err reports the last error that caused an operation on b to fail.  It
// returns nil for a new builder.  Any error causes all subsequent operations
// on the builder to fail with the same error.
func (b *Builder) Err() error { return b.err }

// Reset discards all the data associated with b and restores it to its initial
// state. This also clears any error from a previous failed operation.
func (b *Builder) Reset() { *b = Builder{} }

// WriteTo encodes the property list and writes it in binary form to w.
func (b *Builder) WriteTo(w io.Writer) (int64, error) {
	if b.err != nil {
		return 0, b.err
	} else if len(b.stk) != 1 {
		return 0, b.fail(fmt.Errorf("have %d elements, want 1", len(b.stk)))
	}

	// Encode the variable-size objects.
	e := newEncoder(b.nobj)
	root, err := e.encode(b.stk[0])
	if err != nil {
		return 0, b.fail(err)
	}

	// Write the file header.
	var total int64
	nw, err := io.WriteString(w, "bplist00")
	total += int64(nw)
	if err != nil {
		return total, b.fail(err)
	}
	base := int(total) // start of variable objects

	// Write the encoded objects.
	nc, err := io.Copy(w, e.buf)
	total += nc
	if err != nil {
		return total, b.fail(err)
	}

	// Build the offset table.
	//
	// Each offset in the table must have enough bits to hold the largest
	// possible offset for any object, which is bounded by the offset of the
	// table itself (i.e., the end of the variable objects).
	offStart := total
	offSize := numBytes(uint64(offStart + int64(base)))

	var idx bytes.Buffer
	for i := 0; i < b.nobj; i++ {
		off, ok := e.offset[i]
		if !ok {
			return total, b.fail(fmt.Errorf("object %d missing offset", i))
		}
		writeInt(&idx, offSize, off+base) // shift past header
	}

	// Build the file trailer, a 32-byte index for the rest of the file.  The
	// first word contains the offset and pointer sizes, the rest give the
	// object count, root object pointer, and location of the offset table
	// relative to the start of the file.
	var zbuf [8]byte
	zbuf[6] = byte(offSize)
	zbuf[7] = byte(e.idSize)
	idx.Write(zbuf[:])
	binary.BigEndian.PutUint64(zbuf[:], uint64(b.nobj))
	idx.Write(zbuf[:])
	binary.BigEndian.PutUint64(zbuf[:], uint64(root))
	idx.Write(zbuf[:])
	binary.BigEndian.PutUint64(zbuf[:], uint64(offStart))
	idx.Write(zbuf[:])

	// Copy the offset table and trailer.
	nc, err = io.Copy(w, &idx)
	total += nc
	return int64(total), b.fail(err)
}

// Value adds a single data element to the property list.  It reports an error
// if typ is not a known element type, or if datum is not a valid value for
// that type.
func (b *Builder) Value(typ Type, datum any) error {
	if b.err != nil {
		return b.err
	}
	var ok bool
	switch typ {
	case TNull:
		ok = datum == nil
	case TBool:
		_, ok = datum.(bool)
	case TInteger:
		datum, ok = intValue(datum)
	case TFloat:
		_, ok = datum.(float64)
	case TTime:
		_, ok = datum.(time.Time)
	case TBytes:
		// Allow either a string or a slice for this, but convert the actual
		// value to a string so it can be checked as a map key for deduplication.
		var b []byte
		b, ok = datum.([]byte)
		if ok {
			datum = string(b)
		} else {
			_, ok = datum.(string)
		}
	case TString, TUnicode:
		var r []rune
		r, ok = datum.([]rune)
		if ok {
			datum = string(r)
		} else {
			_, ok = datum.(string)
		}
	case TUID:
		var b []byte
		b, ok = datum.([]byte)
		if ok {
			datum = string(b)
		}
	default:
		return b.fail(fmt.Errorf("unknown element type: %v", typ))
	}
	if !ok {
		return b.fail(fmt.Errorf("invalid datum %T for %v", datum, typ))
	}
	elt := entry{elt: typ, datum: datum}
	b.stk = append(b.stk, elt)
	b.nobj++
	return nil
}

// Open adds a new empty collection of the given type, and calls f to populate
// its contents. When f returns, the collection is automatically closed.  It is
// safe and valid for f to open further nested collections.
//
// For example:
//
//	b.Open(bplist.Array, func(b *bplist.Builder) {
//	  b.Value(bplist.TString, "foo")
//	  b.Value(bplist.TString, "bar")
//	})
func (b *Builder) Open(coll Collection, f func(*Builder)) {
	b.stk = append(b.stk, entry{coll: coll})
	b.nobj++ // +1 for the collection (items are separate)
	defer b.close(coll)
	f(b)
}

// close closes the most recently-opened collection of the given type. It
// reports an error if no collection of that type is open. If coll is a
// dictionary (bplist.Dict) it reports an error if the elements are not
// properly paired (key/value).
func (b *Builder) close(coll Collection) error {
	if b.err != nil {
		return b.err
	}

	// Search back for the nearest open collection of this kind.
	n := len(b.stk) - 1
	for n >= 0 {
		if b.stk[n].coll == coll {
			if !b.stk[n].closed {
				break
			}
		} else if b.stk[n].coll != 0 && !b.stk[n].closed {
			return b.fail(fmt.Errorf("unclosed %v", b.stk[n].coll))
		}
		n--
	}
	if n < 0 {
		return b.fail(fmt.Errorf("close of unopened %v", coll))
	}
	elts := b.stk[n+1:] // everything after the open is now content

	// For dictionaries, contents must be paired (key, value).
	if coll == Dict && len(elts)%2 != 0 {
		return b.fail(errors.New("missing value in dictionary"))
	}

	// Pack the entries into the collection and mark it complete.
	// Note although we have reduced the stack, we do not decrease the object
	// count, since we haven't discarded any.
	b.stk[n].content = elts
	b.stk[n].closed = true
	b.stk = b.stk[:n+1]
	return nil
}

func (b *Builder) fail(err error) error {
	if err != nil {
		b.err = err
	}
	return err
}

func newEncoder(nobj int) *encoder {
	return &encoder{
		idSize: numBytes(uint64(nobj)),
		objref: make(map[string]int),
		offset: make(map[int]int),
		buf:    bytes.NewBuffer(nil),
	}
}

type encoder struct {
	idSize int            // byte count per objid
	nextID int            // next object id
	objref map[string]int // :: key → objid
	offset map[int]int    // :: objid → offset
	buf    *bytes.Buffer
}

func writeInt(w io.Writer, nb, z int) {
	var zbuf [8]byte

	v := uint64(z)
	for i := range nb {
		zbuf[7-i] = byte(v & 255)
		v >>= 8
	}
	w.Write(zbuf[8-nb:])
}

func (e *encoder) encode(elt entry) (int, error) {
	if elt.coll == 0 {
		return e.encodeDatum(elt)
	}
	ids := make([]int, len(elt.content))
	for i, item := range elt.content {
		z, err := e.encode(item)
		if err != nil {
			return 0, err
		}
		ids[i] = z
	}
	return e.encodeCollection(elt, ids)
}

func (e *encoder) encodeDatum(elt entry) (int, error) {
	ck := cacheKey(elt)
	if z, ok := e.objref[ck]; ok {
		return z, nil
	}
	pos := e.buf.Len()
	switch elt.elt {
	case TNull:
		e.buf.WriteByte(0)
	case TBool:
		if elt.datum.(bool) {
			e.buf.WriteByte(8)
		} else {
			e.buf.WriteByte(9)
		}
	case TInteger:
		e.buf.Write(unparseInt(0x10, uint64(elt.datum.(int64))))
	case TFloat:
		e.buf.Write(unparseFloat(elt.datum.(float64)))
	case TTime:
		sec := float64(elt.datum.(time.Time).UTC().Unix() - macEpoch)
		e.buf.WriteByte(0x33)
		var date [8]byte
		binary.BigEndian.PutUint64(date[:], math.Float64bits(sec))
		e.buf.Write(date[:])
	case TBytes:
		writeData(e.buf, 0x40, elt.datum.(string))
	case TString, TUnicode:
		s := elt.datum.(string)
		if isASCII(s) {
			writeData(e.buf, 0x50, s)
		} else if utf8.ValidString(s) {
			writeData(e.buf, 0x70, s)
		} else {
			u16 := utf16.Encode([]rune(s))
			if len(u16) >= 15 {
				e.buf.WriteByte(0x6f)
				e.buf.Write(unparseInt(0x10, uint64(len(u16))))
			} else {
				e.buf.WriteByte(0x60 | byte(len(u16)))
			}
			for _, uc := range u16 {
				v := []byte{byte((uc >> 8) & 0xff), byte(uc & 0xff)}
				e.buf.Write(v)
			}
		}
	default:
		return 0, fmt.Errorf("unexpected entry type: %v", elt.elt)
	}

	ref := e.nextID
	e.nextID++
	e.objref[ck] = ref
	e.offset[ref] = pos
	return ref, nil
}

func (e *encoder) encodeCollection(elt entry, ids []int) (int, error) {
	pos := e.buf.Len()
	nelt := len(ids)

	var tag byte
	switch elt.coll {
	case Array:
		tag = 0xa0
	case Set:
		tag = 0xc0
	case Dict:
		tag = 0xd0
		nelt = len(ids) / 2
	default:
		return 0, fmt.Errorf("invalid collection type: %v", elt.coll)
	}
	if nelt >= 15 {
		e.buf.WriteByte(tag | 0xf)
		e.buf.Write(unparseInt(0x10, uint64(nelt)))
	} else {
		e.buf.WriteByte(tag | byte(nelt))
	}
	if elt.coll == Dict {
		for i := 0; i < len(ids); i += 2 {
			writeInt(e.buf, e.idSize, ids[i]) // keys
		}
		for i := 1; i < len(ids); i += 2 {
			writeInt(e.buf, e.idSize, ids[i]) // values
		}
	} else {
		for _, id := range ids {
			writeInt(e.buf, e.idSize, id)
		}
	}

	ref := e.nextID
	e.nextID++
	e.offset[ref] = pos
	return ref, nil
}

type entry struct {
	coll    Collection // 0 for an element
	elt     Type       // element type; ignored if coll ≠ 0
	datum   any        // nil for a collection
	closed  bool       // collection is complete (content is valid)
	content []entry    // nil for an element
}

// Precondition: e is an element, not a collection.
func cacheKey(e entry) string {
	return fmt.Sprintf("E:%d:%v", e.elt, e.datum)
}

// intValue reports whether v is an integer convertible to int64, and if so
// converts it to one. If not, it returns 0 as the value.
func intValue(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int:
		return int64(t), true
	case int32:
		return int64(t), true
	}
	return 0, false
}

func unparseFloat(f float64) []byte {
	return unparseInt(0x20, math.Float64bits(f))
}

func numBytes(v uint64) int {
	nb := 1
	for s := uint64(256); nb < 8 && s <= v; s *= 256 {
		nb++
	}
	return nb
}

func intSize(v uint64) (nb, p2 int) {
	nb = 1
	for s := uint64(256); nb < 8 && s <= v; s *= s {
		nb *= 2
		p2++
	}
	return nb, p2
}

func unparseInt(tag byte, v uint64) []byte {
	nd, p2 := intSize(v)

	var buf [9]byte
	buf[0] = tag | byte(p2&0xf)
	for i := nd; i > 0; i-- {
		buf[i] = byte(v & 0xff)
		v >>= 8
	}
	return buf[:nd+1]
}

func writeData(buf *bytes.Buffer, tag byte, s string) {
	if len(s) >= 15 {
		buf.WriteByte(tag | 0xf)
		buf.Write(unparseInt(0x10, uint64(len(s))))
	} else {
		buf.WriteByte(tag | byte(len(s)))
	}
	buf.WriteString(s)
}

func isASCII(s string) bool {
	for _, r := range s {
		if r > unicode.MaxASCII {
			return false
		}
	}
	return true
}
