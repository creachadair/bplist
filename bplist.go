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

// Package bplist implements a parser and writer for binary property list files.
package bplist

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"time"
	"unicode/utf16"
)

// References:
//   https://opensource.apple.com/source/CF/CF-550/CFBinaryPList.c

// A Handler provides callbacks to handle objects from a property list.  If a
// handler method reports an error, that error is propagated to the caller.
type Handler interface {
	// Called for the version string, e.g., "00".
	Version(string) error

	// Called for primitive data elements. The concrete type of the datum
	// depends on the Type; see the comments for the Type enumerators.
	Element(typ Type, datum interface{}) error

	// Called to open a new collection of the given type with n elements.
	Open(typ Collection, n int) error

	// Called to close the latest collection of the given type.
	Close(Collection) error
}

// Type enumerates the types of primitive elements in the property list.
type Type int

const (
	// TNull represents the singleton null value. Its datum is nil.
	TNull Type = iota

	// TBool represents a Boolean value. Its datum is a bool.
	TBool

	// TInteger represents an integer value. Its datum is an int64.
	TInteger

	// TFloat represents a floating-point value. Its datum is a float64.
	TFloat

	// TTime represents a timestamp. Its datum is a time.Time in UTC.
	TTime

	// TBytes represents arbitrary bytes. Its datum is a []byte.
	TBytes

	// TString represents a UTF-8 string value. Its datum is a string.
	TString

	// TUnicode represents a UTF-16 string. Its datum is a []rune.
	TUnicode

	// TUID represents a UID value. Its datum is a []byte.
	TUID
)

func (t Type) String() string {
	switch t {
	case TNull:
		return "null"
	case TBool:
		return "bool"
	case TInteger:
		return "int"
	case TFloat:
		return "float"
	case TTime:
		return "time"
	case TBytes:
		return "bytes"
	case TString:
		return "string"
	case TUnicode:
		return "unicode"
	case TUID:
		return "uid"
	}
	return "unknown"
}

// Collection enumerates the types of container elements.
type Collection int

const (
	Array Collection = iota + 1 // an ordered sequence
	Set                         // an unordered group
	Dict                        // a collection of key/value pairs
)

func (c Collection) String() string {
	switch c {
	case Array:
		return "array"
	case Set:
		return "set"
	case Dict:
		return "dict"
	}
	return "unknown"
}

func Parse(data []byte, h Handler) error {
	const magic = "bplist"
	const trailerBytes = 32
	if !bytes.HasPrefix(data, []byte(magic)) {
		return errors.New("invalid magic number")
	} else if len(data) < len(magic)+2+trailerBytes {
		return errors.New("invalid file structure")
	}

	// Call the Version handler eagerly, to give the caller a chance to bail out
	// for an incompatible version before we do more work.
	pos := len(magic)
	if err := h.Version(string(data[pos : pos+2])); err != nil {
		return err
	}

	t := parseTrailer(data[len(data)-32:])
	if t.tableEnd() > len(data)-32 {
		log.Printf("MJF :: len(data)=%d tableEnd=%d", len(data), t.tableEnd())
		return errors.New("invalid offsets table")
	}
	offsets := make([]int, t.NumObjects)
	for i := 0; i < len(offsets); i++ {
		base := t.OffsetTable + t.OffsetBytes*i
		offsets[i] = int(parseInt(data[base : base+t.OffsetBytes]))
	}

	var parseObj func(int) error
	parseObj = func(id int) error {
		off := offsets[id]
		tag := data[off]

		switch sel := tag >> 4; sel {
		case 0: // null, bool, fill
			switch tag & 0xf {
			case 0:
				return h.Element(TNull, nil)
			case 8:
				return h.Element(TBool, false)
			case 9:
				return h.Element(TBool, true)
			}

		case 1: // int
			size := 1 << (tag & 0xf)
			return h.Element(TInteger, parseInt(data[off+1:off+1+size]))

		case 2: // real
			size := 1 << (tag & 0xf)
			return h.Element(TFloat, parseFloat(data[off+1:off+1+size]))

		case 3: // date
			if tag&0xf == 3 {
				const macEpoch = 978307200 // 01-Jan-2001
				sec := parseFloat(data[off+1 : off+9])
				return h.Element(TTime, time.Unix(int64(sec)+macEpoch, 0).In(time.UTC))
			}

		case 4: // data
			size, shift := sizeAndShift(tag, data[off+1:])
			start := off + 1 + shift
			end := start + size
			return h.Element(TBytes, data[start:end])

		case 5, 7: // ASCII or UTF-8 string
			size, shift := sizeAndShift(tag, data[off+1:])
			start := off + 1 + shift
			end := start + size
			return h.Element(TString, string(data[start:end]))

		case 6: // Unicode string
			size, shift := sizeAndShift(tag, data[off+1:])
			start := off + 1 + shift
			runes := make([]uint16, size)
			for i := 0; i < size; i++ {
				runes[i] = binary.BigEndian.Uint16(data[start:])
				start += 2
			}
			return h.Element(TUnicode, utf16.Decode(runes))

		case 8: // UID
			size, shift := sizeAndShift(tag, data[off+1:])
			start := off + 1 + shift
			end := start + size
			return h.Element(TUID, data[start:end])

		case 10, 11, 12: // array or set
			coll := Array
			if sel == 11 || sel == 12 {
				coll = Set
			}
			size, shift := sizeAndShift(tag, data[off+1:])
			if err := h.Open(coll, size); err != nil {
				return err
			}
			start := off + 1 + shift
			for i := 0; i < size; i++ {
				ref := int(parseInt(data[start : start+t.RefBytes]))
				if err := parseObj(ref); err != nil {
					return err
				}
				start += t.RefBytes
			}
			return h.Close(coll)

		case 13: // dict
			size, shift := sizeAndShift(tag, data[off+1:])
			if err := h.Open(Dict, size); err != nil {
				return err
			}
			keyStart := off + 1 + shift
			valStart := keyStart + (size * t.RefBytes)
			for i := 0; i < size; i++ {
				kref := int(parseInt(data[keyStart : keyStart+t.RefBytes]))
				if err := parseObj(kref); err != nil {
					return err
				}
				keyStart += t.RefBytes

				vref := int(parseInt(data[valStart : valStart+t.RefBytes]))
				if err := parseObj(vref); err != nil {
					return err
				}
				valStart += t.RefBytes
			}
			return h.Close(Dict)
		}
		return fmt.Errorf("unrecognized tag %02x", tag)
	}

	return parseObj(t.RootObject)
}

type trailer struct {
	OffsetBytes int
	RefBytes    int
	NumObjects  int
	RootObject  int
	OffsetTable int
}

func (t *trailer) needBytes() int { return t.OffsetBytes * t.NumObjects }
func (t *trailer) tableEnd() int  { return t.OffsetTable + t.needBytes() }

// parseTrailer unpacks the trailer.
// Precondition: len(data) == 32
func parseTrailer(data []byte) *trailer {
	return &trailer{
		OffsetBytes: int(data[6]),
		RefBytes:    int(data[7]),
		NumObjects:  int(binary.BigEndian.Uint64(data[8:])),
		RootObject:  int(binary.BigEndian.Uint64(data[16:])),
		OffsetTable: int(binary.BigEndian.Uint64(data[24:])),
	}
}

func parseInt(data []byte) (v int64) {
	for _, b := range data {
		v = (v << 8) | int64(b)
	}
	return
}

func parseFloat(data []byte) float64 {
	return math.Float64frombits(uint64(parseInt(data)))
}

func sizeAndShift(tag byte, data []byte) (nb, offset int) {
	nb = int(tag & 0xf)
	if nb == 15 {
		size := 1 << int(data[0]&0xf)
		nb = int(parseInt(data[1 : 1+size]))
		offset = 1 + size
	}
	return
}
