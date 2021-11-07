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

package bplist_test

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/creachadair/bplist"
)

var testFile = flag.String("input", "", "Manual test input file")

// This test input is from a standard Apple cookie file.
const testInput = "bplist00\xd1\x01\x02_\x10\x18NSHTTPCookieAcceptPolicy\x10" +
	"\x02\x08\x0b&\x00\x00\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00" +
	"\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00("

func TestManual(t *testing.T) {
	if *testFile == "" {
		t.Skip("Skipping because no -input file is given")
	}
	data, err := os.ReadFile(*testFile)
	if err != nil {
		t.Fatalf("Reading input: %v", err)
	}
	if err := bplist.Parse(data, testHandler{
		log: t.Logf,
		buf: io.Discard,
	}); err != nil {
		t.Errorf("Parse failed: %v", err)
	}
}

func TestBasic(t *testing.T) {
	var buf bytes.Buffer
	if err := bplist.Parse([]byte(testInput), testHandler{
		log: t.Logf,
		buf: &buf,
	}); err != nil {
		t.Errorf("Parse failed; %v", err)
	}
	const want = `V"00"<dict size=1>(string=NSHTTPCookieAcceptPolicy)(int=2)</dict>`
	if got := buf.String(); got != want {
		t.Errorf("Parse result: got %s, want %s", got, want)
	}
}

func TestBuilder(t *testing.T) {
	b := newTestBuilder(t)

	// Assemble a property list equivalent to the testInput, and verify that it
	// round-trips correctly through the parser.
	b.Open(bplist.Dict)
	b.mustElement(bplist.TString, "NSHTTPCookieAcceptPolicy")
	b.mustElement(bplist.TInteger, 2)
	b.mustClose(bplist.Dict)

	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf); err != nil {
		t.Fatalf("Encoding WriteTo failed: %v", err)
	}

	input := buf.String()
	buf.Reset()

	if err := bplist.Parse([]byte(input), testHandler{
		log: t.Logf,
		buf: &buf,
	}); err != nil {
		t.Errorf("Parse failed; %v", err)
	}
	const want = `V"00"<dict size=1>(string=NSHTTPCookieAcceptPolicy)(int=2)</dict>`
	if got := buf.String(); got != want {
		t.Errorf("Parse result: got %s, want %s", got, want)
	}
}

func TestBuilderErrors(t *testing.T) {
	b := bplist.NewBuilder()
	if err := b.Err(); err != nil {
		t.Errorf("Err on new builder: got %v, want nil", err)
	}

	t.Run("UnclosedArray", func(t *testing.T) {
		err := b.Close(bplist.Array)
		cerr := b.Err()
		if err == nil {
			t.Error("Close: got nil, wanted an error")
		}
		if err != cerr {
			t.Errorf("Close/Err: got %v, want %v", cerr, err)
		}
	})

	t.Run("ResetClearsErr", func(t *testing.T) {
		b.Reset()
		if err := b.Err(); err != nil {
			t.Errorf("After reset: got err %v", err)
		}
	})

	t.Run("BadValue", func(t *testing.T) {
		err := b.Element(bplist.TString, 101)
		if err == nil {
			t.Error("Element: got nil, wanted an error")
		}
	})
}

type testHandler struct {
	log func(string, ...interface{})
	buf io.Writer
}

func (h testHandler) Version(s string) error {
	h.log("Version %q", s)
	fmt.Fprintf(h.buf, "V%q", s)
	return nil
}

func (h testHandler) Element(elt bplist.Type, datum interface{}) error {
	h.log("Element %v %v", elt, datum)
	if b, ok := datum.([]byte); ok {
		fmt.Fprintf(h.buf, "(%s=%d bytes)", elt, len(b))
	} else {
		fmt.Fprintf(h.buf, "(%s=%v)", elt, datum)
	}
	return nil
}

func (h testHandler) Open(coll bplist.Collection, n int) error {
	h.log("Open %v %d", coll, n)
	fmt.Fprintf(h.buf, "<%s size=%d>", coll, n)
	return nil
}

func (h testHandler) Close(coll bplist.Collection) error {
	h.log("Close %v", coll)
	fmt.Fprintf(h.buf, "</%s>", coll)
	return nil
}

type testBuilder struct {
	t *testing.T
	*bplist.Builder
}

func newTestBuilder(t *testing.T) *testBuilder {
	return &testBuilder{t: t, Builder: bplist.NewBuilder()}
}

func (b *testBuilder) mustElement(typ bplist.Type, datum interface{}) {
	b.t.Helper()
	if err := b.Element(typ, datum); err != nil {
		b.t.Fatalf("Element %v %v: unexpected error: %v", typ, datum, err)
	}
}

func (b *testBuilder) mustClose(coll bplist.Collection) {
	b.t.Helper()
	if err := b.Close(coll); err != nil {
		b.t.Fatalf("Close %v: unexpected error: %v", coll, err)
	}
}
