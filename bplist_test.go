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
	"io/ioutil"
	"testing"

	"github.com/creachadair/bplist"
)

var testFile = flag.String("input", "", "Manual test input file")

func TestManual(t *testing.T) {
	if *testFile == "" {
		t.Skip("Skipping because no -input file is given")
	}
	data, err := ioutil.ReadFile(*testFile)
	if err != nil {
		t.Fatalf("Reading input: %v", err)
	}
	if err := bplist.Parse(data, testHandler{
		log: t.Logf,
		buf: ioutil.Discard,
	}); err != nil {
		t.Errorf("Parse failed: %v", err)
	}
}

func TestBasic(t *testing.T) {
	const testInput = "bplist00\xd1\x01\x02_\x10\x18NSHTTPCookieAcceptPolicy\x10" +
		"\x02\x08\x0b&\x00\x00\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00" +
		"\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00("

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
