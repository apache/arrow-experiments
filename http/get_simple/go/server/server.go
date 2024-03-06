// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

var schema = arrow.NewSchema([]arrow.Field{
	{Name: "a", Type: arrow.PrimitiveTypes.Int64},
	{Name: "b", Type: arrow.PrimitiveTypes.Int64},
	{Name: "c", Type: arrow.PrimitiveTypes.Int64},
	{Name: "d", Type: arrow.PrimitiveTypes.Int64},
}, nil)

func GetPutData() []arrow.Record {
	const (
		totalRecords = 100000000
		length       = 4096
		ncolumns     = 4
		seed         = 42
	)

	var (
		r    = rand.New(rand.NewSource(seed))
		mem  = memory.DefaultAllocator
		arrs = make([]arrow.Array, 0, ncolumns)
	)
	for i := 0; i < ncolumns; i++ {
		buf := memory.NewResizableBuffer(mem)
		buf.Resize(length * 8)
		_, err := r.Read(buf.Buf())
		if err != nil {
			panic(err)
		}
		defer buf.Release()

		data := array.NewData(arrow.PrimitiveTypes.Int64, length, []*memory.Buffer{nil, buf}, nil, 0, 0)
		defer data.Release()
		a := array.NewInt64Data(data)
		defer a.Release()
		arrs = append(arrs, a)
	}

	batch := array.NewRecord(schema, arrs, length)
	defer batch.Release()

	batches := make([]arrow.Record, 0)
	records := int64(0)
	for records < totalRecords {
		if records+length > totalRecords {
			lastLen := totalRecords - records
			batches = append(batches, batch.NewSlice(0, lastLen))
			records += lastLen
		} else {
			batch.Retain()
			batches = append(batches, batch)
			records += length
		}
	}

	return batches
}

func main() {
	batches := GetPutData()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		hdrs := w.Header()

		// set these headers if testing with a local browser-based client:

		//hdrs.Add("access-control-allow-origin", "http://localhost:8000")
		//hdrs.Add("access-control-allow-methods", "GET")
		//hdrs.Add("access-control-allow-headers", "content-type")

		hdrs.Add("content-type", "application/vnd.apache.arrow.stream")
		w.WriteHeader(http.StatusOK)

		wr := ipc.NewWriter(w, ipc.WithSchema(batches[0].Schema()))
		defer wr.Close()

		for _, b := range batches {
			if err := wr.Write(b); err != nil {
				panic(err)
			}
		}
	})

	fmt.Println("Serving on localhost:8000...")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
