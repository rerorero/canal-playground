package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/CanalClient/canal-go/client"
	protocol "github.com/CanalClient/canal-go/protocol"
	"github.com/golang/protobuf/proto"
)

func main() {
	connector := client.NewSimpleCanalConnector("127.0.0.1", 11111, "", "", "example", 60000, 60*60*1000)
	if err := connector.Connect(); err != nil {
		panic(err)
	}
	if err := connector.Subscribe("test\\\\..*"); err != nil {
		panic(err)
	}

	for range time.Tick(500 * time.Millisecond) {
		message, err := connector.Get(100, nil, nil)
		if err != nil {
			panic(err)
		}

		for _, e := range message.Entries {
			switch tpe := e.GetEntryType(); tpe {
			case protocol.EntryType_ENTRYTYPECOMPATIBLEPROTO2:
				println("compatible proto2")
			case protocol.EntryType_TRANSACTIONBEGIN:
				println("begin transaction")
				var p protocol.TransactionBegin
				if err := proto.Unmarshal(e.GetStoreValue(), &p); err != nil {
					panic(err)
				}
				println(p.TransactionId)
				println(propsToStr(p.Props))
			case protocol.EntryType_ROWDATA:
				println("row data")
				var row protocol.RowChange
				if err := proto.Unmarshal(e.GetStoreValue(), &row); err != nil {
					panic(err)
				}
				event(&row, e.Header)
			case protocol.EntryType_TRANSACTIONEND:
				println("end transaction")
				var p protocol.TransactionEnd
				if err := proto.Unmarshal(e.GetStoreValue(), &p); err != nil {
					panic(err)
				}
				println(p.TransactionId)
				println(propsToStr(p.Props))
			case protocol.EntryType_HEARTBEAT:
				println("heartbeat")
			case protocol.EntryType_GTIDLOG:
				println("GTID LOG")
			default:
				println("unknown event: ", tpe)
			}
		}
	}
}

func event(row *protocol.RowChange, header *protocol.Header) {
	println("=== received row change event ===")
	println("  schema: ", header.SchemaName)
	println("  table: ", header.TableName)
	println("  file: ", header.LogfileName)
	println("  pos: ", header.LogfileOffset)
	println("  props: ", propsToStr(header.Props))
	for i, rowData := range row.GetRowDatas() {
		fmt.Printf("[%d]\n  props: %#v\n  before: %#v\n  after: %#v", i,
			propsToStr(rowData.Props), columnsToStr(rowData.BeforeColumns), columnsToStr(rowData.AfterColumns))
	}
}

func propsToStr(p []*protocol.Pair) string {
	var b strings.Builder
	b.WriteString("{")
	for _, pair := range p {
		b.WriteString(pair.Key)
		b.WriteString("=")
		b.WriteString(pair.Value)
		b.WriteString(",")
	}
	b.WriteString("}")
	return b.String()
}
func columnsToStr(c []*protocol.Column) string {
	var b strings.Builder
	b.WriteString("{")
	for _, cl := range c {
		b.WriteString("name=" + cl.Name)
		b.WriteString(",value=" + cl.Value)
		b.WriteString(",type=" + cl.MysqlType)
		b.WriteString(",props=" + propsToStr(cl.Props))
	}
	b.WriteString("}")
	return b.String()
}
