// Copyright 2025 Redpanda Data, Inc.

package pure_test

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/internal/component/scanner/testutil"
	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestCSVScannerDefault(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  csv: {}
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`a,b,c
a1,b1,c1
a2,b2,c2
a3,b3,c3
a4,b4,c4
`),
		`{"a":"a1","b":"b1","c":"c1"}`,
		`{"a":"a2","b":"b2","c":"c2"}`,
		`{"a":"a3","b":"b3","c":"c3"}`,
		`{"a":"a4","b":"b4","c":"c4"}`,
	)
}

type csvErrorReader struct{ err error }

func (r csvErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestCSVScannerStreamReadError(t *testing.T) {
	for _, readErr := range []error{io.ErrUnexpectedEOF, errors.New("broken stream")} {
		for _, partialRow := range []string{"", "a2", `"a2`} {
			for _, continueOnError := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/partial=%q/continue=%t", readErr, partialRow, continueOnError), func(t *testing.T) {
					ctx := context.Background()
					confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
					conf, err := confSpec.ParseYAML(fmt.Sprintf("test:\n  csv:\n    custom_delimiter: '|'\n    parse_header_row: true\n    lazy_quotes: true\n    continue_on_error: %t\n", continueOnError), nil)
					require.NoError(t, err)
					creator, err := conf.FieldScanner("test")
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, creator.Close(ctx)) })

					var sourceAcks []error
					input := io.NopCloser(io.MultiReader(strings.NewReader("a|b\na1|b1\n"+partialRow), csvErrorReader{err: readErr}))
					scanner, err := creator.Create(input, func(_ context.Context, err error) error {
						sourceAcks = append(sourceAcks, err)
						return nil
					}, nil)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, scanner.Close(ctx)) })

					batch, firstAck, err := scanner.NextBatch(ctx)
					require.NoError(t, err)
					require.Len(t, batch, 1)
					data, err := batch[0].AsBytes()
					require.NoError(t, err)
					require.JSONEq(t, `{"a":"a1","b":"b1"}`, string(data))
					require.Empty(t, sourceAcks)

					batch, ack, err := scanner.NextBatch(ctx)
					require.ErrorIs(t, err, readErr)
					require.Empty(t, batch)
					require.Nil(t, ack)
					require.Len(t, sourceAcks, 1)
					require.ErrorIs(t, sourceAcks[0], readErr)

					require.NoError(t, firstAck(ctx, nil))
					require.NoError(t, scanner.Close(ctx))
					require.Len(t, sourceAcks, 1)
				})
			}
		}
	}
}

func TestCSVScannerParseError(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  string
		err  error
	}{
		{"field count", "a2\n", csv.ErrFieldCount},
		{"bare quote", "a\"2,b2\n", csv.ErrBareQuote},
		{"invalid quote", "\"a2\"x,b2\n", csv.ErrQuote},
	} {
		for _, continueOnError := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/continue=%t", tc.name, continueOnError), func(t *testing.T) {
				ctx := context.Background()
				confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
				conf, err := confSpec.ParseYAML(fmt.Sprintf("test:\n  csv:\n    continue_on_error: %t\n", continueOnError), nil)
				require.NoError(t, err)
				creator, err := conf.FieldScanner("test")
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, creator.Close(ctx)) })

				var sourceAcks []error
				scanner, err := creator.Create(io.NopCloser(strings.NewReader("a,b\n"+tc.row+"a3,b3\n")), func(_ context.Context, err error) error {
					sourceAcks = append(sourceAcks, err)
					return nil
				}, nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, scanner.Close(ctx)) })

				batch, ack, err := scanner.NextBatch(ctx)
				if !continueOnError {
					require.ErrorIs(t, err, tc.err)
					require.Empty(t, batch)
					require.Nil(t, ack)
					require.Len(t, sourceAcks, 1)
					require.ErrorIs(t, sourceAcks[0], tc.err)
					return
				}
				require.NoError(t, err)
				require.Len(t, batch, 1)
				require.ErrorIs(t, batch[0].GetError(), tc.err)
				require.NoError(t, ack(ctx, nil))
				require.Empty(t, sourceAcks)

				batch, ack, err = scanner.NextBatch(ctx)
				require.NoError(t, err)
				require.Len(t, batch, 1)
				require.NoError(t, batch[0].GetError())
				data, err := batch[0].AsBytes()
				require.NoError(t, err)
				require.JSONEq(t, `{"a":"a3","b":"b3"}`, string(data))
				require.NoError(t, ack(ctx, nil))

				batch, ack, err = scanner.NextBatch(ctx)
				require.ErrorIs(t, err, io.EOF)
				require.Empty(t, batch)
				require.Nil(t, ack)
				require.NoError(t, scanner.Close(ctx))
				require.Equal(t, []error{nil}, sourceAcks)
			})
		}
	}
}

func TestCSVScannerCustomDelim(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  csv:
    custom_delimiter: '|'
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`a|b|c
a1|b1|c1
a2|b2|c2
a3|b3|c3
a4|b4|c4
`),
		`{"a":"a1","b":"b1","c":"c1"}`,
		`{"a":"a2","b":"b2","c":"c2"}`,
		`{"a":"a3","b":"b3","c":"c3"}`,
		`{"a":"a4","b":"b4","c":"c4"}`,
	)
}

func TestCSVScannerNoHeaderRow(t *testing.T) {
	confSpec := service.NewConfigSpec().Field(service.NewScannerField("test"))
	pConf, err := confSpec.ParseYAML(`
test:
  csv:
    parse_header_row: false
`, nil)
	require.NoError(t, err)

	rdr, err := pConf.FieldScanner("test")
	require.NoError(t, err)

	testutil.ScannerTestSuite(t, rdr, nil, []byte(`a1,b1,c1
a2,b2,c2
a3,b3,c3
a4,b4,c4
`),
		`["a1","b1","c1"]`,
		`["a2","b2","c2"]`,
		`["a3","b3","c3"]`,
		`["a4","b4","c4"]`,
	)
}
