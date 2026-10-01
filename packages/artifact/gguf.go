package artifact

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// GGUF is what NEBULA reads from a GGUF file's header: enough to fill in and check
// the registry's fields, and nothing it would have to trust blindly.
//
// Format reference: the GGUF specification in ggml's repository (docs/gguf.md).
// Header: magic "GGUF", uint32 version, uint64 tensor count, uint64 key/value count,
// then the key/value pairs. Every read is bounded, because this parses bytes a
// client uploaded.
type GGUF struct {
	Version       uint32  `json:"version"`
	TensorCount   uint64  `json:"tensor_count"`
	Architecture  string  `json:"architecture,omitempty"`
	Name          string  `json:"name,omitempty"`
	ContextLength uint64  `json:"context_length,omitempty"`
	FileType      *uint32 `json:"file_type,omitempty"`
	// Quantization is FileType's name ("Q4_K_M"), when the type is known.
	Quantization string `json:"quantization,omitempty"`
	ChatTemplate string `json:"chat_template,omitempty"`
	// Metadata keeps scalar string and number values under 256 bytes, for display.
	Metadata map[string]any `json:"-"`
}

// ErrNotGGUF means the bytes do not start with a GGUF header.
var ErrNotGGUF = errors.New("not a GGUF file")

const ggufMagic = 0x46554747 // "GGUF" little-endian

// Limits on what a header may contain. Far above any real model, low enough that a
// hostile file cannot make the parser allocate without bound.
const (
	maxKV          = 1 << 16
	maxStringBytes = 1 << 24 // 16 MiB: chat templates are large, vocabularies are split
	maxArrayLen    = 1 << 24
)

// GGUF value types.
const (
	ggufUint8 uint32 = iota
	ggufInt8
	ggufUint16
	ggufInt16
	ggufUint32
	ggufInt32
	ggufFloat32
	ggufBool
	ggufString
	ggufArray
	ggufUint64
	ggufInt64
	ggufFloat64
)

// fileTypes maps general.file_type to its llama.cpp name (llama_ftype).
var fileTypes = map[uint32]string{
	0: "F32", 1: "F16", 2: "Q4_0", 3: "Q4_1", 7: "Q8_0", 8: "Q5_0", 9: "Q5_1",
	10: "Q2_K", 11: "Q3_K_S", 12: "Q3_K_M", 13: "Q3_K_L", 14: "Q4_K_S", 15: "Q4_K_M",
	16: "Q5_K_S", 17: "Q5_K_M", 18: "Q6_K", 19: "IQ2_XXS", 20: "IQ2_XS", 21: "Q2_K_S",
	22: "IQ3_XS", 23: "IQ3_XXS", 24: "IQ1_S", 25: "IQ4_NL", 26: "IQ3_S", 27: "IQ3_M",
	28: "IQ2_S", 29: "IQ2_M", 30: "IQ4_XS", 31: "IQ1_M", 32: "BF16",
}

// ParseGGUF reads a GGUF header. It stops after the key/value section; tensor data
// is never read.
func ParseGGUF(r io.Reader) (*GGUF, error) {
	p := &ggufParser{r: bufio.NewReaderSize(r, 64<<10)}
	magic, err := p.u32()
	if err != nil || magic != ggufMagic {
		return nil, ErrNotGGUF
	}
	g := &GGUF{Metadata: map[string]any{}}
	if g.Version, err = p.u32(); err != nil {
		return nil, err
	}
	if g.Version < 2 || g.Version > 3 {
		return nil, fmt.Errorf("unsupported GGUF version %d (2 and 3 are supported)", g.Version)
	}
	if g.TensorCount, err = p.u64(); err != nil {
		return nil, err
	}
	kvCount, err := p.u64()
	if err != nil {
		return nil, err
	}
	if kvCount > maxKV {
		return nil, fmt.Errorf("GGUF declares %d metadata entries; refusing more than %d", kvCount, maxKV)
	}

	for i := uint64(0); i < kvCount; i++ {
		key, err := p.str()
		if err != nil {
			return nil, fmt.Errorf("metadata key %d: %w", i, err)
		}
		typ, err := p.u32()
		if err != nil {
			return nil, err
		}
		val, err := p.value(typ, 0)
		if err != nil {
			return nil, fmt.Errorf("metadata %q: %w", key, err)
		}
		switch v := val.(type) {
		case string:
			if len(v) < 256 {
				g.Metadata[key] = v
			}
		case nil, []any:
		default:
			g.Metadata[key] = v
		}
		switch key {
		case "general.architecture":
			g.Architecture, _ = val.(string)
		case "general.name":
			g.Name, _ = val.(string)
		case "general.file_type":
			if n, ok := asUint(val); ok && n <= math.MaxUint32 {
				ft := uint32(n)
				g.FileType = &ft
				g.Quantization = fileTypes[ft]
			}
		case "tokenizer.chat_template":
			g.ChatTemplate, _ = val.(string)
		}
	}
	if g.Architecture != "" {
		if n, ok := asUint(g.Metadata[g.Architecture+".context_length"]); ok {
			g.ContextLength = n
		}
	}
	return g, nil
}

type ggufParser struct{ r *bufio.Reader }

func (p *ggufParser) u32() (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(p.r, b[:]); err != nil {
		return 0, truncated(err)
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

func (p *ggufParser) u64() (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(p.r, b[:]); err != nil {
		return 0, truncated(err)
	}
	return binary.LittleEndian.Uint64(b[:]), nil
}

func (p *ggufParser) str() (string, error) {
	n, err := p.u64()
	if err != nil {
		return "", err
	}
	if n > maxStringBytes {
		return "", fmt.Errorf("string of %d bytes exceeds the %d-byte limit", n, maxStringBytes)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(p.r, b); err != nil {
		return "", truncated(err)
	}
	return string(b), nil
}

// value reads one value. Arrays are consumed; their contents are kept only when
// short, because a vocabulary array is hundreds of thousands of strings nobody here
// needs.
func (p *ggufParser) value(typ uint32, depth int) (any, error) {
	fixed := map[uint32]int{
		ggufUint8: 1, ggufInt8: 1, ggufBool: 1, ggufUint16: 2, ggufInt16: 2,
		ggufUint32: 4, ggufInt32: 4, ggufFloat32: 4, ggufUint64: 8, ggufInt64: 8, ggufFloat64: 8,
	}
	if size, ok := fixed[typ]; ok {
		b := make([]byte, size)
		if _, err := io.ReadFull(p.r, b); err != nil {
			return nil, truncated(err)
		}
		switch typ {
		case ggufUint8:
			return uint64(b[0]), nil
		case ggufInt8:
			return int64(int8(b[0])), nil
		case ggufBool:
			return b[0] != 0, nil
		case ggufUint16:
			return uint64(binary.LittleEndian.Uint16(b)), nil
		case ggufInt16:
			return int64(int16(binary.LittleEndian.Uint16(b))), nil
		case ggufUint32:
			return uint64(binary.LittleEndian.Uint32(b)), nil
		case ggufInt32:
			return int64(int32(binary.LittleEndian.Uint32(b))), nil
		case ggufFloat32:
			return float64(math.Float32frombits(binary.LittleEndian.Uint32(b))), nil
		case ggufUint64:
			return binary.LittleEndian.Uint64(b), nil
		case ggufInt64:
			return int64(binary.LittleEndian.Uint64(b)), nil
		case ggufFloat64:
			return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil
		}
	}
	switch typ {
	case ggufString:
		return p.str()
	case ggufArray:
		if depth > 2 {
			return nil, errors.New("arrays nested deeper than 2 are not supported")
		}
		elem, err := p.u32()
		if err != nil {
			return nil, err
		}
		n, err := p.u64()
		if err != nil {
			return nil, err
		}
		if n > maxArrayLen {
			return nil, fmt.Errorf("array of %d elements exceeds the limit", n)
		}
		var keep []any
		for i := uint64(0); i < n; i++ {
			v, err := p.value(elem, depth+1)
			if err != nil {
				return nil, err
			}
			if n <= 64 {
				keep = append(keep, v)
			}
		}
		return keep, nil
	}
	return nil, fmt.Errorf("unknown GGUF value type %d", typ)
}

func asUint(v any) (uint64, bool) {
	switch n := v.(type) {
	case uint64:
		return n, true
	case int64:
		if n >= 0 {
			return uint64(n), true
		}
	}
	return 0, false
}

func truncated(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errors.New("GGUF header is truncated")
	}
	return err
}
