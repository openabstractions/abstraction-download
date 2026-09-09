package download

import (
	"archive/zip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"testing"
)

// TestLiveSliceReadsModelHeaders is the demonstration: what an architecture, a
// context length and a quantisation cost when nobody downloads the model.
//
// Off by default. Set ABSTRACTION_LIVE_SLICE=1. It moves a few hundred
// kilobytes and would move a hundred gigabytes if the layer were broken, so the
// assertion is on bytes fetched and not only on what was parsed.
//
// The GGUF and safetensors parsing lives in this test rather than in the layer
// on purpose: Slice.Head knows nothing about models, and the moment it does,
// every adopter inherits a format table this project would have to maintain.
func TestLiveSliceReadsModelHeaders(t *testing.T) {
	if os.Getenv("ABSTRACTION_LIVE_SLICE") == "" {
		t.Skip("set ABSTRACTION_LIVE_SLICE=1 to read real headers over the network")
	}
	models := []struct{ name, url string }{
		{"llama-2-7b Q4_K_M gguf", "https://huggingface.co/TheBloke/Llama-2-7B-GGUF/resolve/main/llama-2-7b.Q4_K_M.gguf"},
		{"mistral-7b-instruct Q4_K_M gguf", "https://huggingface.co/TheBloke/Mistral-7B-Instruct-v0.2-GGUF/resolve/main/mistral-7b-instruct-v0.2.Q4_K_M.gguf"},
		{"tinyllama-1.1b safetensors", "https://huggingface.co/TinyLlama/TinyLlama-1.1B-Chat-v1.0/resolve/main/model.safetensors"},
		{"qwen2.5-7b shard 1 safetensors", "https://huggingface.co/Qwen/Qwen2.5-7B-Instruct/resolve/main/model-00001-of-00004.safetensors"},
	}
	for _, m := range models {
		t.Run(m.name, func(t *testing.T) {
			sl := &Slice{Fetcher: HTTP{}, Source: Source{Scheme: "https", Locator: m.url}}
			size, err := sl.Size(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			var summary string
			if strings.HasSuffix(m.url, ".gguf") {
				summary, err = describeGGUF(context.Background(), sl)
			} else {
				summary, err = describeSafetensors(context.Background(), sl)
			}
			if err != nil {
				t.Fatal(err)
			}
			r := sl.Receipt()
			t.Logf("%s\n  size      %d bytes\n  fetched   %d bytes in %d requests (%.5f%% of the file)\n  read      %v\n  %s\n  %s",
				m.name, size, r.Fetched, r.Requests, 100*float64(r.Fetched)/float64(size), r.Read, summary, r.Unverified)
			if r.Fetched > size/100 {
				t.Fatalf("fetched %d of %d bytes: this is a download wearing a slice's name", r.Fetched, size)
			}
		})
	}
}

// TestLiveSliceTakesOneMemberOfARemoteZip does the same for an archive nobody
// downloaded, on the two host shapes the survey said differ: a Maven artifact
// served straight off origin, and a GitHub release asset behind a CDN that
// refuses suffix ranges.
func TestLiveSliceTakesOneMemberOfARemoteZip(t *testing.T) {
	if os.Getenv("ABSTRACTION_LIVE_SLICE") == "" {
		t.Skip("set ABSTRACTION_LIVE_SLICE=1 to read real archives over the network")
	}
	for _, a := range []struct{ name, url string }{
		{"maven slf4j-api jar", "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.9/slf4j-api-2.0.9.jar"},
		{"pypi wheel", "https://files.pythonhosted.org/packages/f9/9b/335f9764261e915ed497fcdeb11df5dfd6f7bf257d4a6a2a686d80da4d54/requests-2.32.3-py3-none-any.whl"},
		{"github release zip behind a CDN that refuses suffix ranges", "https://github.com/cli/cli/releases/download/v2.40.1/gh_2.40.1_windows_amd64.zip"},
	} {
		t.Run(a.name, func(t *testing.T) {
			if err := Indexable(a.url); err != nil {
				t.Fatal(err)
			}
			sl := &Slice{Fetcher: HTTP{}, Source: Source{Scheme: "https", Locator: a.url}}
			ar, err := sl.Zip(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			afterDirectory := sl.Receipt()
			size := afterDirectory.Size

			// Every entry is validated, not only the one taken: a `../` entry a
			// caller never extracts is still a `../` entry a caller might.
			var pick *zip.File
			for _, f := range ar.Entries() {
				if _, err := MemberPath("live", f.Name); err != nil {
					t.Fatalf("%s: %v", f.Name, err)
				}
				if f.FileInfo().IsDir() || f.UncompressedSize64 == 0 {
					continue
				}
				if pick == nil || f.UncompressedSize64 < pick.UncompressedSize64 {
					pick = f
				}
			}
			if pick == nil {
				t.Fatal("no member worth taking")
			}
			t.Logf("%s\n  archive   %d bytes, %d members\n  directory %d bytes in %d requests (%.4f%%)",
				a.name, size, len(ar.Entries()), afterDirectory.Fetched, afterDirectory.Requests,
				100*float64(afterDirectory.Fetched)/float64(size))

			n, err := ar.Member(context.Background(), pick.Name, io.Discard, 64<<20)
			if err != nil {
				t.Fatal(err)
			}
			r := sl.Receipt()
			t.Logf("  member    %q, %d bytes\n  total     %d bytes in %d requests (%.4f%% of the archive)\n  %s",
				pick.Name, n, r.Fetched, r.Requests, 100*float64(r.Fetched)/float64(size), r.Unverified)
			if r.Fetched >= size {
				t.Fatalf("fetched %d of a %d byte archive", r.Fetched, size)
			}
		})
	}
}

// describeGGUF reads architecture, context length and quantisation out of the
// metadata block. Grown rather than guessed: the tokenizer's vocabulary is IN
// that block, so its length varies by two orders of magnitude between models.
func describeGGUF(ctx context.Context, sl *Slice) (string, error) {
	p := sl.Prefix()
	var last error
	for _, want := range []int64{1 << 18, 1 << 20, 1 << 22, 24 << 20} {
		buf, err := p.Grow(ctx, want)
		if err != nil {
			return "", err
		}
		s, err := parseGGUF(buf)
		if err == nil {
			return s, nil
		}
		last = err
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			return "", err
		}
		if int64(len(buf)) < want {
			break
		}
	}
	return "", last
}

type ggufReader struct {
	b []byte
	i int
}

func (r *ggufReader) take(n int) ([]byte, error) {
	if n < 0 || r.i+n > len(r.b) {
		return nil, io.ErrUnexpectedEOF
	}
	v := r.b[r.i : r.i+n]
	r.i += n
	return v, nil
}

func (r *ggufReader) u32() (uint32, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (r *ggufReader) u64() (uint64, error) {
	b, err := r.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *ggufReader) str() (string, error) {
	n, err := r.u64()
	if err != nil {
		return "", err
	}
	if n > 1<<24 {
		return "", fmt.Errorf("gguf: a %d byte string is not a name", n)
	}
	b, err := r.take(int(n))
	return string(b), err
}

var ggufFixed = map[uint32]int{0: 1, 1: 1, 2: 2, 3: 2, 4: 4, 5: 4, 6: 4, 7: 1, 10: 8, 11: 8, 12: 8}

func (r *ggufReader) value(kind uint32) (string, error) {
	if n, ok := ggufFixed[kind]; ok {
		b, err := r.take(n)
		if err != nil {
			return "", err
		}
		switch kind {
		case 4:
			return fmt.Sprint(binary.LittleEndian.Uint32(b)), nil
		case 5:
			return fmt.Sprint(int32(binary.LittleEndian.Uint32(b))), nil
		case 10:
			return fmt.Sprint(binary.LittleEndian.Uint64(b)), nil
		case 11:
			return fmt.Sprint(int64(binary.LittleEndian.Uint64(b))), nil
		case 6:
			return fmt.Sprint(math.Float32frombits(binary.LittleEndian.Uint32(b))), nil
		case 12:
			return fmt.Sprint(math.Float64frombits(binary.LittleEndian.Uint64(b))), nil
		}
		return fmt.Sprint(b[0]), nil
	}
	switch kind {
	case 8:
		return r.str()
	case 9:
		elem, err := r.u32()
		if err != nil {
			return "", err
		}
		count, err := r.u64()
		if err != nil {
			return "", err
		}
		if n, ok := ggufFixed[elem]; ok {
			if _, err := r.take(int(count) * n); err != nil {
				return "", err
			}
		} else if elem == 8 {
			for i := uint64(0); i < count; i++ {
				if _, err := r.str(); err != nil {
					return "", err
				}
			}
		} else {
			return "", fmt.Errorf("gguf: array of type %d", elem)
		}
		return fmt.Sprintf("[%d items]", count), nil
	}
	return "", fmt.Errorf("gguf: value type %d", kind)
}

func parseGGUF(b []byte) (string, error) {
	r := &ggufReader{b: b}
	magic, err := r.take(4)
	if err != nil {
		return "", err
	}
	if string(magic) != "GGUF" {
		return "", errors.New("gguf: not a GGUF file")
	}
	version, err := r.u32()
	if err != nil {
		return "", err
	}
	tensors, err := r.u64()
	if err != nil {
		return "", err
	}
	kvs, err := r.u64()
	if err != nil {
		return "", err
	}
	if kvs > 1<<16 {
		return "", fmt.Errorf("gguf: %d metadata keys", kvs)
	}
	want := map[string]string{
		"general.architecture": "", "general.name": "", "general.file_type": "",
		"general.quantization_version": "",
	}
	arch := ""
	for i := uint64(0); i < kvs; i++ {
		key, err := r.str()
		if err != nil {
			return "", err
		}
		kind, err := r.u32()
		if err != nil {
			return "", err
		}
		v, err := r.value(kind)
		if err != nil {
			return "", err
		}
		if key == "general.architecture" {
			arch = v
		}
		if _, ok := want[key]; ok {
			want[key] = v
		}
		if arch != "" && strings.HasPrefix(key, arch+".") {
			want[key] = v
		}
	}
	keys := make([]string, 0, len(want))
	for k, v := range want {
		if v != "" && !strings.Contains(v, "items") {
			keys = append(keys, k+"="+v)
		}
	}
	// The tensor table follows the metadata and its first entry names the
	// quantisation actually used, which general.file_type only summarises.
	firstTensor := ""
	if tensors > 0 {
		name, err := r.str()
		if err == nil {
			dims, e := r.u32()
			if e == nil {
				shape := make([]uint64, 0, dims)
				for d := uint32(0); d < dims && e == nil; d++ {
					var n uint64
					n, e = r.u64()
					shape = append(shape, n)
				}
				if kind, e2 := r.u32(); e == nil && e2 == nil {
					firstTensor = fmt.Sprintf(" first tensor %s %v type %d", name, shape, kind)
				}
			}
		}
	}
	return fmt.Sprintf("GGUF v%d, %d tensors, %d metadata keys; %s;%s",
		version, tensors, kvs, strings.Join(keys, " "), firstTensor), nil
}

func describeSafetensors(ctx context.Context, sl *Slice) (string, error) {
	p := sl.Prefix()
	head, err := p.Grow(ctx, 8)
	if err != nil {
		return "", err
	}
	n := binary.LittleEndian.Uint64(head)
	if n > 64<<20 {
		return "", fmt.Errorf("safetensors: a %d byte header is not a header", n)
	}
	buf, err := p.Grow(ctx, int64(8+n))
	if err != nil {
		return "", err
	}
	var hdr map[string]json.RawMessage
	if err := json.Unmarshal(buf[8:8+n], &hdr); err != nil {
		return "", err
	}
	dtypes := map[string]int{}
	for name, raw := range hdr {
		if name == "__metadata__" {
			continue
		}
		var t struct {
			Dtype string `json:"dtype"`
		}
		json.Unmarshal(raw, &t)
		dtypes[t.Dtype]++
	}
	parts := make([]string, 0, len(dtypes))
	for d, c := range dtypes {
		parts = append(parts, fmt.Sprintf("%s x%d", d, c))
	}
	return fmt.Sprintf("safetensors, %d byte JSON header, %d tensors; %s", n, len(hdr), strings.Join(parts, ", ")), nil
}
