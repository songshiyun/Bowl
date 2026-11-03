package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/starsrc/bowl/client/internal/session"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const diffHelp = `用法:
  bowl diff <disk-id> [--from <ref>] [--to <ref>] [--endpoint <url>] [--token <token>] [--json]

--from 和 --to 默认为 main；ref 可以是分支名或 Commit ID。
`

const (
	diffMaxTextSize = 1 << 20
	diffSniffSize   = 8192
	diffFileType    = 0o170000
	diffRegularFile = 0o100000
)

func validDiffRef(ref string) bool {
	if len(ref) == 0 || len(ref) > 255 || ref == "." || ref == ".." {
		return false
	}
	for i := range len(ref) {
		if ref[i] == '/' || ref[i] < 0x20 || ref[i] == 0x7f {
			return false
		}
	}
	return true
}

func executeDiff(args []string, stdout, stderr io.Writer) int {
	from, to := "main", "main"
	var endpoint, token string
	var jsonOutput bool
	var positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "-") {
			positional = append(positional, arg)
			continue
		}
		name, value, hasValue := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		switch name {
		case "h", "help":
			fmt.Fprint(stdout, diffHelp)
			return 0
		case "json":
			if hasValue {
				fmt.Fprintln(stderr, "错误: --json 不接受参数值")
				return 2
			}
			jsonOutput = true
		case "from", "to", "endpoint", "token":
			if !hasValue {
				if i+1 == len(args) || strings.HasPrefix(args[i+1], "--") {
					fmt.Fprintf(stderr, "错误: --%s 需要参数值\n", name)
					return 2
				}
				i++
				value = args[i]
			}
			if value == "" {
				fmt.Fprintf(stderr, "错误: --%s 不能为空\n", name)
				return 2
			}
			switch name {
			case "from":
				from = value
			case "to":
				to = value
			case "endpoint":
				endpoint = value
			case "token":
				token = value
			}
		default:
			fmt.Fprintf(stderr, "错误: 未知标志 %q\n%s", arg, diffHelp)
			return 2
		}
	}
	if len(positional) != 1 {
		fmt.Fprint(stderr, diffHelp)
		return 2
	}
	id, err := uuid.Parse(positional[0])
	if err != nil || id == uuid.Nil {
		fmt.Fprintln(stderr, "错误: disk-id 必须是非零 UUID")
		return 2
	}
	if !validDiffRef(from) || !validDiffRef(to) {
		fmt.Fprintln(stderr, "错误: 版本引用无效")
		return 2
	}
	client, code, err := newDiskStorageClient(endpoint, token)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return code
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	response, err := client.Diff(ctx, connect.NewRequest(&storagev1.DiffRequest{DiskId: id.String(), FromRef: from, ToRef: to}))
	if err != nil {
		fmt.Fprintf(stderr, "错误: 获取差异失败: %v\n", err)
		return 1
	}
	if jsonOutput {
		data, err := (protojson.MarshalOptions{UseProtoNames: true, Indent: "  "}).Marshal(response.Msg)
		if err != nil {
			fmt.Fprintf(stderr, "错误: 序列化差异失败: %v\n", err)
			return 1
		}
		fmt.Fprintln(stdout, string(data))
		return 0
	}
	renderer := diffRenderer{client: client, disk: id.String(), from: from, to: to}
	defer renderer.close()
	for _, entry := range response.Msg.Entries {
		if err := renderer.render(ctx, stdout, entry); err != nil {
			fmt.Fprintf(stderr, "错误: 渲染 %s 失败: %v\n", entry.GetPath(), err)
			return 1
		}
	}
	return 0
}

type diffRenderer struct {
	client storagev1connect.StorageServiceClient
	disk   string
	from   string
	to     string
	old    *session.Manager
	new    *session.Manager
}

func (r *diffRenderer) close() {
	if r.old != nil {
		_ = r.old.Close()
	}
	if r.new != nil {
		_ = r.new.Close()
	}
}

func (r *diffRenderer) getSession(ctx context.Context, old bool) (*session.Manager, error) {
	current, ref := &r.new, r.to
	if old {
		current, ref = &r.old, r.from
	}
	if *current != nil {
		return *current, nil
	}
	config := session.Config{DiskID: r.disk, ReadOnly: true, BranchName: ref}
	var err error
	*current, err = session.New(ctx, r.client, config)
	if err != nil && connect.CodeOf(err) == connect.CodeNotFound {
		if id, parseErr := uuid.Parse(ref); parseErr == nil && id != uuid.Nil {
			config.BranchName = ""
			config.CommitID = id.String()
			*current, err = session.New(ctx, r.client, config)
		}
	}
	if err != nil {
		return nil, err
	}
	if err := (*current).Start(ctx); err != nil {
		_ = (*current).Close()
		*current = nil
		return nil, err
	}
	return *current, nil
}

var errDiffSnapshotMismatch = errors.New("只读快照与分支工作区不一致")

func (r *diffRenderer) lookup(ctx context.Context, mgr *session.Manager, path string, expected *storagev1.InodeAttribute) (uint64, error) {
	ino := uint64(1)
	var inode *storagev1.Inode
	for _, name := range strings.Split(path, "/") {
		sid, err := mgr.AcquireSessionID(ctx)
		if err != nil {
			return 0, err
		}
		response, err := r.client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{ParentIno: ino, Name: []byte(name), SessionId: &sid}))
		if connect.CodeOf(err) == connect.CodeNotFound {
			return 0, errDiffSnapshotMismatch
		}
		if err != nil {
			return 0, err
		}
		inode = response.Msg.GetInode()
		if inode == nil || inode.Ino == 0 {
			return 0, errors.New("服务端返回空 Inode")
		}
		ino = inode.Ino
	}
	if inode == nil || !proto.Equal(inode.Attr, expected) {
		return 0, errDiffSnapshotMismatch
	}
	return ino, nil
}

type diffContent struct {
	manager *session.Manager
	ino     uint64
	size    uint64
	data    []byte
}

func (r *diffRenderer) sniff(ctx context.Context, old bool, path string, attr *storagev1.InodeAttribute) (*diffContent, error) {
	content := &diffContent{}
	if attr == nil || attr.Size == 0 {
		return content, nil
	}
	manager, err := r.getSession(ctx, old)
	if err != nil {
		return nil, err
	}
	ino, err := r.lookup(ctx, manager, path, attr)
	if err != nil {
		return nil, err
	}
	content.manager, content.ino, content.size = manager, ino, attr.Size
	if err := r.readUntil(ctx, content, min(attr.Size, diffSniffSize)); err != nil {
		return nil, err
	}
	return content, nil
}

func (r *diffRenderer) readUntil(ctx context.Context, content *diffContent, size uint64) error {
	for uint64(len(content.data)) < size {
		sid, err := content.manager.AcquireSessionID(ctx)
		if err != nil {
			return err
		}
		response, err := r.client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: content.ino, Offset: uint64(len(content.data)), Size: uint32(size - uint64(len(content.data))), SessionId: &sid}))
		if err != nil {
			return err
		}
		if len(response.Msg.Data) == 0 || uint64(len(response.Msg.Data)) > size-uint64(len(content.data)) {
			return errors.New("服务端返回无效的读取长度")
		}
		content.data = append(content.data, response.Msg.Data...)
	}
	return nil
}

func diffName(entry *storagev1.DiffEntry) (string, string) {
	old := entry.Path
	if entry.OldPath != "" {
		old = entry.OldPath
	}
	return old, entry.Path
}

func diffSummary(w io.Writer, entry *storagev1.DiffEntry) {
	old, newPath := diffName(entry)
	fmt.Fprintf(w, "diff --bowl a/%s b/%s\n", old, newPath)
	fmt.Fprintf(w, "%s: size %d -> %d, mode %06o -> %06o, modified_blocks: %v\n", strings.TrimPrefix(entry.ChangeType.String(), "DIFF_CHANGE_TYPE_"), entry.GetOldAttr().GetSize(), entry.GetNewAttr().GetSize(), entry.GetOldAttr().GetMode(), entry.GetNewAttr().GetMode(), entry.ModifiedBlockIndices)
}

func (r *diffRenderer) render(ctx context.Context, w io.Writer, entry *storagev1.DiffEntry) error {
	if entry == nil || entry.Path == "" {
		return errors.New("服务端返回空路径")
	}
	oldAttr, newAttr := entry.OldAttr, entry.NewAttr
	if oldAttr.GetSize() > diffMaxTextSize || newAttr.GetSize() > diffMaxTextSize || oldAttr != nil && oldAttr.Mode&diffFileType != diffRegularFile || newAttr != nil && newAttr.Mode&diffFileType != diffRegularFile {
		diffSummary(w, entry)
		return nil
	}
	oldPath, newPath := diffName(entry)
	oldContent, err := r.sniff(ctx, true, oldPath, oldAttr)
	if err != nil {
		if errors.Is(err, errDiffSnapshotMismatch) {
			diffSummary(w, entry)
			fmt.Fprintln(w, "只读快照与分支工作区不一致，无法展示文本")
			return nil
		}
		return err
	}
	newContent, err := r.sniff(ctx, false, newPath, newAttr)
	if err != nil {
		if errors.Is(err, errDiffSnapshotMismatch) {
			diffSummary(w, entry)
			fmt.Fprintln(w, "只读快照与分支工作区不一致，无法展示文本")
			return nil
		}
		return err
	}
	if bytes.IndexByte(oldContent.data, 0) >= 0 || bytes.IndexByte(newContent.data, 0) >= 0 {
		fmt.Fprintf(w, "Binary files a/%s and b/%s differ\n", oldPath, newPath)
		return nil
	}
	if oldContent.size > 0 {
		if err := r.readUntil(ctx, oldContent, oldContent.size); err != nil {
			return err
		}
	}
	if newContent.size > 0 {
		if err := r.readUntil(ctx, newContent, newContent.size); err != nil {
			return err
		}
	}
	if bytes.IndexByte(oldContent.data, 0) >= 0 || bytes.IndexByte(newContent.data, 0) >= 0 {
		fmt.Fprintf(w, "Binary files a/%s and b/%s differ\n", oldPath, newPath)
		return nil
	}
	diffSummary(w, entry)
	oldLabel, newLabel := "a/"+oldPath, "b/"+newPath
	if oldAttr == nil {
		oldLabel = "/dev/null"
	}
	if newAttr == nil {
		newLabel = "/dev/null"
	}
	writeUnifiedDiff(w, oldLabel, newLabel, oldContent.data, newContent.data)
	return nil
}

type diffLine struct {
	kind byte
	text []byte
}

func diffLines(data []byte) [][]byte {
	if len(data) == 0 {
		return nil
	}
	lines := bytes.SplitAfter(data, []byte{'\n'})
	if len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	return lines
}

// 限制回溯表的内存；差异过大时输出完整的删除和新增行。
func lineEdits(old, newLines [][]byte) []diffLine {
	n, m := len(old), len(newLines)
	maxDistance := n + m
	v := make([]int, 2*maxDistance+3)
	offset := maxDistance + 1
	v[offset+1] = 0
	var trace [][]int
	for d := 0; d <= maxDistance; d++ {
		if len(v)*(d+1) > 4_000_000 {
			break
		}
		found := false
		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || k != d && v[offset+k-1] < v[offset+k+1] {
				x = v[offset+k+1]
			} else {
				x = v[offset+k-1] + 1
			}
			y := x - k
			for x < n && y < m && bytes.Equal(old[x], newLines[y]) {
				x++
				y++
			}
			v[offset+k] = x
			if x >= n && y >= m {
				found = true
				break
			}
		}
		trace = append(trace, append([]int(nil), v...))
		if found {
			x, y := n, m
			out := make([]diffLine, 0, n+m)
			for step := d; step > 0; step-- {
				previous := trace[step-1]
				k := x - y
				previousK := k - 1
				if k == -step || k != step && previous[offset+k-1] < previous[offset+k+1] {
					previousK = k + 1
				}
				previousX := previous[offset+previousK]
				previousY := previousX - previousK
				for x > previousX && y > previousY {
					x--
					y--
					out = append(out, diffLine{' ', old[x]})
				}
				if x == previousX {
					y--
					out = append(out, diffLine{'+', newLines[y]})
				} else {
					x--
					out = append(out, diffLine{'-', old[x]})
				}
			}
			for x > 0 && y > 0 {
				x--
				y--
				out = append(out, diffLine{' ', old[x]})
			}
			for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
				out[i], out[j] = out[j], out[i]
			}
			return out
		}
	}
	out := make([]diffLine, 0, n+m)
	for _, line := range old {
		out = append(out, diffLine{'-', line})
	}
	for _, line := range newLines {
		out = append(out, diffLine{'+', line})
	}
	return out
}

func writeUnifiedDiff(w io.Writer, oldLabel, newLabel string, oldData, newData []byte) {
	fmt.Fprintf(w, "--- %s\n+++ %s\n", oldLabel, newLabel)
	edits := lineEdits(diffLines(oldData), diffLines(newData))
	oldCounts, newCounts := make([]int, len(edits)+1), make([]int, len(edits)+1)
	var changes []int
	for i, edit := range edits {
		oldCounts[i+1], newCounts[i+1] = oldCounts[i], newCounts[i]
		if edit.kind != '+' {
			oldCounts[i+1]++
		}
		if edit.kind != '-' {
			newCounts[i+1]++
		}
		if edit.kind != ' ' {
			changes = append(changes, i)
		}
	}
	for i := 0; i < len(changes); {
		first, last := changes[i], changes[i]
		i++
		for i < len(changes) && changes[i]-last <= 7 {
			last = changes[i]
			i++
		}
		start, end := max(0, first-3), min(len(edits), last+4)
		oldCount, newCount := oldCounts[end]-oldCounts[start], newCounts[end]-newCounts[start]
		oldStart, newStart := oldCounts[start]+1, newCounts[start]+1
		if oldCount == 0 {
			oldStart--
		}
		if newCount == 0 {
			newStart--
		}
		fmt.Fprintf(w, "@@ -%d,%d +%d,%d @@\n", oldStart, oldCount, newStart, newCount)
		for _, edit := range edits[start:end] {
			fmt.Fprintf(w, "%c%s", edit.kind, edit.text)
			if len(edit.text) == 0 || edit.text[len(edit.text)-1] != '\n' {
				fmt.Fprint(w, "\n\\ No newline at end of file\n")
			}
		}
	}
}
