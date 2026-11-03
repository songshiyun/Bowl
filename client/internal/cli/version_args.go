package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// versionArgs 保留显式指定的标志，以便检查互斥选项。
type versionArgs struct {
	values     map[string]string
	set        map[string]bool
	positional []string
}

func parseVersionArgs(args []string, extra map[string]bool, help string, stdout, stderr io.Writer) (versionArgs, int) {
	opts := versionArgs{values: make(map[string]string), set: make(map[string]bool)}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			opts.positional = append(opts.positional, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			opts.positional = append(opts.positional, arg)
			continue
		}
		name, value, hasValue := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if name == "h" || name == "help" {
			if hasValue {
				fmt.Fprintln(stderr, "错误: --help 不接受参数值")
				return opts, 2
			}
			fmt.Fprint(stdout, help)
			return opts, 0
		}
		if name == "m" {
			name = "message"
		}
		takesValue, known := extra[name]
		if name == "endpoint" || name == "token" {
			takesValue, known = true, true
		} else if name == "json" {
			takesValue, known = false, true
		}
		if !known {
			fmt.Fprintf(stderr, "错误: 未知标志 %q\n%s", arg, help)
			return opts, 2
		}
		if takesValue {
			if !hasValue {
				if i+1 == len(args) || strings.HasPrefix(args[i+1], "--") {
					fmt.Fprintf(stderr, "错误: --%s 需要参数值\n", name)
					return opts, 2
				}
				i++
				value = args[i]
			}
		} else if hasValue {
			fmt.Fprintf(stderr, "错误: --%s 不接受参数值\n", name)
			return opts, 2
		}
		opts.values[name], opts.set[name] = value, true
	}
	return opts, -1
}

func (a versionArgs) diskID(stderr io.Writer, help string) (string, bool) {
	if len(a.positional) != 1 {
		fmt.Fprintf(stderr, "错误: 需要且仅需要一个 disk-id\n%s", help)
		return "", false
	}
	id, err := uuid.Parse(a.positional[0])
	if err != nil || id == uuid.Nil {
		fmt.Fprintln(stderr, "错误: disk-id 必须是非零 UUID")
		return "", false
	}
	return id.String(), true
}

func parseVersionUUID(value string) (string, bool) {
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil {
		return "", false
	}
	return id.String(), true
}

func validVersionBranchName(name string) bool {
	if name == "" || !validBranchName(name) || !utf8.ValidString(name) {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func writeVersionJSON(stdout, stderr io.Writer, value any) int {
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		fmt.Fprintf(stderr, "错误: 序列化 JSON 失败: %v\n", err)
		return 1
	}
	return 0
}

func writeVersionProtoJSON(stdout, stderr io.Writer, value proto.Message) int {
	data, err := (protojson.MarshalOptions{UseProtoNames: true, Indent: "  "}).Marshal(value)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 序列化 JSON 失败: %v\n", err)
		return 1
	}
	if _, err := fmt.Fprintln(stdout, string(data)); err != nil {
		fmt.Fprintf(stderr, "错误: 输出 JSON 失败: %v\n", err)
		return 1
	}
	return 0
}
