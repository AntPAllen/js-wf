package checkpoint

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
)

var rawMessageType = reflect.TypeOf(json.RawMessage{})

// DecodeUnambiguous preserves the exact hashed serialization, including field
// order and whitespace, while rejecting duplicate or case-aliased typed keys.
// RawMessage values are opaque user/record bytes. Callers enforce byte limits
// and use target only after successful admission.
func DecodeUnambiguous(raw []byte, target any) error {
	t := reflect.TypeOf(target)
	if t == nil || t.Kind() != reflect.Pointer {
		return invalid("envelope target")
	}
	if err := inspectEnvelope(raw, t.Elem()); err != nil {
		return err
	}
	return decodeStrict(raw, target)
}

func inspectEnvelope(raw []byte, t reflect.Type) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == rawMessageType {
		return nil
	}
	raw = bytes.TrimSpace(raw)
	if bytes.Equal(raw, []byte("null")) || len(raw) == 0 {
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	switch t.Kind() {
	case reflect.Struct, reflect.Map:
		if raw[0] != '{' {
			return nil
		} // Typed decode reports shape errors.
		if _, err := d.Token(); err != nil {
			return err
		}
		fields := map[string]reflect.Type{}
		if t.Kind() == reflect.Struct {
			for i := 0; i < t.NumField(); i++ {
				field := t.Field(i)
				if !field.IsExported() {
					continue
				}
				name := strings.Split(field.Tag.Get("json"), ",")[0]
				if name == "-" {
					continue
				}
				if name == "" {
					name = field.Name
				}
				fields[name] = field.Type
			}
		}
		seen := map[string]bool{}
		for d.More() {
			token, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := token.(string)
			if !ok {
				return invalid("envelope key")
			}
			key := name
			var valueType reflect.Type
			if t.Kind() == reflect.Struct {
				valueType = fields[name]
				if valueType == nil {
					return invalid("unknown or aliased envelope field")
				}
			} else {
				valueType = t.Elem()
				switch t.Key().Kind() {
				case reflect.String:
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					value, err := strconv.ParseUint(name, 10, t.Key().Bits())
					if err != nil {
						return err
					}
					key = strconv.FormatUint(value, 10)
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					value, err := strconv.ParseInt(name, 10, t.Key().Bits())
					if err != nil {
						return err
					}
					key = strconv.FormatInt(value, 10)
				default:
					return invalid("unsupported envelope map key")
				}
			}
			if seen[key] {
				return invalid("duplicate envelope key")
			}
			seen[key] = true
			var value json.RawMessage
			if err := d.Decode(&value); err != nil {
				return err
			}
			if err := inspectEnvelope(value, valueType); err != nil {
				return err
			}
		}
		_, err := d.Token()
		return err
	case reflect.Slice, reflect.Array:
		if raw[0] != '[' || t.Elem().Kind() == reflect.Uint8 {
			return nil
		}
		if _, err := d.Token(); err != nil {
			return err
		}
		for d.More() {
			var value json.RawMessage
			if err := d.Decode(&value); err != nil {
				return err
			}
			if err := inspectEnvelope(value, t.Elem()); err != nil {
				return err
			}
		}
		_, err := d.Token()
		return err
	}
	return nil
}
