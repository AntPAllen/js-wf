package graphpublication

import "testing"

func TestNativeReaderMaintenanceScopeIsolation(t *testing.T) {
	p := &NativePort{NativeAuthority: &NativeAuthority{name: "AUTH", prefix: "wf.graph.auth"}, bucket: "OBJECTS"}
	want := p.ReaderMaintenanceScope()
	if len(want) != 64 {
		t.Fatal(want)
	}
	copyPort := *p
	if copyPort.ReaderMaintenanceScope() != want {
		t.Fatal("scope is not stable")
	}
	for _, other := range []*NativePort{
		{NativeAuthority: &NativeAuthority{name: "OTHER", prefix: "wf.graph.auth"}, bucket: "OBJECTS"},
		{NativeAuthority: &NativeAuthority{name: "AUTH", prefix: "wf.graph.other"}, bucket: "OBJECTS"},
		{NativeAuthority: &NativeAuthority{name: "AUTH", prefix: "wf.graph.auth"}, bucket: "OTHER_OBJECTS"},
	} {
		if other.ReaderMaintenanceScope() == want {
			t.Fatal("namespace scope collision")
		}
	}
	if (*NativePort)(nil).ReaderMaintenanceScope() != "" || (&NativePort{}).ReaderMaintenanceScope() != "" {
		t.Fatal("incomplete port returned scope")
	}
}
