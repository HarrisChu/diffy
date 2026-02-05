package main

import (
	"syscall"
	"testing"
)

func TestNebula(t *testing.T) {
	syscall.Setenv("NEBULA_ADDRESS", "192.168.15.8:9669")
	syscall.Setenv("NEBULA_USER", "root")
	syscall.Setenv("NEBULA_PASSWORD", "NebulaGraph01")
	syscall.Setenv("NEBULA_STATEMENT", "use sf10 match(v) return v limit 10000")
	for _, typ := range []string{"default", "no_scan", "no_reuse"} {
		bs, err := run(typ)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("scan type: %s, resp: %s", typ, string(bs))
	}

	t.Fatal(1)
}
