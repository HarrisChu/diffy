package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/HarrisChu/diffy/go/pkg"
)

type resp struct {
	AvgLatencyUs  int64 `json:"avg_latency_us"`
	AvgResponseUs int64 `json:"avg_response_us"`
	AvgDecodeUs   int64 `json:"avg_decode_us"`
}

func main() {
	scanTypeStr := os.Args[1]
	scanTypeStr = strings.TrimSpace(scanTypeStr)
	bs, err := run(scanTypeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Print(string(bs))
}

func run(typStr string) ([]byte, error) {
	cfg := pkg.NewConfigFromEnv()
	var scanType pkg.ScanType
	switch typStr {
	case "default":
		scanType = pkg.ScanTypeDefault
	case "no_scan":
		scanType = pkg.ScanTypeNoScan
	case "no_reuse":
		scanType = pkg.ScanTypeNoReuse
	default:
		return nil, fmt.Errorf("unknown scan type: %s", typStr)
	}
	controller := pkg.NewController(cfg, scanType)
	latencyUs, responseUs, decodeUs, err := controller.Run()
	if err != nil {
		return nil, err
	}
	r := resp{
		AvgLatencyUs:  latencyUs,
		AvgResponseUs: responseUs,
		AvgDecodeUs:   decodeUs,
	}
	bs, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return bs, nil
}
