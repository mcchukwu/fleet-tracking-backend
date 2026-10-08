package config

import "testing"

func TestLoadIngestRejectsMalformedNumericConfiguration(t *testing.T) {
	t.Setenv("ACCEPT_RATE", "fast")
	if _, err := LoadIngest(); err == nil {
		t.Fatal("LoadIngest accepted malformed ACCEPT_RATE")
	}
}

func TestIngestValidateRejectsSamePublicAndAdminAddress(t *testing.T) {
	cfg := Ingest{OverflowPolicy: "drop_oldest", IdleTimeout: 1, AcceptRate: 1, AcceptBurst: 1, ColdPathBatchWindow: 1, ColdPathBatchSize: 1, ListenAddr: ":8080", AdminAddr: ":8080"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate accepted equal listener addresses")
	}
}
