package identity

import "testing"

func TestPasswordRoundTrip(t *testing.T) {
	hash, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	ok, err := VerifyPassword(hash, "correct horse battery staple")
	if err != nil || !ok {
		t.Fatalf("verify = %v, %v", ok, err)
	}
	ok, _ = VerifyPassword(hash, "wrong")
	if ok {
		t.Fatal("wrong password accepted")
	}
}

func TestTokenHashStable(t *testing.T) {
	raw, hash, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	if raw == "" || hash == "" || HashToken(raw) != hash {
		t.Fatal("token hash mismatch")
	}
}
