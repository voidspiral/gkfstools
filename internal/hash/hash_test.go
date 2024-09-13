package hash

import "testing"

func TestGetHash(t *testing.T) {
	t.Log(GetHash("/1G"))
}
