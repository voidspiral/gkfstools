package main

import "testing"

func TestGetDaemonPidByRank(t *testing.T) {
	type args struct {
		hostFile string
		line     int
	}
	tests := []struct {
		name    string
		args    args
		wantPid string
	}{
		// TODO: Add test cases.
		{name: "testfile", args: args{
			hostFile: "gkfs_host.txt.pid",
			line:     2,
		}, wantPid: "2163292"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotPid := GetDaemonPidByRank(tt.args.hostFile, tt.args.line); gotPid != tt.wantPid {
				t.Errorf("GetDaemonPidByRank() = %v, want %v", gotPid, tt.wantPid)
			}
		})
	}
}
