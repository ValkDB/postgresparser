package ident

import "testing"

func TestTrimQuotes(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "no quotes",
			input: "users",
			want:  "users",
		},
		{
			name:  "wrapped quotes",
			input: `"users"`,
			want:  "users",
		},
		{
			name:  "only leading quote",
			input: `"users`,
			want:  "users",
		},
		{
			name:  "only trailing quote",
			input: `users"`,
			want:  "users",
		},
		{
			name:  "escaped quote inside preserved",
			input: `"A""B"`,
			want:  `A""B`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := TrimQuotes(tc.input)
			if got != tc.want {
				t.Fatalf("TrimQuotes(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}
