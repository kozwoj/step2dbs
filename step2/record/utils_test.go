package record

import (
	"testing"
)

func TestParseCompactTime(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected uint64
		wantErr  bool
	}{
		// H:MM format (seconds and milliseconds default to 0)
		{"H:MM single digit hour", "5:30", 5*3_600_000 + 30*60_000, false},
		{"H:MM double digit hour", "14:45", 14*3_600_000 + 45*60_000, false},
		{"H:MM zero minutes", "3:00", 3 * 3_600_000, false},
		{"H:MM midnight", "0:00", 0, false},

		// H:MM:SS format (milliseconds default to 0)
		{"H:MM:SS single digit hour", "5:30:15", 5*3_600_000 + 30*60_000 + 15*1_000, false},
		{"H:MM:SS double digit hour", "14:45:30", 14*3_600_000 + 45*60_000 + 30*1_000, false},
		{"H:MM:SS zero seconds", "3:15:00", 3*3_600_000 + 15*60_000, false},
		{"H:MM:SS all zeros", "0:00:00", 0, false},

		// H:MM:SS.mmm format (full format)
		{"H:MM:SS.mmm single digit hour", "5:30:15.123", 5*3_600_000 + 30*60_000 + 15*1_000 + 123, false},
		{"H:MM:SS.mmm double digit hour", "14:45:30.500", 14*3_600_000 + 45*60_000 + 30*1_000 + 500, false},
		{"H:MM:SS.mmm zero milliseconds", "3:15:20.000", 3*3_600_000 + 15*60_000 + 20*1_000, false},
		{"H:MM:SS.mmm all zeros", "0:00:00.000", 0, false},

		// Edge cases
		{"23:59:59.999", "23:59:59.999", 23*3_600_000 + 59*60_000 + 59*1_000 + 999, false},
		{"Single digit values", "1:01", 1*3_600_000 + 1*60_000, false},

		// Error cases
		{"Invalid format - no colon", "1234", 0, true},
		{"Invalid format - single part", "12", 0, true},
		{"Invalid format - too many parts", "12:30:45:123", 0, true},
		{"Invalid hours", "abc:30", 0, true},
		{"Invalid minutes", "12:abc", 0, true},
		{"Invalid seconds", "12:30:abc", 0, true},
		{"Invalid milliseconds", "12:30:45.abc", 0, true},
		{"Empty string", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConvertCompactTime(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ConvertCompactTime(%q) expected error, got nil", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("ConvertCompactTime(%q) unexpected error: %v", tt.input, err)
				return
			}
			if result != tt.expected {
				t.Errorf("ConvertCompactTime(%q) = %d, want %d", tt.input, result, tt.expected)
			}
		})
	}
}

func TestFormatMillis(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected string
	}{
		// H:MM format (when seconds and milliseconds are 0)
		{"Zero", 0, "0:00"},
		{"5:30", 5*3_600_000 + 30*60_000, "5:30"},
		{"14:45", 14*3_600_000 + 45*60_000, "14:45"},
		{"23:59", 23*3_600_000 + 59*60_000, "23:59"},

		// H:MM:SS format (when milliseconds are 0)
		{"5:30:15", 5*3_600_000 + 30*60_000 + 15*1_000, "5:30:15"},
		{"14:45:30", 14*3_600_000 + 45*60_000 + 30*1_000, "14:45:30"},
		{"0:00:01", 1_000, "0:00:01"},
		{"23:59:59", 23*3_600_000 + 59*60_000 + 59*1_000, "23:59:59"},

		// H:MM:SS.mmm format (full format with milliseconds)
		{"5:30:15.123", 5*3_600_000 + 30*60_000 + 15*1_000 + 123, "5:30:15.123"},
		{"14:45:30.500", 14*3_600_000 + 45*60_000 + 30*1_000 + 500, "14:45:30.500"},
		{"0:00:00.001", 1, "0:00:00.001"},
		{"23:59:59.999", 23*3_600_000 + 59*60_000 + 59*1_000 + 999, "23:59:59.999"},

		// Edge cases - verify zero padding
		{"1:01", 1*3_600_000 + 1*60_000, "1:01"},
		{"1:01:01", 1*3_600_000 + 1*60_000 + 1*1_000, "1:01:01"},
		{"1:01:01.001", 1*3_600_000 + 1*60_000 + 1*1_000 + 1, "1:01:01.001"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatMillis(tt.input)
			if result != tt.expected {
				t.Errorf("FormatMillis(%d) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseAndFormatRoundTrip(t *testing.T) {
	// Test that parsing and formatting are inverse operations for compact formats
	inputs := []string{
		"5:30",
		"14:45",
		"5:30:15",
		"14:45:30",
		"5:30:15.123",
		"14:45:30.500",
		"0:00",
		"0:00:00",
		"0:00:00.000",
		"23:59:59.999",
	}

	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			// Parse the input
			millis, err := ConvertCompactTime(input)
			if err != nil {
				t.Fatalf("ConvertCompactTime(%q) error: %v", input, err)
			}

			// Format it back
			formatted := FormatMillis(millis)

			// Parse again to compare values
			millis2, err := ConvertCompactTime(formatted)
			if err != nil {
				t.Fatalf("ConvertCompactTime(%q) error after format: %v", formatted, err)
			}

			// Values should match
			if millis != millis2 {
				t.Errorf("Round trip failed: %q -> %d -> %q -> %d", input, millis, formatted, millis2)
			}
		})
	}
}
