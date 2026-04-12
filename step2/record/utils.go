package record

import (
	"encoding/binary"
	"fmt"
	"math"
	"github.com/kozwoj/step2/db"
	"strconv"
	"strings"
	"time"
)

// ================================== Record Utilities ==================================

/*
GetRecordHeader returns RecordHeader from the given record bytes.
Parameters:
- recordBytes: the byte slice containing the record data, including header and field data.
- tableDescription: the table description with RecordLayout.
Returns:
- RecordHeader struct containing the header fields extracted from the recordBytes.
- error if the recordBytes are too short to contain a valid header or if any header field is invalid.
*/
func GetRecordHeader(recordBytes []byte, tableDescription *db.TableDescription) (*RecordHeader, error) {
	header := &RecordHeader{}
	if len(recordBytes) < int(tableDescription.RecordLayout.HeaderSize) {
		return header, fmt.Errorf("record too short to contain header: got %d bytes, needs at least %d", len(recordBytes), tableDescription.RecordLayout.HeaderSize)
	}
	header.DeletedFlag = recordBytes[0]
	header.NextDeletedID = binary.LittleEndian.Uint32(recordBytes[1:5])
	header.Sets = make([]uint32, len(tableDescription.Sets))
	for i := 0; i < len(tableDescription.Sets); i++ {
		header.Sets[i] = binary.LittleEndian.Uint32(recordBytes[5+i*4 : 9+i*4])
	}
	return header, nil
}

/*
ConvertRecordHeaderToBytes converts a RecordHeader struct back to a byte slice that can be written to the file.
Parameters:
- header: the RecordHeader struct to convert.
Returns:
- byte slice containing the header fields in the correct format for writing to the file
- error if any header field is invalid or if the resulting byte slice would be too short.
*/
func ConvertRecordHeaderToBytes(header *RecordHeader) ([]byte, error) {
	headerBytes := make([]byte, 1+4+len(header.Sets)*4)
	headerBytes[0] = header.DeletedFlag
	binary.LittleEndian.PutUint32(headerBytes[1:5], header.NextDeletedID)
	for i, setBlockNo := range header.Sets {
		binary.LittleEndian.PutUint32(headerBytes[5+i*4:9+i*4], setBlockNo)
	}
	return headerBytes, nil
}

/* =============================== Time Representation ===============================
In JSON input records, time is represented as string showing the time from midnight, in one of these formats:
- "H:MM" - hours and minutes (seconds and milliseconds assumed to be 0)
- "H:MM:SS" - hours, minutes, and seconds (milliseconds assumed to be 0)
- "H:MM:SS.mmm" - full format with hours, minutes, seconds, and milliseconds

Where:
- H is hours (can be 1 or 2 digits)
- MM is minutes (always 2 digits, zero-padded)
- SS is seconds (always 2 digits, zero-padded)
- mmm is milliseconds (always 3 digits, zero-padded).

In memory, time is represented as a single uint64 value that counts the number of milliseconds since
midnight.
*/

// FormatMillis converts a time in milliseconds to the shortest time format, skipping trailing zeros.
// Returns "H:MM" if seconds and milliseconds are 0, "H:MM:SS" if only milliseconds are 0,
// or "H:MM:SS.mmm" for the full format.
func FormatMillis(t uint64) string {
	h := t / 3_600_000
	t %= 3_600_000

	m := t / 60_000
	t %= 60_000

	s := t / 1_000
	ms := t % 1_000

	// Return shortest format by skipping trailing zeros
	if s == 0 && ms == 0 {
		return fmt.Sprintf("%d:%02d", h, m)
	}
	if ms == 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%d:%02d:%02d.%03d", h, m, s, ms)
}

// ParseCompactTime validates a time string in flexible formats.
// Accepts "H:MM", "H:MM:SS", or "H:MM:SS.mmm" formats.
// Returns nil if the format is valid, or an error describing the problem.
func ParseCompactTime(s string) error {
	// Expected formats:
	// - H:MM (seconds and milliseconds default to 0)
	// - H:MM:SS (milliseconds default to 0)
	// - H:MM:SS.mmm (full format)
	// Hours may be 1–2 digits, everything else is fixed width.

	// Split on ':'
	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return fmt.Errorf("invalid time format: %q (expected H:MM, H:MM:SS, or H:MM:SS.mmm)", s)
	}

	// Parse hours
	_, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid hours in time %q: %w", s, err)
	}

	// Parse minutes
	_, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid minutes in time %q: %w", s, err)
	}

	// Parse seconds and milliseconds (if present)
	if len(parts) == 3 {
		// Check if there are milliseconds (contains '.')
		secParts := strings.SplitN(parts[2], ".", 2)
		_, err = strconv.ParseUint(secParts[0], 10, 64)
		if err != nil {
			return fmt.Errorf("invalid seconds in time %q: %w", s, err)
		}

		if len(secParts) == 2 {
			// Parse milliseconds
			_, err = strconv.ParseUint(secParts[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid milliseconds in time %q: %w", s, err)
			}
		}
	}
	// If len(parts) == 2, seconds and milliseconds default to 0 (H:MM format)

	return nil
}

// ConvertCompactTime parses a time string in flexible formats and converts it to milliseconds.
// Accepts "H:MM", "H:MM:SS", or "H:MM:SS.mmm" formats.
// Missing trailing parts (seconds, milliseconds) are assumed to be 0.
func ConvertCompactTime(s string) (uint64, error) {
	// Expected formats:
	// - H:MM (seconds and milliseconds default to 0)
	// - H:MM:SS (milliseconds default to 0)
	// - H:MM:SS.mmm (full format)
	// Hours may be 1–2 digits, everything else is fixed width.

	var h, m, s2, ms uint64

	// Split on ':'
	parts := strings.Split(s, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return 0, fmt.Errorf("invalid time format: %q (expected H:MM, H:MM:SS, or H:MM:SS.mmm)", s)
	}

	// Parse hours
	var err error
	h, err = strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid hours in time %q: %w", s, err)
	}

	// Parse minutes
	m, err = strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid minutes in time %q: %w", s, err)
	}

	// Parse seconds and milliseconds (if present)
	if len(parts) == 3 {
		// Check if there are milliseconds (contains '.')
		secParts := strings.SplitN(parts[2], ".", 2)
		s2, err = strconv.ParseUint(secParts[0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid seconds in time %q: %w", s, err)
		}

		if len(secParts) == 2 {
			// Parse milliseconds
			ms, err = strconv.ParseUint(secParts[1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid milliseconds in time %q: %w", s, err)
			}
		}
	}
	// If len(parts) == 2, s2 and ms remain 0 (H:MM format)

	/*
	   Go lets writing long numeric literals with _ inside them to improve readability:
	   - 3_600_000 is exactly the same value as 3600000
	   - 60_000 is exactly the same as 60000
	   - 1_000 is exactly the same as 1000
	*/
	return h*3_600_000 + m*60_000 + s2*1_000 + ms, nil
}

/* =============================== Date Representation ===============================
In JSON input records, date is represented as string "YYYY-MM-DD". Internally, date is represented
as a single uint64 value that counts the number of days since January 1 2000 (2000-01-01).
*/

// FormatDate converts a date in days since 2000-01-01 to the "YYYY-MM-DD" format.
func FormatDate(d uint64) string {
	// Calculate the date by adding d days to 2000-01-01
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	target := base.AddDate(0, 0, int(d))
	return target.Format("2006-01-02")
}

// ParseDate parses a date string in the "YYYY-MM-DD" format and converts it to days since 2000-01-01.
func ParseDate(s string) (uint64, error) {
	// Parse the date string
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return 0, err
	}
	// Calculate the number of days since 2000-01-01
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	days := t.Sub(base).Hours() / 24
	return uint64(days), nil
}

/* =============================== Decimal Representation ===============================
In JSON input records decimal numbers are represented as strings, e.g. "-123.045600". Internally
decimal numbers are represented as three integers: IntPart, FracPart, and Scale, along with a
boolean Neg to indicate if the number is negative. For example, the string "-123.045600" would be
represented as:
- IntPart: 123
- FracPart: 45600
- Scale: 5 (because there are 5 digits in the fractional part)
- Neg: true

https://pkg.go.dev/github.com/shopspring/decimal package can be used for decimal arithmetics.
*/

type Decimal struct {
	IntPart  uint64 // digits before decimal point
	FracPart uint64 // digits after decimal point (no leading zeros)
	Scale    uint8  // number of digits in FracPart
	Neg      bool   // true = negative
}

// IsDecimalString checks if a string is a valid decimal number in the expected format.
func IsDecimalString(s string) bool {
	if s == "" {
		return false
	}
	if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	parts := strings.SplitN(s, ".", 2)
	if len(parts) > 2 {
		return false
	}
	if _, err := strconv.ParseUint(parts[0], 10, 64); err != nil {
		return false
	}
	if len(parts) == 2 {
		if _, err := strconv.ParseUint(parts[1], 10, 64); err != nil {
			return false
		}
	}
	return true
}

// DecimalFromString parses a decimal string like "-123.045600" and constructs the Decimal representation.
func DecimalFromString(s string) (Decimal, error) {
	d := Decimal{}
	// sign
	if strings.HasPrefix(s, "-") {
		d.Neg = true
		s = s[1:]
	} else if strings.HasPrefix(s, "+") {
		s = s[1:]
	}
	parts := strings.SplitN(s, ".", 2)
	// integer part
	intPart, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return d, err
	}
	d.IntPart = intPart
	// fractional part
	if len(parts) == 2 {
		fracStr := parts[1]
		d.Scale = uint8(len(fracStr))

		if d.Scale > 0 {
			fracPart, err := strconv.ParseUint(fracStr, 10, 64)
			if err != nil {
				return d, err
			}
			d.FracPart = fracPart
		}
	}
	return d, nil
}

// String reconstructs the exact decimal string, preserving leading zeros in the fractional part.
func (d Decimal) String() string {
	if d.Scale == 0 {
		if d.Neg {
			return fmt.Sprintf("-%d", d.IntPart)
		}
		return fmt.Sprintf("%d", d.IntPart)
	}
	frac := fmt.Sprintf("%0*d", d.Scale, d.FracPart)
	if d.Neg {
		return fmt.Sprintf("-%d.%s", d.IntPart, frac)
	}
	return fmt.Sprintf("%d.%s", d.IntPart, frac)
}

// Float64 converts the decimal to float64. This is NOT exact, but useful for quick math.
func (d Decimal) Float64() float64 {
	v := float64(d.IntPart)
	if d.Scale > 0 {
		v += float64(d.FracPart) / math.Pow10(int(d.Scale))
	}
	if d.Neg {
		v = -v
	}
	return v
}
