package cli

// ANSI color codes for terminal output
// Used to provide color-coded output for better readability
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorCyan   = "\033[36m"
	ColorBold   = "\033[1m"
)

// Helper functions for color formatting
func Red(s string) string {
	return ColorRed + s + ColorReset
}

func Green(s string) string {
	return ColorGreen + s + ColorReset
}

func Yellow(s string) string {
	return ColorYellow + s + ColorReset
}

func Blue(s string) string {
	return ColorBlue + s + ColorReset
}

func Cyan(s string) string {
	return ColorCyan + s + ColorReset
}

func Bold(s string) string {
	return ColorBold + s + ColorReset
}
