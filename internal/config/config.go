package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	SourceBucket    string
	BronzeRoot      string
	BronzeVersion   string // "v1" or "v2"
	VersionBoundary int64  // ledger boundary
	Network         string
}

func FromEnv() (*Config, error) {
	boundary, err := parseInt64(os.Getenv("VERSION_BOUNDARY"))
	if err != nil {
		return nil, fmt.Errorf("invalid VERSION_BOUNDARY: %w", err)
	}

	return &Config{
		SourceBucket:    os.Getenv("SOURCE_BUCKET"),
		BronzeRoot:      os.Getenv("BRONZE_ROOT"),
		BronzeVersion:   os.Getenv("BRONZE_VERSION"),
		VersionBoundary: boundary,
		Network:         os.Getenv("NETWORK"),
	}, nil
}

func parseInt64(v string) (int64, error) {
	if v == "" {
		return 0, nil
	}
	return strconv.ParseInt(v, 10, 64)
}
