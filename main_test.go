package main

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"

	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestJobMessageValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		message JobMessage
		wantErr string
	}{
		{
			name: "valid",
			message: JobMessage{
				JobID:          "job-1",
				Bucket:         "videos",
				SourceS3Key:    "uploads/job-1/input.mov",
				OutputS3Key:    "outputs/job-1.mp4",
				ThumbnailS3Key: "thumbnails/job-1.jpg",
			},
		},
		{
			name:    "all fields missing",
			message: JobMessage{},
			wantErr: "message missing required fields: jobId, bucket, sourceS3Key, outputS3Key, thumbnailS3Key",
		},
		{
			name: "only source missing",
			message: JobMessage{
				JobID:          "job-1",
				Bucket:         "videos",
				OutputS3Key:    "outputs/job-1.mp4",
				ThumbnailS3Key: "thumbnails/job-1.jpg",
			},
			wantErr: "message missing required fields: sourceS3Key",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.message.validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() error = %v, want nil", err)
				}
				return
			}

			if err == nil {
				t.Fatal("validate() error = nil, want error")
			}
			if err.Error() != tt.wantErr {
				t.Fatalf("validate() error = %q, want %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestHandleMessagePermanentValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "invalid JSON",
			body: "{not-json",
		},
		{
			name: "missing required field after bucket fallback",
			body: `{"jobId":"job-1","sourceS3Key":"uploads/job-1/input.mov","outputS3Key":"outputs/job-1.mp4"}`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := handleMessage(context.Background(), Config{VideoBucket: "videos"}, nil, nil, tt.body)
			if err == nil {
				t.Fatal("handleMessage() error = nil, want error")
			}

			var permanent permanentError
			if !errors.As(err, &permanent) {
				t.Fatalf("handleMessage() error type = %T, want permanentError", err)
			}
		})
	}
}

func TestApproximateReceiveCount(t *testing.T) {
	t.Parallel()

	key := string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount)

	tests := []struct {
		name       string
		attributes map[string]string
		want       int
	}{
		{name: "missing defaults to one", attributes: nil, want: 1},
		{name: "invalid defaults to one", attributes: map[string]string{key: "abc"}, want: 1},
		{name: "zero defaults to one", attributes: map[string]string{key: "0"}, want: 1},
		{name: "valid", attributes: map[string]string{key: "4"}, want: 4},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := approximateReceiveCount(tt.attributes); got != tt.want {
				t.Fatalf("approximateReceiveCount() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestPostgresURL(t *testing.T) {
	t.Parallel()

	got := postgresURL("video_app", "p@ss/word", "db.example.com", "5432", "videoprocessing", "require")
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatalf("postgresURL() produced invalid URL: %v", err)
	}

	if parsed.Scheme != "postgres" {
		t.Fatalf("scheme = %q, want postgres", parsed.Scheme)
	}
	if parsed.User.Username() != "video_app" {
		t.Fatalf("username = %q, want video_app", parsed.User.Username())
	}
	password, ok := parsed.User.Password()
	if !ok || password != "p@ss/word" {
		t.Fatalf("password = %q ok=%v, want p@ss/word true", password, ok)
	}
	if parsed.Host != "db.example.com:5432" {
		t.Fatalf("host = %q, want db.example.com:5432", parsed.Host)
	}
	if parsed.Path != "/videoprocessing" {
		t.Fatalf("path = %q, want /videoprocessing", parsed.Path)
	}
	if parsed.Query().Get("sslmode") != "require" {
		t.Fatalf("sslmode = %q, want require", parsed.Query().Get("sslmode"))
	}
}

func TestSecretAndStringHelpers(t *testing.T) {
	t.Parallel()

	secret := map[string]any{
		"username": "video_app",
		"port":     float64(5432),
		"enabled":  true,
		"nil":      nil,
	}

	if got := stringFromSecret(secret, "username"); got != "video_app" {
		t.Fatalf("stringFromSecret(username) = %q, want video_app", got)
	}
	if got := stringFromSecret(secret, "port"); got != "5432" {
		t.Fatalf("stringFromSecret(port) = %q, want 5432", got)
	}
	if got := stringFromSecret(secret, "enabled"); got != "true" {
		t.Fatalf("stringFromSecret(enabled) = %q, want true", got)
	}
	if got := stringFromSecret(secret, "nil"); got != "" {
		t.Fatalf("stringFromSecret(nil) = %q, want empty", got)
	}
	if got := stringFromSecret(secret, "missing"); got != "" {
		t.Fatalf("stringFromSecret(missing) = %q, want empty", got)
	}

	if got := firstNonEmpty("", "", "database-host", "fallback"); got != "database-host" {
		t.Fatalf("firstNonEmpty() = %q, want database-host", got)
	}
}

func TestTruncate(t *testing.T) {
	t.Parallel()

	if got := truncate("short", 10); got != "short" {
		t.Fatalf("truncate(short, 10) = %q, want short", got)
	}

	got := truncate("abcdef", 3)
	if got != "abc" {
		t.Fatalf("truncate(abcdef, 3) = %q, want abc", got)
	}

	long := strings.Repeat("x", 2000)
	if got := truncate(long, 1900); len(got) != 1900 {
		t.Fatalf("truncate(long, 1900) length = %d, want 1900", len(got))
	}
}
