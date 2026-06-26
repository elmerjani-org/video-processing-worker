package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type Config struct {
	Region                   string
	QueueURL                 string
	VideoBucket              string
	DatabaseSecretARN        string
	DatabaseHost             string
	DatabasePort             string
	DatabaseName             string
	DatabaseSSLMode          string
	VisibilityTimeoutSeconds int32
	MaxReceiveCount          int
	ReceiveWaitSeconds       int32
}

type JobMessage struct {
	JobID          string `json:"jobId"`
	Bucket         string `json:"bucket"`
	SourceS3Key    string `json:"sourceS3Key"`
	OutputS3Key    string `json:"outputS3Key"`
	ThumbnailS3Key string `json:"thumbnailS3Key"`
}

type permanentError struct {
	err error
}

func (e permanentError) Error() string {
	return e.err.Error()
}

func (e permanentError) Unwrap() error {
	return e.err
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := loadConfig()

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(cfg.Region))
	if err != nil {
		log.Fatalf("load AWS config: %v", err)
	}

	sqsClient := sqs.NewFromConfig(awsCfg)
	s3Client := s3.NewFromConfig(awsCfg)
	secretsClient := secretsmanager.NewFromConfig(awsCfg)

	db, err := openDB(ctx, cfg, secretsClient)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer db.Close()

	log.Printf("worker started region=%s queue=%s visibility_timeout=%ds", cfg.Region, cfg.QueueURL, cfg.VisibilityTimeoutSeconds)

	for ctx.Err() == nil {
		resp, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    &cfg.QueueURL,
			MaxNumberOfMessages:         1,
			WaitTimeSeconds:             cfg.ReceiveWaitSeconds,
			VisibilityTimeout:           cfg.VisibilityTimeoutSeconds,
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameApproximateReceiveCount},
		})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("receive message: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		for _, msg := range resp.Messages {
			if msg.Body == nil || msg.ReceiptHandle == nil {
				log.Printf("received malformed SQS message without body or receipt handle")
				continue
			}

			stopExtender := make(chan struct{})
			go extendMessageVisibility(ctx, sqsClient, cfg.QueueURL, *msg.ReceiptHandle, cfg.VisibilityTimeoutSeconds, stopExtender)

			err := handleMessage(ctx, cfg, db, s3Client, *msg.Body)
			close(stopExtender)

			if err != nil {
				receiveCount := approximateReceiveCount(msg.Attributes)
				shouldDelete := false

				var permanent permanentError
				if errors.As(err, &permanent) {
					log.Printf("permanent job failure: %v", err)
					shouldDelete = markFailedFromMessage(ctx, db, *msg.Body, err)
				} else if receiveCount >= cfg.MaxReceiveCount {
					log.Printf("job failed after %d receives: %v", receiveCount, err)
					shouldDelete = markFailedFromMessage(ctx, db, *msg.Body, err)
				} else {
					log.Printf("job failed, leaving message for retry receive_count=%d: %v", receiveCount, err)
				}

				if !shouldDelete {
					continue
				}
			}

			if err := deleteMessage(ctx, sqsClient, cfg.QueueURL, *msg.ReceiptHandle); err != nil {
				log.Printf("delete message: %v", err)
			}
		}
	}

	log.Println("worker stopped")
}

func loadConfig() Config {
	return Config{
		Region:                   getenv("AWS_REGION", "us-east-1"),
		QueueURL:                 mustGetenv("VIDEO_JOBS_QUEUE_URL"),
		VideoBucket:              mustGetenv("VIDEO_BUCKET"),
		DatabaseSecretARN:        mustGetenv("DATABASE_SECRET_ARN"),
		DatabaseHost:             os.Getenv("DATABASE_HOST"),
		DatabasePort:             getenv("DATABASE_PORT", "5432"),
		DatabaseName:             getenv("DATABASE_NAME", "videoprocessing"),
		DatabaseSSLMode:          getenv("DATABASE_SSLMODE", "require"),
		VisibilityTimeoutSeconds: int32(getenvInt("WORKER_VISIBILITY_TIMEOUT_SECONDS", 3600)),
		MaxReceiveCount:          getenvInt("WORKER_MAX_RECEIVE_COUNT", 3),
		ReceiveWaitSeconds:       int32(getenvInt("WORKER_RECEIVE_WAIT_SECONDS", 20)),
	}
}

func handleMessage(ctx context.Context, cfg Config, db *sql.DB, s3Client *s3.Client, body string) error {
	var jm JobMessage
	if err := json.Unmarshal([]byte(body), &jm); err != nil {
		return permanentError{err: fmt.Errorf("bad message JSON: %w", err)}
	}

	if jm.Bucket == "" {
		jm.Bucket = cfg.VideoBucket
	}
	if err := jm.validate(); err != nil {
		return permanentError{err: err}
	}

	status, err := currentJobStatus(ctx, db, jm.JobID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return permanentError{err: fmt.Errorf("job %s not found", jm.JobID)}
		}
		return fmt.Errorf("read job status: %w", err)
	}
	if status == "COMPLETED" {
		log.Printf("job already completed job_id=%s", jm.JobID)
		return nil
	}

	if err := markProcessing(ctx, db, jm.JobID); err != nil {
		return fmt.Errorf("mark processing: %w", err)
	}

	tmpDir, err := os.MkdirTemp("", "video-job-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	inputPath := filepath.Join(tmpDir, "input")
	outputPath := filepath.Join(tmpDir, "output.mp4")
	thumbPath := filepath.Join(tmpDir, "thumbnail.jpg")

	log.Printf("downloading source job_id=%s bucket=%s key=%s", jm.JobID, jm.Bucket, jm.SourceS3Key)
	if err := downloadFromS3(ctx, s3Client, jm.Bucket, jm.SourceS3Key, inputPath); err != nil {
		return fmt.Errorf("download source: %w", err)
	}

	log.Printf("processing video job_id=%s", jm.JobID)
	if err := runFFmpeg(ctx, inputPath, outputPath, thumbPath); err != nil {
		return permanentError{err: err}
	}

	log.Printf("uploading output job_id=%s output=%s thumbnail=%s", jm.JobID, jm.OutputS3Key, jm.ThumbnailS3Key)
	if err := uploadToS3(ctx, s3Client, jm.Bucket, jm.OutputS3Key, outputPath, "video/mp4"); err != nil {
		return fmt.Errorf("upload output: %w", err)
	}
	if err := uploadToS3(ctx, s3Client, jm.Bucket, jm.ThumbnailS3Key, thumbPath, "image/jpeg"); err != nil {
		return fmt.Errorf("upload thumbnail: %w", err)
	}

	if err := markCompleted(ctx, db, jm.JobID, jm.OutputS3Key, jm.ThumbnailS3Key); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}

	log.Printf("job completed job_id=%s", jm.JobID)
	return nil
}

func (m JobMessage) validate() error {
	missing := make([]string, 0)
	if m.JobID == "" {
		missing = append(missing, "jobId")
	}
	if m.Bucket == "" {
		missing = append(missing, "bucket")
	}
	if m.SourceS3Key == "" {
		missing = append(missing, "sourceS3Key")
	}
	if m.OutputS3Key == "" {
		missing = append(missing, "outputS3Key")
	}
	if m.ThumbnailS3Key == "" {
		missing = append(missing, "thumbnailS3Key")
	}
	if len(missing) > 0 {
		return fmt.Errorf("message missing required fields: %s", strings.Join(missing, ", "))
	}
	return nil
}

func openDB(ctx context.Context, cfg Config, secretsClient *secretsmanager.Client) (*sql.DB, error) {
	secret, err := getDatabaseSecret(ctx, secretsClient, cfg.DatabaseSecretARN)
	if err != nil {
		return nil, err
	}

	username := stringFromSecret(secret, "username")
	password := stringFromSecret(secret, "password")
	host := firstNonEmpty(cfg.DatabaseHost, stringFromSecret(secret, "host"))
	port := firstNonEmpty(cfg.DatabasePort, stringFromSecret(secret, "port"))
	dbName := firstNonEmpty(cfg.DatabaseName, stringFromSecret(secret, "dbname"))

	if username == "" || password == "" || host == "" || port == "" || dbName == "" {
		return nil, fmt.Errorf("database secret/config is missing username, password, host, port, or dbname")
	}

	dsn := postgresURL(username, password, host, port, dbName, cfg.DatabaseSSLMode)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func getDatabaseSecret(ctx context.Context, client *secretsmanager.Client, arn string) (map[string]any, error) {
	out, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: &arn,
	})
	if err != nil {
		return nil, fmt.Errorf("get database secret: %w", err)
	}
	if out.SecretString == nil {
		return nil, fmt.Errorf("database secret has no string value")
	}

	var secret map[string]any
	if err := json.Unmarshal([]byte(*out.SecretString), &secret); err != nil {
		return nil, fmt.Errorf("parse database secret JSON: %w", err)
	}
	return secret, nil
}

func currentJobStatus(ctx context.Context, db *sql.DB, jobID string) (string, error) {
	var status string
	err := db.QueryRowContext(ctx, `select status from video_jobs where id=$1`, jobID).Scan(&status)
	return status, err
}

func markProcessing(ctx context.Context, db *sql.DB, jobID string) error {
	_, err := db.ExecContext(ctx,
		`update video_jobs set status='PROCESSING', error_message=null, updated_at=now() where id=$1 and status <> 'COMPLETED'`,
		jobID,
	)
	return err
}

func markCompleted(ctx context.Context, db *sql.DB, jobID, outputKey, thumbnailKey string) error {
	_, err := db.ExecContext(ctx,
		`update video_jobs
		 set status='COMPLETED',
		     output_s3_key=$2,
		     thumbnail_s3_key=$3,
		     error_message=null,
		     updated_at=now()
		 where id=$1`,
		jobID, outputKey, thumbnailKey,
	)
	return err
}

func markFailedFromMessage(ctx context.Context, db *sql.DB, body string, cause error) bool {
	var jm JobMessage
	if err := json.Unmarshal([]byte(body), &jm); err != nil || jm.JobID == "" {
		return true
	}

	_, err := db.ExecContext(ctx,
		`update video_jobs set status='FAILED', error_message=$2, updated_at=now() where id=$1`,
		jm.JobID,
		truncate(cause.Error(), 1900),
	)
	if err != nil {
		log.Printf("mark failed job_id=%s: %v", jm.JobID, err)
		return false
	}
	return true
}

func downloadFromS3(ctx context.Context, s3Client *s3.Client, bucket, key, outPath string) error {
	out, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return err
	}
	defer out.Body.Close()

	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, out.Body)
	return err
}

func uploadToS3(ctx context.Context, s3Client *s3.Client, bucket, key, path, contentType string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &bucket,
		Key:         &key,
		Body:        f,
		ContentType: &contentType,
	})
	return err
}

func runFFmpeg(ctx context.Context, inputPath, outputPath, thumbPath string) error {
	transcode := exec.CommandContext(ctx,
		"ffmpeg",
		"-hide_banner",
		"-loglevel", "warning",
		"-y",
		"-i", inputPath,
		"-map", "0:v:0",
		"-map", "0:a?",
		"-c:v", "libx264",
		"-preset", "veryfast",
		"-crf", "23",
		"-c:a", "aac",
		"-movflags", "+faststart",
		outputPath,
	)
	if out, err := transcode.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg transcode failed: %w output=%s", err, truncate(string(out), 1400))
	}

	thumbnail := exec.CommandContext(ctx,
		"ffmpeg",
		"-hide_banner",
		"-loglevel", "warning",
		"-y",
		"-ss", "00:00:01",
		"-i", inputPath,
		"-frames:v", "1",
		"-q:v", "2",
		thumbPath,
	)
	if out, err := thumbnail.CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg thumbnail failed: %w output=%s", err, truncate(string(out), 1400))
	}

	return nil
}

func extendMessageVisibility(ctx context.Context, client *sqs.Client, queueURL, receiptHandle string, timeoutSeconds int32, stop <-chan struct{}) {
	interval := time.Duration(timeoutSeconds) * time.Second / 2
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			_, err := client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          &queueURL,
				ReceiptHandle:     &receiptHandle,
				VisibilityTimeout: timeoutSeconds,
			})
			if err != nil {
				log.Printf("extend message visibility: %v", err)
			}
		}
	}
}

func deleteMessage(ctx context.Context, client *sqs.Client, queueURL, receiptHandle string) error {
	_, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: &receiptHandle,
	})
	return err
}

func approximateReceiveCount(attributes map[string]string) int {
	v := attributes[string(sqstypes.MessageSystemAttributeNameApproximateReceiveCount)]
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 1
	}
	return n
}

func postgresURL(username, password, host, port, dbName, sslMode string) string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(username, password),
		Host:   net.JoinHostPort(host, port),
		Path:   dbName,
	}
	q := u.Query()
	q.Set("sslmode", sslMode)
	u.RawQuery = q.Encode()
	return u.String()
}

func stringFromSecret(secret map[string]any, key string) string {
	value, ok := secret[key]
	if !ok || value == nil {
		return ""
	}

	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		return strconv.FormatInt(int64(typed), 10)
	default:
		return fmt.Sprint(typed)
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func truncate(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}

func mustGetenv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("%s is required", key)
	}
	return value
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func getenvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("%s must be an integer: %v", key, err)
	}
	return parsed
}
