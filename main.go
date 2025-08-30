package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/lib/pq"
)

var totalCount int
var maxDuration time.Duration

var awsCfg = &awsConfig{}

const total = 10

type awsConfig struct {
	config       aws.Config
	endpoint     string
	usePathStyle bool
}
type Logger interface {
	Log(message string)
	Close()
	Logf(format string, v ...any)
	CreateLogFile(dir string, file string) (*os.File, error)
}

type FileLogger struct {
	tmpFile  *os.File
	i        int
	parallel int
}
type LogStatus struct {
	Count    int
	Duration time.Duration
	HasErr   bool
	End      bool
	File     string
}

type SlackPayload struct {
	Channel     string       `json:"channel"`
	Mrkdwn      bool         `json:"mrkdwn"`
	Username    string       `json:"username"`
	Attachments []Attachment `json:"attachments"`
	IconEmoji   string       `json:"icon_emoji"`
}

type Attachment struct {
	MrkdwnIn []string `json:"mrkdwn_in"`
	Fallback string   `json:"fallback"`
	Title    string   `json:"title"`
	Text     string   `json:"text"`
	Color    string   `json:"color"`
	Footer   string   `json:"footer"`
	TS       string   `json:"ts"`
}

func (f FileLogger) Close() {
	err := f.tmpFile.Close()
	if err != nil {
		return
	}
}
func (f FileLogger) Log(message string) {
	if _, err := f.tmpFile.WriteString(message + "\n"); err != nil {
		log.Printf("write failed: %v", err)
	}
	var cmsg string
	if len(message) > 22 {
		cmsg = fmt.Sprintf(": Batch %d/%d : %s", f.i+1, f.parallel, message[22:])
	} else {
		cmsg = fmt.Sprintf(": Batch %d/%d : %s", f.i+1, f.parallel, message)
	}
	log.Println(cmsg)
}
func NewFileLogger(name string, i int, parallel int, tm string, lb int, up int, logs *LogStatus) (*FileLogger, error) {
	f, err := os.CreateTemp("", getTempFilename(name, tm, lb, up))
	if err != nil {
		return nil, err
	}
	logs.File = f.Name()

	return &FileLogger{
		tmpFile:  f,
		i:        i,
		parallel: parallel,
	}, nil
}
func (f FileLogger) Logf(format string, v ...any) {
	message := fmt.Sprintf(format, v)
	if _, err := f.tmpFile.WriteString(message + "\n"); err != nil {
		log.Printf("write failed: %v", err)
	}
	var cmsg string
	if len(message) > 22 {
		cmsg = fmt.Sprintf(": Batch %d/%d : %s", f.i+1, f.parallel, message[22:])
	} else {
		cmsg = fmt.Sprintf(": Batch %d/%d : %s", f.i+1, f.parallel, message)
	}
	log.Println(cmsg)

}
func (c FileLogger) CreateLogFile(dir string, file string) (*os.File, error) {
	tmp, err := os.CreateTemp(dir, file)
	if err != nil {
		return nil, err
	}
	return tmp, nil
}

type Service struct {
	logger Logger
}

func NewService(logger Logger) *Service {
	return &Service{logger: logger}
}
func (s *Service) Println(txt string) {
	if s != nil {
		s.logger.Log(txt)
	} else {
		log.Println(txt)
	}

}
func (s *Service) PrintlnWithTime(txt string) {
	if s != nil {
		s.logger.Log(fmt.Sprintf("%s : %s", time.Now().Format("2006-01-02 15:04:05"), txt))
	} else {
		log.Println(txt)
	}

}
func (s *Service) Printf(format string, v ...any) {
	if s != nil {
		s.logger.Logf(format, v)
	} else {
		log.Printf(format, v)
	}
}
func (s *Service) Close() {
	if s != nil {
		s.logger.Close()
	}

}

func (s *Service) CreateLogFile(dir string, file string) (*os.File, error) {
	return s.logger.CreateLogFile(dir, file)
}

type s3file struct {
	Bucket string
	Key    string
}

type Event struct {
	Parallel   int        `json:"parallel"`
	SqlFile    string     `header:"sqlFile"`
	LogPrefix  string     `header:"logPrefix"`
	Name       string     `json:"name"`
	S3Log      bool       `json:"s3Log"`
	SqlBucket  Bucket     `json:"sqlBucket"`
	LogBucket  Bucket     `json:"logBucket"`
	Slack      Slack      `json:"slack"`
	Connection Connection `json:"connection"`
}
type Bucket struct {
	Name string `json:"name"`
	Key  string `json:"folder"`
}
type Slack struct {
	Url     string `json:"url"`
	Channel string `json:"channel"`
}
type Connection struct {
	Host         string `json:"host"`
	Port         string `json:"port"`
	User         string `json:"user"`
	Password     string `json:"password"`
	DatabaseName string `json:"databaseName"`
	Force        bool   `json:"force"`
}

func (e *Event) Init() error {
	if e.Connection.Port == "" {
		e.Connection.Port = "5432"
	}

	if e.Connection.Host == "" {
		log.Println("host field is required")
		return errors.New("missing host")
	}
	if e.Connection.User == "" {
		log.Println("user not set,defaulting to postgres")
		e.Connection.User = "postgres"
	}
	if e.Connection.Password == "" {
		log.Println("password field is required")
		return errors.New("missing password")
	}
	if e.Name == "" {
		filename := filepath.Base(e.SqlFile)
		ext := filepath.Ext(filename)
		e.Name = filename[:len(filename)-len(ext)]
	}
	return nil

}
func handler(ctx context.Context, event Event) (string, error) {
	loadAwsConfig(ctx)
	ssmClient, _ := getssmClient(ctx)
	event.Connection.Host = getParameter(ctx, ssmClient, event.Connection.Host, true, event.Connection.Force)
	event.Connection.Port = getParameter(ctx, ssmClient, event.Connection.Port, true, event.Connection.Force)
	event.Connection.User = getParameter(ctx, ssmClient, event.Connection.User, true, event.Connection.Force)
	event.Connection.Password = getParameter(ctx, ssmClient, event.Connection.Password, true, event.Connection.Force)

	log.Println("Process started")
	if err := event.Init(); err != nil {
		return "", err
	}

	if event.Parallel < 1 {
		event.Parallel = 1
	}
	if event.Parallel > 10 {
		event.Parallel = 10
	}

	sqlBlock, err := getSqlFromS3(ctx, &event)
	if err != nil {
		log.Println(fmt.Sprintf("Cannot get sql from %s", event.SqlFile))
		log.Println(err.Error())
		return "", err

	}

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		event.Connection.Host, event.Connection.Port, event.Connection.User, event.Connection.Password, event.Connection.DatabaseName)

	_, err = runParallelSQL(ctx, connStr, sqlBlock, &event)
	if err != nil {
		return "", nil
	}

	log.Println("Process completed")
	return "Success", nil
}
func getParameter(ctx context.Context, client *ssm.Client, name string, decrypt bool, force bool) string {
	out, err := client.GetParameter(ctx, &ssm.GetParameterInput{
		Name:           &name,
		WithDecryption: &decrypt,
	})
	if err != nil {
		if force {
			log.Printf("Warning : Failed to get parameter %s: %v", name, err)
			return name
		} else {
			log.Fatalf("Error : Failed to get parameter %s: %v", name, err)
		}

	}
	return *out.Parameter.Value
}

func loadAwsConfig(ctx context.Context) {
	var errcfg error

	awsCfg.config, errcfg = config.LoadDefaultConfig(ctx)
	if errcfg != nil {
		log.Fatalf("Error : Failed to load aws parameter : %v", errcfg)
	}
	awsCfg.endpoint = os.Getenv("AWS_ENDPOINT_URL")
	awsCfg.usePathStyle, _ = strconv.ParseBool(os.Getenv("AWS_USE_PATH_STYLE"))

}

func getS3Client(ctx context.Context) (*s3.Client, error) {
	s3Client := s3.NewFromConfig(awsCfg.config, func(o *s3.Options) {
		o.EndpointResolver = s3.EndpointResolverFromURL(awsCfg.endpoint)
		o.UsePathStyle = awsCfg.usePathStyle
	})
	return s3Client, nil
}
func getssmClient(ctx context.Context) (*ssm.Client, error) {
	ssmClient := ssm.NewFromConfig(awsCfg.config, func(o *ssm.Options) {
		o.EndpointResolver = ssm.EndpointResolverFromURL(awsCfg.endpoint)
	})
	return ssmClient, nil
}
func getSqlFromS3(ctx context.Context, event *Event) (string, error) {
	prefix := event.SqlBucket.Key
	if prefix != "" {
		prefix += "/"
	}
	file := s3file{Bucket: event.SqlBucket.Name, Key: fmt.Sprintf("%s%s", prefix, event.SqlFile)}

	client, err := getS3Client(ctx)
	if err != nil {
		return "AWS Config not loaded.", err
	}
	getObj, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &file.Bucket,
		Key:    &file.Key,
	})
	if err != nil {
		return "S3 file could not be retrieved", err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {

		}
	}(getObj.Body)

	bodyBytes, err := io.ReadAll(getObj.Body)
	if err != nil {
		return "S3 file could not be read", err
	}
	sqlContent := string(bodyBytes)

	return sqlContent, nil
}

func runParallelSQL(ctx context.Context, connStr string, sql string, event *Event) (string, error) {
	tm := time.Now().Format("20060102T150405") + fmt.Sprintf("%03d", time.Now().Nanosecond()/1e6)
	var wg sync.WaitGroup

	chunkSize := total / event.Parallel
	remainder := total % event.Parallel
	uniqID := fmt.Sprintf("%d", event.Parallel)
	date := time.Now().Format("20060102T150405000")
	mergedLog := fmt.Sprintf("%s_%s_%s.log", event.LogPrefix, uniqID, date)
	lb := 0
	logFiles := make([]string, 0)
	Log := make([]LogStatus, event.Parallel)
	for i := 0; i < event.Parallel; i++ {
		extra := 0
		if i < remainder {
			extra = 1
		}
		count := chunkSize + extra
		up := lb + count - 1
		logFile := fmt.Sprintf("%s_%s_%d_%d_%d_%s.log", event.LogPrefix, uniqID, i, lb, up, date)
		logFiles = append(logFiles, logFile)

		wg.Add(1)
		go func(lb, up int, logFile string) {
			defer wg.Done()

			var logService *Service

			if event.S3Log {
				logger, err := NewFileLogger(event.Name, i, event.Parallel, tm, lb, up, &Log[i])
				if err != nil {
					log.Fatalf("[Worker %d-%d] Temp file error: %v", lb, up, err)
				}
				logService = NewService(logger)
			}

			logService.PrintlnWithTime("Started")

			parseConfig, err := pgx.ParseConfig(connStr)
			if err != nil {
				log.Fatalf("parseConfig parse failed: %v", err)
			}
			parseConfig.RuntimeParams["client_min_messages"] = "info"
			parseConfig.Config.OnNotice = func(c *pgconn.PgConn, n *pgconn.Notice) {

				if !Log[i].End {
					if strings.Contains(n.Message, "Completed") {
						Log[i].End = true
					} else {
						reCount := regexp.MustCompile(`:\s*(\d+)\s+\w+\(s\)`)
						countMatch := reCount.FindStringSubmatch(n.Message)
						if len(countMatch) >= 2 {
							count, _ := strconv.Atoi(countMatch[1])
							Log[i].Count += count
						}

						reDuration := regexp.MustCompile(`Duration\s*:\s*([0-9]{2}):([0-9]{2}):([0-9]{2}).([0-9]+)?`)
						durMatch := reDuration.FindStringSubmatch(n.Message)
						if len(durMatch) == 5 {
							hh, _ := strconv.Atoi(durMatch[1])
							mm, _ := strconv.Atoi(durMatch[2])
							ss, _ := strconv.Atoi(durMatch[3])
							ms, _ := strconv.Atoi(durMatch[4])

							Log[i].Duration += +time.Duration(hh)*time.Hour + time.Duration(mm)*time.Minute + time.Duration(ss)*time.Second + time.Duration(ms)*time.Microsecond
						}
					}
				}
				logService.Println(fmt.Sprintf("%s", n.Message))
			}

			conn, err := pgx.ConnectConfig(ctx, parseConfig)
			if err != nil {
				log.Fatal("connect:", err)
			}
			defer func(conn *pgx.Conn, ctx context.Context) {
				err := conn.Close(ctx)
				if err != nil {
					return
				}
			}(conn, ctx)

			_, err = conn.Exec(ctx, "set time zone 'Europe/Istanbul';")
			if err != nil {
				return
			}
			sql := strings.ReplaceAll(strings.ReplaceAll(sql, "@lb@", strconv.Itoa(lb)), "@up@", strconv.Itoa(up))
			_, err = conn.Exec(ctx, sql)
			if err != nil {
				Log[i].HasErr = true
				Log[i].End = true
				logService.Println("Not completed, but records processed before the error were successfully committed.")
				logService.Printf("write failed: %v", err)
				logService.Close()
				return
			}
			logService.Close()
		}(lb, up, logFile)

		lb = up + 1
	}

	wg.Wait()

	file := mergeLog(event.Name, tm, &Log, event.S3Log)
	if file != nil {
		prefix := event.LogBucket.Key
		if prefix != "" {
			prefix += "/"
		}
		event.LogBucket.Key = fmt.Sprintf("%s%s_%d_%s.log", prefix, event.Name, total, tm)
		if err := uploadToS3FromFile(ctx, event, file.Name()); err != nil {
			log.Printf("upload error: %v", err)
		}
	}
	err := notify(event)
	if err != nil {
		return "", err
	}
	return mergedLog, nil
}

func notify(event *Event) error {

	fallback := fmt.Sprintf(
		"Archive for [%s]\nStatus : %s\nDuration : %s\n\nParallel : %d\nDatabase : %s", event.Name,
		"Success", maxDuration.String(), event.Parallel, event.Connection.Host)

	attachment := Attachment{
		MrkdwnIn: []string{"text", "fallback"},
		Fallback: fallback,
		Title:    fmt.Sprintf("Archive Summary   :white_check_mark:"),
		Text:     fallback,
		Color:    "#00b300",
		Footer:   "Source Time",
		TS:       fmt.Sprintf("%d", time.Now().Unix()),
	}
	payload := SlackPayload{
		Channel:     "#db-backup",
		Mrkdwn:      true,
		Username:    "Cepte Sok",
		Attachments: []Attachment{attachment},
		IconEmoji:   ":elephant:",
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := http.Post(event.Slack.Url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Slack API error: %s", resp.Status)
	}

	return nil
}
func getTempFilename(name string, tm string, lb int, up int) string {

	return fmt.Sprintf("%s_%d_%d_", getTemp(name, tm), lb, up)
}

func getTemp(name string, tm string) string {
	return fmt.Sprintf("tmp_%s_%d_%s", name, total, tm)
}
func mergeLog(name string, tm string, logs *[]LogStatus, s3log bool) *os.File {
	tmp := getTemp(name, tm)

	var tmpfile *os.File
	var err error
	if s3log {
		tmpfile, err = os.CreateTemp("", fmt.Sprintf("%s.log", tmp))
		if err != nil {
			log.Fatalf("Temp file error: %v", err)
		}
		defer func(name string) {
			err := tmpfile.Close()
			if err != nil {

			}
		}(tmpfile.Name())
	}
	for _, lg := range *logs {
		if s3log {
			_, err = tmpfile.WriteString(fmt.Sprintf("%s %s\n\n", time.Now().Format("2006-01-02 15:04:05 :"), lg.File))
			if err != nil {
				return nil
			}

			in, err := os.Open(lg.File)
			if err != nil {
				log.Printf("Failed to open %s: %v", lg, err)
				continue
			}
			_, err = io.Copy(tmpfile, in)
			in.Close()
			if err != nil {
				fmt.Printf("Write error: %v\n", err)
			}

			err = os.Remove(in.Name())
			if err != nil {
				log.Printf("Failed to delete %s: %v", lg, err)
			}
			_, err = tmpfile.WriteString("\n")
			if err != nil {
				return nil
			}
		}
		totalCount += lg.Count

		if lg.Duration > maxDuration {
			maxDuration = lg.Duration
		}
	}

	if s3log {
		_, err = tmpfile.WriteString(fmt.Sprintf("\nTotal Count: %d\nTotal Duration: %02d:%02d:%02d.%03d", totalCount,
			int(maxDuration.Hours()),
			int(maxDuration.Minutes())%60,
			int(maxDuration.Seconds())%60,
			int(maxDuration.Microseconds())%1000000))
		if err != nil {
			return nil
		}
	}
	log.Printf("Total Count: %d", totalCount)
	log.Printf("Total Duration: %s", maxDuration.String())
	return tmpfile
}
func uploadToS3FromFile(ctx context.Context, event *Event, filePath string) error {

	client, err := getS3Client(ctx)
	if err != nil {

		return err

	}

	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open temp file: %w", err)
	}

	defer func(name string) {
		err := f.Close()
		if err != nil {

		}
		err = os.Remove(name)
		if err != nil {

		}
	}(f.Name())

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(event.LogBucket.Name),
		Key:    aws.String(event.LogBucket.Key),
		Body:   f,
	})
	if err != nil {
		return fmt.Errorf("s3 upload error: %w", err)
	}

	return nil
}

func main() {

	lambda.Start(handler)
	//return
	//event := Event{
	//	Parallel:  10,
	//	SqlFile:   "order_archive_daily.sql",
	//	LogPrefix: "order_archive_daily",
	//	SqlBucket: Bucket{
	//		Name: "scripts",
	//		//Key:  "zzz",
	//	},
	//	LogBucket: Bucket{
	//		Name: "scriptlog",
	//		Key:  "abc",
	//	},
	//	Slack: Slack{
	//		Url:     "https://hooks.slack.com/services/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",
	//		Channel: "new-channel",
	//	},
	//	Connection: Connection{
	//		Host:         "host.docker.internal",
	//		Port:         "5432",
	//		DatabaseName: "destination_db",
	//		User:         "postgres",
	//		Password:     "1",
	//		Force:        true,
	//	},
	//	S3Log: true,
	//}
	//event := Event{
	//	Parallel:  10,
	//	SqlFile:   "order_archive_daily.sql",
	//	LogPrefix: "order_archive_daily",
	//	SqlBucket: Bucket{
	//		Name: "scripts",
	//		//Key:  "zzz",
	//	},
	//	LogBucket: Bucket{
	//		Name: "logs",
	//		//Key:  "abc",
	//	},
	//	Slack: Slack{
	//		Url:     "https://hooks.slack.com/services/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",
	//		Channel: "new-channel",
	//	},
	//	Connection: Connection{
	//		Host:         "/ceptesok/fulfillment/qa/aurora_postgres/cluster_endpoint",
	//		Port:         "/ceptesok/fulfillment/qa/aurora_postgres/cluster_port",
	//		User:         "/ceptesok/fulfillment/qa/aurora_postgres/cluster_master_username",
	//		Password:     "/ceptesok/fulfillment/qa/aurora_postgres/cluster_master_password",
	//		DatabaseName: "destination_db",
	//	},
	//	S3Log: true,
	//}
	//handler(context.Background(), event)
}
