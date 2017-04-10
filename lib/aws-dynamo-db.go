package mpawsdynamodb

import (
	"errors"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	mp "github.com/mackerelio/go-mackerel-plugin-helper"
)

const (
	namespace              = "AWS/DynamoDB"
	metricsTypeAverage     = "Average"
	metricsTypeSum         = "Sum"
	metricsTypeMaximum     = "Maximum"
	metricsTypeMinimum     = "Minimum"
	metricsTypeSampleCount = "SampleCount"
)

type metrics struct {
	CloudWatchName string
	MackerelName   string
	Type           string
}

// DynamoDBPlugin mackerel plugin for aws kinesis
type DynamoDBPlugin struct {
	TableName string
	Prefix    string

	AccessKeyID     string
	SecretAccessKey string
	Region          string
	CloudWatch      *cloudwatch.CloudWatch
}

// MetricKeyPrefix interface for PluginWithPrefix
func (p DynamoDBPlugin) MetricKeyPrefix() string {
	if p.Prefix == "" {
		p.Prefix = "dynamodb"
	}
	return p.Prefix
}

// prepare creates CloudWatch instance
func (p *DynamoDBPlugin) prepare() error {
	sess, err := session.NewSession()
	if err != nil {
		return err
	}

	config := aws.NewConfig()
	if p.AccessKeyID != "" && p.SecretAccessKey != "" {
		config = config.WithCredentials(credentials.NewStaticCredentials(p.AccessKeyID, p.SecretAccessKey, ""))
	}
	if p.Region != "" {
		config = config.WithRegion(p.Region)
	}

	p.CloudWatch = cloudwatch.New(sess, config)

	return nil
}

// getLastPoint fetches a CloudWatch metric and parse
func (p DynamoDBPlugin) getLastPoint(metric metrics) (float64, error) {
	now := time.Now()

	dimensions := []*cloudwatch.Dimension{
		{
			Name:  aws.String("TableName"),
			Value: aws.String(p.TableName),
		},
	}

	response, err := p.CloudWatch.GetMetricStatistics(&cloudwatch.GetMetricStatisticsInput{
		Dimensions: dimensions,
		StartTime:  aws.Time(now.Add(time.Duration(180) * time.Second * -1)), // 3 min
		EndTime:    aws.Time(now),
		MetricName: aws.String(metric.CloudWatchName),
		Period:     aws.Int64(60),
		Statistics: []*string{aws.String(metric.Type)},
		Namespace:  aws.String(namespace),
	})
	if err != nil {
		return 0, err
	}

	datapoints := response.Datapoints
	if len(datapoints) == 0 {
		return 0, errors.New("fetched no datapoints")
	}

	latest := new(time.Time)
	var latestVal float64
	for _, dp := range datapoints {
		if dp.Timestamp.Before(*latest) {
			continue
		}

		latest = dp.Timestamp
		switch metric.Type {
		case metricsTypeAverage:
			latestVal = *dp.Average
		case metricsTypeSum:
			latestVal = *dp.Sum
		case metricsTypeMaximum:
			latestVal = *dp.Maximum
		case metricsTypeMinimum:
			latestVal = *dp.Minimum
		}
	}

	return latestVal, nil
}

// FetchMetrics fetch the metrics
func (p DynamoDBPlugin) FetchMetrics() (map[string]interface{}, error) {
	stat := make(map[string]interface{})

	for _, met := range [...]metrics{
		{CloudWatchName: "ConditionalCheckFailedRequests", MackerelName: "ConditionalCheckFailedRequests", Type: metricsTypeSum},
		{CloudWatchName: "ConsumedReadCapacityUnits", MackerelName: "ConsumedReadCapacityUnitsSum", Type: metricsTypeSum},
		{CloudWatchName: "ConsumedReadCapacityUnits", MackerelName: "ConsumedReadCapacityUnitsAverage", Type: metricsTypeAverage},
		{CloudWatchName: "ConsumedWriteCapacityUnits", MackerelName: "ConsumedWriteCapacityUnitsSum", Type: metricsTypeSum},
		{CloudWatchName: "ConsumedWriteCapacityUnits", MackerelName: "ConsumedWriteCapacityUnitsAverage", Type: metricsTypeAverage},
		{CloudWatchName: "ProvisionedReadCapacityUnits", MackerelName: "ProvisionedReadCapacityUnits", Type: metricsTypeMinimum},
		{CloudWatchName: "ProvisionedWriteCapacityUnits", MackerelName: "ProvisionedWriteCapacityUnits", Type: metricsTypeMinimum},
		{CloudWatchName: "ReadThrottleEvents", MackerelName: "ReadThrottleEvents", Type: metricsTypeSum},
		{CloudWatchName: "SuccessfulRequestLatency", MackerelName: "SuccessfulRequestLatencyMinimum", Type: metricsTypeMinimum},
		{CloudWatchName: "SuccessfulRequestLatency", MackerelName: "SuccessfulRequestLatencyMaximum", Type: metricsTypeMaximum},
		{CloudWatchName: "SuccessfulRequestLatency", MackerelName: "SuccessfulRequestLatencyAverage", Type: metricsTypeAverage},
		{CloudWatchName: "SuccessfulRequestLatency", MackerelName: "SuccessfulRequestLatencySampleCount", Type: metricsTypeSampleCount},
		{CloudWatchName: "SystemErrors", MackerelName: "SystemErrors", Type: metricsTypeSum},
		{CloudWatchName: "ThrottledRequests", MackerelName: "ThrottledRequests", Type: metricsTypeSum}, // can take Operation
		{CloudWatchName: "UserErrors", MackerelName: "UserErrors", Type: metricsTypeSum},
		{CloudWatchName: "WriteThrottleEvents", MackerelName: "WriteThrottleEvents", Type: metricsTypeSum},
	} {
		v, err := p.getLastPoint(met)
		if err == nil {
			stat[met.MackerelName] = v
		} else {
			log.Printf("%s: %s", met, err)
		}
	}
	return stat, nil
}

// GraphDefinition of DynamoDBPlugin
func (p DynamoDBPlugin) GraphDefinition() map[string]mp.Graphs {
	labelPrefix := strings.Title(p.Prefix)
	labelPrefix = strings.Replace(labelPrefix, "-", " ", -1)

	var graphdef = map[string]mp.Graphs{
		"ReadCapacity": {
			Label: (labelPrefix + " Read Capacity Units"),
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "ProvisionedReadCapacityUnits", Label: "Provisioned"},
				{Name: "ConsumedReadCapacityUnitsSum", Label: "Consumed (Sum)"},
				{Name: "ConsumedReadCapacityUnitsAverage", Label: "Consumed (Average)"},
			},
		},
		"WriteCapacity": {
			Label: (labelPrefix + " Write Capacity Units"),
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "ProvisionedWriteCapacityUnits", Label: "Provisioned"},
				{Name: "ConsumedWriteCapacityUnitsSum", Label: "Consumed (Sum)"},
				{Name: "ConsumedWriteCapacityUnitsAverage", Label: "Consumed (Average)"},
			},
		},
		"ThrottledEvents": {
			Label: (labelPrefix + " Throttle Events"),
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "ReadThrottleEvents", Label: "Read"},
				{Name: "WriteThrottleEvents", Label: "Write"},
			},
		},
		"Requests": {
			Label: (labelPrefix + " Requests"),
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "ConditionalCheckFailedRequests", Label: "ConditionalCheck Failure"},
				{Name: "SystemErrors", Label: "System Error"},
				{Name: "UserErrors", Label: "User Error"},
				{Name: "ThrottledRequests", Label: "Throttled"},
				{Name: "SuccessfulRequestLatencySampleCount", Label: "Success"},
			},
		},
		"SuccessfulRequestLatency": {
			Label: (labelPrefix + " Latency of Succesful Requests"),
			Unit:  "integer",
			Metrics: []mp.Metrics{
				{Name: "SuccessfulRequestLatencyAverage", Label: "Average"},
				{Name: "SuccessfulRequestLatencyMaximum", Label: "Maximum"},
				{Name: "SuccessfulRequestLatencyMinimum", Label: "Minimum"},
			},
		},
	}
	return graphdef
}

// Do the plugin
func Do() {
	optAccessKeyID := flag.String("access-key-id", "", "AWS Access Key ID")
	optSecretAccessKey := flag.String("secret-access-key", "", "AWS Secret Access Key")
	optRegion := flag.String("region", "", "AWS Region")
	optTableName := flag.String("table-name", "", "DynamoDB Table Name")
	optTempfile := flag.String("tempfile", "", "Temp file name")
	optPrefix := flag.String("metric-key-prefix", "dynamodb", "Metric key prefix")
	flag.Parse()

	var plugin DynamoDBPlugin

	plugin.AccessKeyID = *optAccessKeyID
	plugin.SecretAccessKey = *optSecretAccessKey
	plugin.Region = *optRegion
	plugin.TableName = *optTableName
	plugin.Prefix = *optPrefix

	err := plugin.prepare()
	if err != nil {
		log.Fatalln(err)
	}

	helper := mp.NewMackerelPlugin(plugin)
	helper.Tempfile = *optTempfile

	helper.Run()
}
