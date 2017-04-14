package mpawsdynamodb

import (
	"flag"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
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

// has 1 CloudWatch MetricName and corresponding N Mackerel Metrics
type metricsGroup struct {
	CloudWatchName string
	Metrics        []metric
}

type metric struct {
	MackerelName string
	Type         string
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
func getLastPointFromCloudWatch(cw cloudwatchiface.CloudWatchAPI, tableName string, metric metricsGroup) (*cloudwatch.Datapoint, error) {
	now := time.Now()
	statsInput := make([]*string, len(metric.Metrics))
	for i, typ := range metric.Metrics {
		statsInput[i] = aws.String(typ.Type)
	}
	input := &cloudwatch.GetMetricStatisticsInput{
		// 8 min, since some metrics are aggregated over 5 min
		StartTime:  aws.Time(now.Add(time.Duration(480) * time.Second * -1)),
		EndTime:    aws.Time(now),
		MetricName: aws.String(metric.CloudWatchName),
		Period:     aws.Int64(60),
		Statistics: statsInput,
		Namespace:  aws.String(namespace),
	}
	input.Dimensions = []*cloudwatch.Dimension{{
		Name:  aws.String("TableName"),
		Value: aws.String(tableName),
	}}
	response, err := cw.GetMetricStatistics(input)
	if err != nil {
		return nil, err
	}

	datapoints := response.Datapoints
	if len(datapoints) == 0 {
		return nil, nil
	}

	latest := new(time.Time)
	var latestDp *cloudwatch.Datapoint
	for _, dp := range datapoints {
		if dp.Timestamp.Before(*latest) {
			continue
		}

		latest = dp.Timestamp
		latestDp = dp
	}

	return latestDp, nil
}

func mergeStatsFromDatapoint(stats map[string]interface{}, dp *cloudwatch.Datapoint, mg metricsGroup) map[string]interface{} {
	if dp != nil {
		for _, met := range mg.Metrics {
			switch met.Type {
			case metricsTypeAverage:
				stats[met.MackerelName] = *dp.Average
			case metricsTypeSum:
				stats[met.MackerelName] = *dp.Sum
			case metricsTypeMaximum:
				stats[met.MackerelName] = *dp.Maximum
			case metricsTypeMinimum:
				stats[met.MackerelName] = *dp.Minimum
			}
		}
	}
	return stats
}

var defaultMetricsGroup = []metricsGroup{
	{CloudWatchName: "ConditionalCheckFailedRequests", Metrics: []metric{
		{MackerelName: "ConditionalCheckFailedRequests", Type: metricsTypeSum},
	}},
	{CloudWatchName: "ConsumedReadCapacityUnits", Metrics: []metric{
		{MackerelName: "ConsumedReadCapacityUnitsSum", Type: metricsTypeSum},
		{MackerelName: "ConsumedReadCapacityUnitsAverage", Type: metricsTypeAverage},
	}},
	{CloudWatchName: "ConsumedWriteCapacityUnits", Metrics: []metric{
		{MackerelName: "ConsumedWriteCapacityUnitsSum", Type: metricsTypeSum},
		{MackerelName: "ConsumedWriteCapacityUnitsAverage", Type: metricsTypeAverage},
	}},
	{CloudWatchName: "ProvisionedReadCapacityUnits", Metrics: []metric{
		{MackerelName: "ProvisionedReadCapacityUnits", Type: metricsTypeMinimum},
	}},
	{CloudWatchName: "ProvisionedWriteCapacityUnits", Metrics: []metric{
		{MackerelName: "ProvisionedWriteCapacityUnits", Type: metricsTypeMinimum},
	}},
	{CloudWatchName: "SystemErrors", Metrics: []metric{
		{MackerelName: "SystemErrors", Type: metricsTypeSum},
	}},
	{CloudWatchName: "UserErrors", Metrics: []metric{
		{MackerelName: "UserErrors", Type: metricsTypeSum},
	}},
	{CloudWatchName: "WriteThrottleEvents", Metrics: []metric{
		{MackerelName: "WriteThrottleEvents", Type: metricsTypeSum},
	}},
}

// FetchMetrics fetch the metrics
func (p DynamoDBPlugin) FetchMetrics() (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	for _, met := range defaultMetricsGroup {
		v, err := getLastPointFromCloudWatch(p.CloudWatch, p.TableName, met)
		if err == nil {
			stats = mergeStatsFromDatapoint(stats, v, met)
		} else {
			log.Printf("%s: %s", met, err)
		}
	}
	return transformMetrics(stats), nil
}

// TransformMetrics converts some of datapoints to post differences of two metrics
func transformMetrics(stats map[string]interface{}) map[string]interface{} {
	// Although stats are interface{}, those values from cloudwatch.Datapoint are guaranteed to be numerical
	if consumedReadCapacitySum, ok := stats["ConsumedReadCapacityUnitsSum"].(float64); ok {
		stats["ConsumedReadCapacityUnitsNormalized"] = consumedReadCapacitySum / 60.0
	}
	if consumedWriteCapacitySum, ok := stats["ConsumedWriteCapacityUnitsSum"].(float64); ok {
		stats["ConsumedWriteCapacityUnitsNormalized"] = consumedWriteCapacitySum / 60.0
	}
	return stats
}

// GraphDefinition of DynamoDBPlugin
func (p DynamoDBPlugin) GraphDefinition() map[string]mp.Graphs {
	labelPrefix := strings.Title(p.Prefix)
	labelPrefix = strings.Replace(labelPrefix, "-", " ", -1)

	var graphdef = map[string]mp.Graphs{
		"ReadCapacity": {
			Label: (labelPrefix + " Read Capacity Units"),
			Unit:  "float",
			Metrics: []mp.Metrics{
				{Name: "ProvisionedReadCapacityUnits", Label: "Provisioned"},
				{Name: "ConsumedReadCapacityUnitsNormalized", Label: "Consumed"},
				{Name: "ConsumedReadCapacityUnitsAverage", Label: "Consumed (Average per request)"},
			},
		},
		"WriteCapacity": {
			Label: (labelPrefix + " Write Capacity Units"),
			Unit:  "float",
			Metrics: []mp.Metrics{
				{Name: "ProvisionedWriteCapacityUnits", Label: "Provisioned"},
				{Name: "ConsumedWriteCapacityUnitsNormalized", Label: "Consumed"},
				{Name: "ConsumedWriteCapacityUnitsAverage", Label: "Consumed (Average per request)"},
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
