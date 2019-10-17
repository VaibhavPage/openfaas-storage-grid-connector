package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/joncalhoun/qson"
	"github.com/openfaas-incubator/connector-sdk/types"
	"github.com/sirupsen/logrus"
)

// storageGridConfiguration holds the configuration required to consume and process storage grid events.
type storageGridConfiguration struct {
	// routes are REST endpoints for incoming events
	// +required
	routes map[string]*routeConfiguration
	// port to run the http server on
	// +required
	port string
	// logger
	logger *logrus.Logger
	// controller is a connector SDK controller
	controller *types.Controller
}

// routeConfiguration configures a REST endpoint that consumes StorageGrid events
type routeConfiguration struct {
	// REST endpoint
	endpoint string
	// prefixFilter is a filter applied as prefix on object key which caused the notification.
	// +optional
	prefixFilter string
	// suffixFilters is a filter applied as suffix on object key which caused the notification.
	// +optional
	suffixFilter string
}

// storageGridNotification is the bucket notification received from storage grid
type storageGridNotification struct {
	Action  string `json:"Action"`
	Message struct {
		Records []struct {
			EventVersion string    `json:"eventVersion"`
			EventSource  string    `json:"eventSource"`
			EventTime    time.Time `json:"eventTime"`
			EventName    string    `json:"eventName"`
			UserIdentity struct {
				PrincipalID string `json:"principalId"`
			} `json:"userIdentity"`
			RequestParameters struct {
				SourceIPAddress string `json:"sourceIPAddress"`
			} `json:"requestParameters"`
			ResponseElements struct {
				XAmzRequestID string `json:"x-amz-request-id"`
			} `json:"responseElements"`
			S3 struct {
				S3SchemaVersion string `json:"s3SchemaVersion"`
				ConfigurationID string `json:"configurationId"`
				Bucket          struct {
					Name          string `json:"name"`
					OwnerIdentity struct {
						PrincipalID string `json:"principalId"`
					} `json:"ownerIdentity"`
					Arn string `json:"arn"`
				} `json:"bucket"`
				Object struct {
					Key       string `json:"key"`
					Size      int    `json:"size"`
					ETag      string `json:"eTag"`
					Sequencer string `json:"sequencer"`
				} `json:"object"`
			} `json:"s3"`
		} `json:"Records"`
	} `json:"Message"`
	TopicArn string `json:"TopicArn"`
	Version  string `json:"Version"`
}

var (
	respBody = `
<PublishResponse xmlns="http://argoevents-sns-server/">
    <PublishResult> 
        <MessageId>` + generateUUID().String() + `</MessageId> 
    </PublishResult> 
    <ResponseMetadata>
       <RequestId>` + generateUUID().String() + `</RequestId>
    </ResponseMetadata> 
</PublishResponse>` + "\n"
)

// generateUUID returns a new uuid
func generateUUID() uuid.UUID {
	return uuid.New()
}

func main() {
	creds := types.GetCredentials()
	gatewayURL, ok := os.LookupEnv("GATEWAY_URL")
	if !ok {
		panic("gateway url is not specified")
	}

	upstreamTimeout := time.Second * 30
	rebuildInterval := time.Second * 3

	if val, exists := os.LookupEnv("upstream_timeout"); exists {
		parsedVal, err := time.ParseDuration(val)
		if err == nil {
			upstreamTimeout = parsedVal
		}
	}

	if val, exists := os.LookupEnv("rebuild_interval"); exists {
		parsedVal, err := time.ParseDuration(val)
		if err == nil {
			rebuildInterval = parsedVal
		}
	}

	printResponse := false
	if val, exists := os.LookupEnv("print_response"); exists {
		printResponse = val == "1" || val == "true"
	}

	printResponseBody := false
	if val, exists := os.LookupEnv("print_response_body"); exists {
		printResponseBody = val == "1" || val == "true"
	}

	delimiter := ","
	if val, exists := os.LookupEnv("topic_delimiter"); exists {
		if len(val) > 0 {
			delimiter = val
		}
	}

	asynchronousInvocation := false
	if val, exists := os.LookupEnv("asynchronous_invocation"); exists {
		asynchronousInvocation = val == "1" || val == "true"
	}

	config := &types.ControllerConfig{
		UpstreamTimeout:          upstreamTimeout,
		GatewayURL:               gatewayURL,
		PrintResponse:            printResponse,
		PrintResponseBody:        printResponseBody,
		RebuildInterval:          rebuildInterval,
		TopicAnnotationDelimiter: delimiter,
		AsyncFunctionInvocation:  asynchronousInvocation,
	}

	controller := types.NewController(creds, config)

	controller.BeginMapBuilder()

	r := mux.NewRouter()

	sgCfg, err := newStorageGridConfiguration()
	if err != nil {
		panic(err)
	}
	sgCfg.controller = controller

	for endpoint, _ := range sgCfg.routes {
		r.HandleFunc(endpoint, sgCfg.handler).Methods(http.MethodHead, http.MethodPost)
	}

	server := http.Server{
		Addr:    fmt.Sprintf(":%s", sgCfg.port),
		Handler: r,
	}

	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}

func (sgCfg *storageGridConfiguration) handler(writer http.ResponseWriter, request *http.Request) {
	sgCfg.logger.WithField("endpoint", request.URL.Path).Infoln("received a notification")

	if _, ok := sgCfg.routes[request.URL.Path]; !ok {
		sgCfg.logger.WithField("endpoint", request.URL.Path).Errorln("unknown route")
		writer.WriteHeader(http.StatusNotFound)
		writer.Write([]byte("unknown route"))
		return
	}

	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		sgCfg.logger.WithError(err).Errorln("failed to parse request body")
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write([]byte("malformed request body"))
		return
	}

	switch request.Method {
	case http.MethodHead:
		respBody = ""
	}
	writer.WriteHeader(http.StatusOK)
	writer.Header().Add("Content-Type", "text/plain")
	writer.Write([]byte(respBody))

	// notification received from storage grid is url encoded.
	parsedURL, err := url.QueryUnescape(string(body))
	if err != nil {
		sgCfg.logger.WithError(err).Errorln("failed to unescape request body url")
		writer.WriteHeader(http.StatusBadRequest)
		writer.Write([]byte("malformed query url"))
		return
	}
	b, err := qson.ToJSON(parsedURL)
	if err != nil {
		sgCfg.logger.WithError(err).Errorln("failed to convert request body in JSON format")
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte("failed to parse request body"))
		return
	}

	var notification *storageGridNotification
	err = json.Unmarshal(b, &notification)
	if err != nil {
		sgCfg.logger.WithError(err).Errorln("failed to unmarshal request body")
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte("failed to construct the notification"))
		return
	}

	if !filterName(notification, sgCfg.routes[request.URL.Path]) {
		sgCfg.logger.Warn("discarding notification since it did not pass all filters")
	}

	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("ok"))

	sgCfg.controller.Invoke(strings.TrimLeft(request.URL.Path, "/"), &body)
}

// filterName filters object key based on configured prefix and/or suffix
func filterName(notification *storageGridNotification, rc *routeConfiguration) bool {
	if rc.prefixFilter != "" && rc.suffixFilter != "" {
		return strings.HasPrefix(notification.Message.Records[0].S3.Object.Key, rc.prefixFilter) && strings.HasSuffix(notification.Message.Records[0].S3.Object.Key, rc.suffixFilter)
	}
	if rc.prefixFilter != "" {
		return strings.HasPrefix(notification.Message.Records[0].S3.Object.Key, rc.prefixFilter)
	}
	if rc.suffixFilter != "" {
		return strings.HasSuffix(notification.Message.Records[0].S3.Object.Key, rc.suffixFilter)
	}
	return true
}

func newStorageGridConfiguration() (*storageGridConfiguration, error) {
	routesStr, ok := os.LookupEnv("STORAGE_GRID_ROUTES")
	if !ok {
		return nil, fmt.Errorf("route configuration is not specified")
	}

	port, ok := os.LookupEnv("PORT")
	if !ok {
		return nil, fmt.Errorf("port is not specified")
	}

	routes, err := parseRoutes(routesStr)
	if err != nil {
		return nil, err
	}

	return &storageGridConfiguration{
		port:   port,
		routes: routes,
	}, nil
}

// parseRoutes constructs a route configurations.
func parseRoutes(routesStr string) (map[string]*routeConfiguration, error) {
	routes := strings.Split(routesStr, ";")
	if len(routes) == 0 {
		return nil, fmt.Errorf("no routes are configured")
	}
	routeCfgs := map[string]*routeConfiguration{}

	for _, routeCfg := range routes {
		cfg := strings.Split(routeCfg, ",")
		if len(cfg) != 3 {
			return nil, fmt.Errorf("route %s is misconfigured", routeCfg)
		}

		routeCfgs[cfg[0]] = &routeConfiguration{
			endpoint:     cfg[0],
			prefixFilter: cfg[2],
			suffixFilter: cfg[3],
		}
	}
	return routeCfgs, nil
}
