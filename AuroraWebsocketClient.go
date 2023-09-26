package AuroraWebsocketClient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log" // Avoid name clash with the custom logger
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/microsoft/ApplicationInsights-Go/appinsights"
	"github.com/microsoft/ApplicationInsights-Go/appinsights/contracts"
)

const (
	API_ENDPOINT     = "https://auroracapi.eaift.com"
	AUTH_ACTION      = "auth"
	SUBSCRIBE_ACTION = "subscribe"
)

type StatusMessage struct {
	Ev      string `json:"ev"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

type AuroraWebsocketClient struct {
	APIKey          string                      // The API key to be used for authentication
	Logger          *log.Logger                 // The logger to be used for logging
	MaxWorkers      int                         // The maximum number of workers to be used for processing messages
	TelemetryClient appinsights.TelemetryClient // The telemetry client to be used for telemetry
	Pairs           string                      // The pairs to be subscribed to
	VerboseLogging  bool                        // Whether or not to use verbose logging
	apiPath         string                      // The API path for the client
	ReceivedRPS     int64                       // Average number of records received per second
	ProcessedRPS    int64                       // Average number of records processed per second
	signalCount     int32                       // Counters for worker signals
	closeMsgCount   int32                       // Counters for worker close messages
	useTelemetry    bool                        // Whether or not to use telemetry
}

// NewAuroraWebsocketClient initializes a new instance of AuroraWebsocketClient.
// It manages the configuration and instantiation of an Eudaimonia Aurora websocket client
// by providing parameters to customize the behavior and characteristics of the client.
//
// Parameters:
//
//   - apiKey (string): The API key used for authentication. It is mandatory and the function
//     will return an error if it's not provided.
//
//   - customLogger (*log.Logger): An optional custom logger instance. If not provided,
//     a default logger that writes to standard output will be created.
//
//   - maxWorkers (int): The maximum number of workers to be used for processing messages.
//     If not provided or set to 0, it defaults to 2.
//
//   - telemetryClient (appinsights.TelemetryClient): An optional telemetry client for sending
//     data to Application Insights. If not provided,
//     a default telemetry client with no instrumentation
//     key will be created.
//
// - pairs (string): The pairs to be subscribed to. If not provided, it defaults to "*".
//
//   - apiPath (string): The API path for the websocket client. If not provided, it defaults
//     to "/hubs/forecast".
//
// - verboseLogging (bool): A flag to indicate if verbose logging should be enabled.
//
// Returns:
// - *AuroraWebsocketClient: A pointer to the instantiated AuroraWebsocketClient.
//
//   - error: An error object that describes why the function failed. It will return an error
//     if the mandatory 'apiKey' parameter is not provided.
func NewAuroraWebsocketClient(apiKey string, customLogger *log.Logger, maxWorkers int, telemetryClient appinsights.TelemetryClient, pairs string, apiPath string, verboseLogging bool) (*AuroraWebsocketClient, error) {
	if customLogger == nil {
		customLogger = log.New(os.Stdout, "aurora: ", log.LstdFlags)
	}
	if maxWorkers == 0 {
		maxWorkers = 2
	}
	if telemetryClient == nil {
		telemetryClient = appinsights.NewTelemetryClient("")
	}
	if pairs == "" {
		pairs = "*"
	}
	if apiKey == "" {
		return nil, errors.New("API key is required")
	}
	if apiPath == "" {
		apiPath = "/hubs/forecast"
	}
	useTelemetry := true
	if telemetryClient == nil {
		useTelemetry = false
		telemetryClient = appinsights.NewTelemetryClient("")
	}

	return &AuroraWebsocketClient{
		APIKey:          apiKey,
		Logger:          customLogger,
		MaxWorkers:      maxWorkers,
		TelemetryClient: telemetryClient,
		Pairs:           pairs,
		apiPath:         apiPath,
		VerboseLogging:  verboseLogging,
		useTelemetry:    useTelemetry,
	}, nil
}

func (awc *AuroraWebsocketClient) logAndTelemetryTrace(message string, level contracts.SeverityLevel) {
	awc.Logger.Println(message)
	if awc.useTelemetry && awc.TelemetryClient != nil {
		awc.TelemetryClient.TrackTrace(message, level)
	}
}

// startWebSocket establishes a WebSocket connection to the specified API endpoint, authenticates,
// subscribes to the market data feed, and then listens for incoming messages. This function manages
// the lifecycle of a WebSocket connection, including setting it up, handling authentication, and
// managing subscriptions.
//
// Parameters:
// - ctx: A context that can be used to cancel or timeout the WebSocket connection and message handling.
//
// Returns:
// - A channel of type map[string]interface{} which emits incoming messages from the WebSocket.
// - An error if there's an issue with any step of the connection or subscription process.
//
// The function proceeds as follows:
// 1. Tries to establish a WebSocket connection to the API_ENDPOINT.
// 2. Authenticates the connection using the authenticate() method.
// 3. Subscribes to the market data feed using the subscribe() method.
// 4. Initiates message handling using the handleMessages() method and returns the resulting channel.
//
// Usage:
// messagesCh, err := awc.startWebSocket(context.Background())
//
//	if err != nil {
//	    // Handle error
//	}
//
//	for msg := range messagesCh {
//	    // Process incoming message
//	}
func (awc *AuroraWebsocketClient) startWebSocket(ctx context.Context) (<-chan map[string]interface{}, error) {
	message := "Attempting to connect to WebSocket..."
	awc.logAndTelemetryTrace(message, contracts.Information)

	u := url.URL{Scheme: "wss", Host: API_ENDPOINT, Path: awc.apiPath}
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	awc.logAndTelemetryTrace("Connected to WebSocket successfully.", contracts.Information)

	if err = awc.authenticate(conn); err != nil {
		awc.logAndTelemetryTrace("Failed to authenticate with WebSocket.", contracts.Error)
		return nil, err
	}
	awc.logAndTelemetryTrace("Authenticated with auroracapi.eafit.com successfully.", contracts.Information)

	if err = awc.subscribe(conn); err != nil {
		awc.logAndTelemetryTrace("Failed to subscribe to market data feed.", contracts.Error)
		return nil, err
	}
	awc.logAndTelemetryTrace("Subscribed to Eudaimonia Aurora data feed successfully.", contracts.Information)

	// Return the channel from handleMessages
	return awc.handleMessages(ctx, conn)
}

// subscribe sends a subscription message to the WebSocket connection to initiate the subscription
// to market data for the specified trading pairs. It constructs a subscription message and writes it
// as JSON to the provided WebSocket connection.
//
// Parameters:
// - conn: The active WebSocket connection on which the subscription message will be sent.
//
// Returns:
// - An error if there's an issue sending the subscription message to the WebSocket.
//
// The function proceeds as follows:
// 1. Constructs a subscription message using the SUBSCRIBE_ACTION and the trading pairs specified in awc.Pairs.
// 2. Sends the subscription message to the WebSocket connection using the WriteJSON method.
//
// If the subscription message is sent successfully, the client will start receiving market data updates
// for the specified trading pairs over the WebSocket connection.
//
// Usage:
// err := awc.subscribe(conn)
//
//	if err != nil {
//	    // Handle subscription error
//	}
func (awc *AuroraWebsocketClient) subscribe(conn *websocket.Conn) error {
	message := "Attempting to subscribe to market data..."
	awc.logAndTelemetryTrace(message, contracts.Information)

	subscribeMessage := map[string]string{
		"action": SUBSCRIBE_ACTION,
		"params": awc.Pairs,
	}

	err := conn.WriteJSON(subscribeMessage)
	if err != nil {
		awc.logAndTelemetryTrace("Failed to send subscribe message.", contracts.Error)
	}
	return err
}

// authenticate attempts to authenticate the WebSocket client using the provided API key.
// It constructs an authentication message and sends it to the WebSocket server for validation.
//
// Parameters:
// - conn: The active WebSocket connection on which the authentication message will be sent.
//
// Returns:
// - An error if there's an issue sending the authentication message or if the authentication fails.
//
// The function proceeds as follows:
// 1. Logs an informational message indicating the start of the authentication process.
// 2. Resets notification counters to ensure that signals and close messages are tracked properly for this session.
// 3. Constructs an authentication message using the AUTH_ACTION and the API key stored in awc.APIKey.
// 4. Sends the authentication message to the WebSocket server using the WriteJSON method.
//
// If the authentication message is accepted by the server, the client will be authenticated and will
// be able to proceed with other WebSocket operations. If there's an error, the function logs the
// failure and returns the encountered error.
//
// Usage:
// err := awc.authenticate(conn)
//
//	if err != nil {
//	    // Handle authentication error
//	}
func (awc *AuroraWebsocketClient) authenticate(conn *websocket.Conn) error {
	message := "Attempting authentication..."
	awc.logAndTelemetryTrace(message, contracts.Information)

	// Resetting notification counters if connection is reset
	atomic.StoreInt32(&awc.signalCount, 0)
	atomic.StoreInt32(&awc.closeMsgCount, 0)

	authMessage := map[string]string{
		"action": AUTH_ACTION,
		"params": awc.APIKey,
	}

	err := conn.WriteJSON(authMessage)
	if err != nil {
		errMessage := fmt.Sprintf("Failed to authenticate to the API: %v", err)
		awc.logAndTelemetryTrace(errMessage, contracts.Error)
	}
	return err
}

// handleInitialStatusMessages reads status messages from the WebSocket connection after
// initial authentication and subscription processes, looking specifically for an authentication
// success status message. It has a timeout, after which it logs a warning if no pertinent
// status messages are received.
//
// Parameters:
// - conn: The active WebSocket connection from which the status messages will be read.
//
// This function operates as follows:
//  1. Starts a timer that represents the maximum duration the function will wait for status messages.
//  2. Iteratively reads messages from the WebSocket, attempting to unmarshal them into status messages.
//  3. Checks the event type of each status message. If an authentication success status is detected,
//     logs this success and returns.
//  4. If the timeout duration is exceeded without recognizing an authentication success status,
//     logs a warning indicating the timeout.
//  5. Any errors encountered while reading messages from the WebSocket are logged, and the function returns.
//
// Usage:
// awc.handleInitialStatusMessages(conn)
//
// Notes:
//   - The function uses a defined StatusMessage struct (not shown in the snippet) which should
//     contain fields that map to the expected structure of the WebSocket status messages.
func (awc *AuroraWebsocketClient) handleInitialStatusMessages(conn *websocket.Conn) {
	const timeoutDuration = 5 * time.Second
	timeout := time.NewTicker(timeoutDuration)
	defer timeout.Stop()

	message := "Checking initial status messages."
	awc.logAndTelemetryTrace(message, contracts.Information)

	for {
		select {
		case <-timeout.C:
			message = "Timeout reached while checking initial status messages."
			awc.logAndTelemetryTrace(message, contracts.Warning)
			return

		default:
			_, msg, err := conn.ReadMessage()
			if err != nil {
				errMessage := fmt.Sprintf("Error reading message during status check: %v", err)
				awc.logAndTelemetryTrace(errMessage, contracts.Error)
				return
			}

			var statuses []StatusMessage
			if err := json.Unmarshal(msg, &statuses); err != nil {
				continue
			}

			for _, status := range statuses {
				if status.Ev == "status" {
					statusMessage := fmt.Sprintf("Received status: %s, Message: %s", status.Status, status.Message)
					awc.logAndTelemetryTrace(statusMessage, contracts.Information)
					if status.Status == "auth_success" {
						successMessage := "Authentication successful."
						awc.logAndTelemetryTrace(successMessage, contracts.Information)
						return
					}
				}
			}
		}
	}
}

// handleMessages manages the reception of messages from a WebSocket connection and processes them asynchronously using worker goroutines.
// This function sets up various mechanisms to handle incoming messages, process them, and forward the processed messages via channels.
// It also monitors the system's available memory to allocate buffer sizes for channels and applies backpressure handling by observing
// the buffer usage, ensuring the system remains responsive under heavy load.
//
// Parameters:
// - ctx: A context.Context that can be used to cancel or timeout the ongoing operations.
// - conn: A pointer to the WebSocket connection from which messages will be received.
//
// Returns:
// 1. A channel from which processed messages can be read.
// 2. An error which will be non-nil if any issues arise during the setup or execution.
//
// The function works by:
//  1. Reading available system memory and calculating an appropriate buffer size for channels.
//  2. Creating channels for incoming and processed messages.
//  3. Launching worker goroutines that process the incoming messages.
//  4. Continuously reading from the WebSocket connection, unmarshaling the received data,
//     and sending it to the processing workers, all while monitoring for backpressure conditions.
//
// Usage:
// messagesChan, err := awc.handleMessages(ctx, conn)
//
//	if err != nil {
//	    log.Fatalf("Failed to handle messages: %v", err)
//	}
//
//	for message := range messagesChan {
//	    // Process the message further or forward it.
//	}
func (awc *AuroraWebsocketClient) handleMessages(ctx context.Context, conn *websocket.Conn) (<-chan map[string]interface{}, error) {
	message := "Ready to receive messages..."
	awc.logAndTelemetryTrace(message, contracts.Information)

	// Get the available system memory
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)
	availableMemory := memStats.Sys

	// Calculate the buffer size based on the available memory (use 20% of the available memory for the buffer)
	bufferSize := int(0.2 * float64(availableMemory))

	// Create a channel to receive messages from the WebSocket
	incomingCh := make(chan map[string]interface{}, bufferSize)

	// Create a channel to send processed messages back to the caller
	processedCh := make(chan map[string]interface{}, bufferSize)

	// Create a WaitGroup to wait for all workers to finish
	var wg sync.WaitGroup

	// Start worker goroutines to process messages
	for i := 0; i < awc.MaxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			awc.processMessages(incomingCh, processedCh)
		}()
	}

	// Read messages in another goroutine
	go func() {
		defer func() {
			close(incomingCh)
			wg.Wait()
		}()

		for {
			select {
			case <-ctx.Done():
				return

			default:
				_, msg, err := conn.ReadMessage()
				if err != nil {
					// Detect connection reset
					if strings.Contains(err.Error(), "unexpected EOF") {
						awc.logAndTelemetryTrace("WebSocket connection reset detected.", contracts.Critical)
					}
					closeMessage := "Error encountered. Closing message channel."
					awc.logAndTelemetryTrace(closeMessage, contracts.Error)
					return
				}

				var events []map[string]interface{}
				if err := json.Unmarshal(msg, &events); err != nil {
					errMessage := fmt.Sprintf("Failed to unmarshal message: %s", err)
					awc.logAndTelemetryTrace(errMessage, contracts.Warning)
					continue
				}

				for _, event := range events {
					currentBufferUsage := len(incomingCh)
					atomic.AddInt64(&awc.ReceivedRPS, 1)

					// Check buffer usage and log warnings or critical alerts as necessary
					if currentBufferUsage > int(0.8*float64(bufferSize)) {
						telemetryLevel := contracts.Warning
						messagePrefix := "Potential"

						if currentBufferUsage > int(0.9*float64(bufferSize)) {
							telemetryLevel = contracts.Critical
							messagePrefix = "Critical"
						}

						alertMessage := fmt.Sprintf("%s backpressure detected! Buffer usage %s. Buffer size is currently at %d out of %d.", messagePrefix, strings.ToLower(messagePrefix), currentBufferUsage, bufferSize)
						awc.logAndTelemetryTrace(alertMessage, telemetryLevel)
						if awc.useTelemetry {
							awc.TelemetryClient.TrackMetric(messagePrefix+"BackpressureDetected", 1)
						}
					}

					incomingCh <- event
				}
			}
		}
	}()

	return processedCh, nil
}

// processMessages continuously reads messages from the incomingCh and sends them to the processedCh.
// This function acts as a worker that transfers messages from one channel to another, possibly
// allowing for transformations or processing of the messages in between. As of now, it primarily
// serves as a forwarding mechanism, but it's designed with extensibility in mind —
// specific transformations based on consumer requirements (e.g., converting data formats)
// can be added in the future.
//
// Parameters:
// - incomingCh: A channel from which raw or incoming messages will be read.
// - processedCh: A channel to which messages will be sent after any necessary processing.
//
// The function works by:
//  1. Continuously reading from the incomingCh until it's closed.
//  2. Attempting to forward each read message to the processedCh.
//  3. Handling scenarios where the processedCh might be full or no receivers are present
//     by logging a warning.
//
// Usage:
// incoming := make(chan map[string]interface{}, 100)
// processed := make(chan map[string]interface{}, 100)
// awc.processMessages(incoming, processed)
// // Feed messages to incoming and read them from processed.
func (awc *AuroraWebsocketClient) processMessages(incomingCh, processedCh chan map[string]interface{}) {
	for message := range incomingCh {
		// Implement Metrics
		//atomic.AddInt64(&awc.ProcessedRPS, 1)

		// Send the message to the processedCh.
		select {
		case processedCh <- message:
			// Add handling of record conversion based on data type here if our consumer requests it. (Struct vs raw JSON, for example.)
		default:
			// Handle the case when the processedCh is full or no one is reading from it.
			// For now, we'll just log the error and continue.
			awc.logAndTelemetryTrace("Failed to send the processed message. Channel might be full.", contracts.Warning)
		}
	}
}

// StartService initializes and starts the WebSocket client service for Aurora.
// It establishes a connection to the Aurora WebSocket, authenticates, and subscribes to the data feed.
// Once started, it continuously listens for incoming messages from the WebSocket.
//
// Returns:
// - A read-only channel of maps containing the received WebSocket messages. Consumers can listen on this channel for incoming messages.
// - A CancelFunc which can be called to terminate the WebSocket listener and stop the service.
// - An error, if any occurred during the initialization or startup process. If an error is returned, the service might not be running, and the returned channel might be nil.
//
// The function internally uses a goroutine to initiate the WebSocket connection and handle incoming messages.
// It ensures synchronization by using a channel of channels to pass the messages channel back to the caller,
// and an error channel to communicate any errors encountered during the initialization or runtime.
//
// Usage:
// messagesChan, cancel, err := awc.StartService()
//
//	if err != nil {
//	    log.Fatalf("Failed to start service: %v", err)
//	}
//
//	for message := range messagesChan {
//	    // Process received message
//	}
//
// ...
// cancel() // When you want to stop the service
func (awc *AuroraWebsocketClient) StartService() (<-chan map[string]interface{}, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	messagesChChan := make(chan (<-chan map[string]interface{}), 1) // channel to pass the messagesChan
	errChan := make(chan error, 1)

	go func() {
		messagesChan, err := awc.startWebSocket(ctx)
		if err != nil && err != context.Canceled {
			// handle error, logging it using our shared logging method
			awc.logAndTelemetryTrace(fmt.Sprintf("Error in startWebSocket: %v", err), contracts.Error)
			errChan <- err
			return
		}
		messagesChChan <- messagesChan
	}()

	select {
	case messagesChan := <-messagesChChan:
		return messagesChan, cancel, nil
	case err := <-errChan:
		return nil, cancel, err
	}
}

// StopService terminates the WebSocket client service for Aurora.
// It stops the continuous listening of incoming messages from the WebSocket and gracefully shuts down any associated goroutines.
//
// Parameters:
// - cancel: A CancelFunc returned by the StartService method. When called, it will signal the service to terminate its operations.
//
// Once StopService is invoked, any ongoing operations inside the WebSocket client service will be stopped. It's important to ensure that the service is no longer needed before calling this function, as invoking it will permanently stop the service for its current lifecycle.
//
// Usage:
// cancelFunc, err := awc.StartService()
//
//	if err != nil {
//	    log.Fatalf("Failed to start service: %v", err)
//	}
//
// ...
// awc.StopService(cancelFunc) // When you want to stop the service
func (awc *AuroraWebsocketClient) StopService(cancel context.CancelFunc) {
	cancel() // this will stop the websocket listener
}
