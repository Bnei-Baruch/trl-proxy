// Package mqttx is a thin wrapper around paho.mqtt.golang (v3) that adds
// auto-reconnect handling and convenient retained-message publishing.
//
// Highlights:
//   - LWT (Last Will & Testament): on disconnect the broker publishes the
//     "offline" retained payload to our status topic.
//   - On every successful (re)connect we publish the "online" retained
//     payload and re-subscribe to the declared topics via the OnConnect hook.
//   - Subscriptions are declared once via Subscribe; the library and our
//     OnConnect hook together guarantee that they are restored after every
//     reconnect (we keep a per-client subscription table).
package mqttx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Handler is invoked when a subscribed message arrives.
type Handler func(topic string, payload []byte)

// Options is the input config for building a client.
type Options struct {
	Brokers        []string      // host:port (no scheme — we prepend tcp://)
	ClientID       string        // must stay stable across restarts
	Username       string        // optional
	Password       string        // optional
	KeepAlive      time.Duration // e.g. 15s
	ConnectTimeout time.Duration // e.g. 10s

	WillTopic   string // retained LWT topic
	WillPayload []byte // e.g. "offline"

	// OnlineTopic / OnlinePayload — what to publish on every (re)connect.
	// retained=true is implicit.
	OnlineTopic   string
	OnlinePayload []byte
}

// Client is a single MQTT client instance.
type Client struct {
	cli    mqtt.Client
	log    *slog.Logger
	opts   Options
	mu     sync.Mutex
	subs   map[string]subscription // declared subscriptions, restored on reconnect
	closed bool
}

type subscription struct {
	qos     byte
	handler Handler
}

// New constructs the client but does NOT connect. Call Connect afterwards.
func New(o Options, log *slog.Logger) *Client {
	c := &Client{
		log:  log,
		opts: o,
		subs: make(map[string]subscription),
	}
	po := mqtt.NewClientOptions()
	for _, b := range o.Brokers {
		po.AddBroker(normalizeBroker(b))
	}
	po.SetClientID(o.ClientID)
	if o.Username != "" {
		po.SetUsername(o.Username)
	}
	if o.Password != "" {
		po.SetPassword(o.Password)
	}
	if o.KeepAlive > 0 {
		po.SetKeepAlive(o.KeepAlive)
	}
	if o.ConnectTimeout > 0 {
		po.SetConnectTimeout(o.ConnectTimeout)
	}
	po.SetCleanSession(true)
	po.SetAutoReconnect(true)
	po.SetMaxReconnectInterval(30 * time.Second)
	po.SetOrderMatters(false)

	if o.WillTopic != "" {
		po.SetBinaryWill(o.WillTopic, o.WillPayload, 1, true)
	}

	po.SetOnConnectHandler(c.onConnect)
	po.SetConnectionLostHandler(c.onConnectionLost)
	po.SetReconnectingHandler(func(_ mqtt.Client, _ *mqtt.ClientOptions) {
		c.log.Info("mqtt reconnecting")
	})

	c.cli = mqtt.NewClient(po)
	return c
}

// Connect dials one of the configured brokers and blocks until the result
// (or the timeout) is known. After Connect, handlers and subscriptions are
// restored automatically on every reconnect.
func (c *Client) Connect(ctx context.Context) error {
	token := c.cli.Connect()
	select {
	case <-token.Done():
		if err := token.Error(); err != nil {
			return fmt.Errorf("mqtt connect: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Subscribe declares a subscription. If the client is already connected,
// it subscribes immediately; otherwise (or after a reconnect) the OnConnect
// hook restores all declared subscriptions.
func (c *Client) Subscribe(topic string, qos byte, h Handler) error {
	c.mu.Lock()
	c.subs[topic] = subscription{qos: qos, handler: h}
	c.mu.Unlock()

	if !c.cli.IsConnectionOpen() {
		return nil // will be restored via onConnect
	}
	token := c.cli.Subscribe(topic, qos, c.makeMQTTHandler(h))
	token.Wait()
	return token.Error()
}

// Publish sends a message. retained=true leaves it as the retained payload.
func (c *Client) Publish(topic string, qos byte, retained bool, payload []byte) error {
	token := c.cli.Publish(topic, qos, retained, payload)
	if !token.WaitTimeout(5 * time.Second) {
		return errors.New("mqtt publish timeout")
	}
	return token.Error()
}

// PublishJSON marshals v and publishes it with retained=true, QoS 1.
func (c *Client) PublishJSON(topic string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Publish(topic, 1, true, b)
}

// Disconnect terminates the connection. Before leaving we publish the offline
// LWT payload ourselves as well (the broker would do it on detect, but doing
// it explicitly is more reliable on a clean shutdown).
func (c *Client) Disconnect() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	if c.opts.WillTopic != "" {
		_ = c.Publish(c.opts.WillTopic, 1, true, c.opts.WillPayload)
	}
	c.cli.Disconnect(500)
}

// IsConnected returns the current connection state (handy for diagnostics).
func (c *Client) IsConnected() bool { return c.cli.IsConnectionOpen() }

func (c *Client) onConnect(cli mqtt.Client) {
	c.log.Info("mqtt connected", "client_id", c.opts.ClientID)
	if c.opts.OnlineTopic != "" {
		token := cli.Publish(c.opts.OnlineTopic, 1, true, c.opts.OnlinePayload)
		token.WaitTimeout(2 * time.Second)
		if err := token.Error(); err != nil {
			c.log.Warn("mqtt publish online failed", "err", err)
		}
	}
	c.mu.Lock()
	subs := make(map[string]subscription, len(c.subs))
	for t, s := range c.subs {
		subs[t] = s
	}
	c.mu.Unlock()

	for topic, s := range subs {
		topic, s := topic, s
		token := cli.Subscribe(topic, s.qos, c.makeMQTTHandler(s.handler))
		token.WaitTimeout(5 * time.Second)
		if err := token.Error(); err != nil {
			c.log.Warn("mqtt subscribe failed", "topic", topic, "err", err)
			continue
		}
		c.log.Info("mqtt subscribed", "topic", topic, "qos", s.qos)
	}
}

func (c *Client) onConnectionLost(_ mqtt.Client, err error) {
	c.log.Warn("mqtt connection lost", "err", err)
}

func (c *Client) makeMQTTHandler(h Handler) mqtt.MessageHandler {
	return func(_ mqtt.Client, msg mqtt.Message) {
		defer func() {
			if r := recover(); r != nil {
				c.log.Error("mqtt handler panic", "topic", msg.Topic(), "panic", r)
			}
		}()
		h(msg.Topic(), msg.Payload())
	}
}

func normalizeBroker(b string) string {
	for _, prefix := range []string{"tcp://", "ssl://", "tls://", "ws://", "wss://"} {
		if len(b) >= len(prefix) && b[:len(prefix)] == prefix {
			return b
		}
	}
	return "tcp://" + b
}
