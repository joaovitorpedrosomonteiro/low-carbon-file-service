package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
)

type Publisher struct {
	client *pubsub.Client
	topic  *pubsub.Topic
}

func NewPublisher(ctx context.Context, projectID string, topicID string) (*Publisher, error) {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("create pubsub client: %w", err)
	}

	topic := client.Topic(topicID)

	return &Publisher{
		client: client,
		topic:  topic,
	}, nil
}

func (p *Publisher) Close() error {
	p.topic.Stop()
	return p.client.Close()
}

func (p *Publisher) Publish(ctx context.Context, eventType string, payload []byte) error {
	event := map[string]interface{}{
		"event_type": eventType,
		"payload":    json.RawMessage(payload),
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	result := p.topic.Publish(ctx, &pubsub.Message{
		Data: data,
	})

	_, err = result.Get(ctx)
	if err != nil {
		return fmt.Errorf("publish event: %w", err)
	}

	log.Printf("[PubSub] Published event: %s", eventType)
	return nil
}

func (p *Publisher) CloseTopic() {
	p.topic.Stop()
}

type MockPublisher struct{}

func NewMockPublisher() *MockPublisher {
	return &MockPublisher{}
}

func (m *MockPublisher) Publish(ctx context.Context, eventType string, payload []byte) error {
	log.Printf("[PubSub Mock] Event: %s Payload: %s", eventType, string(payload))
	return nil
}
