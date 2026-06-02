package dlq

import "time"

type Message struct {
	OriginalTopic     string `json:"original_topic"`
	OriginalPartition int    `json:"original_partition"`
	OriginalOffset    int64  `json:"original_offset"`

	Key     string `json:"key"`
	Payload string `json:"payload"`

	Error string `json:"error"`

	FailedAt time.Time `json:"failed_at"`
}
