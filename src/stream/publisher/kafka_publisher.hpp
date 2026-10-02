#pragma once

#include <atomic>
#include <cstdint>
#include <stdexcept>
#include <string>

#include "config.hpp"
#include "stream/publisher/change_event.hpp"

struct rd_kafka_s;
struct rd_kafka_topic_s;
struct rd_kafka_message_s;

// Thrown when the producer cannot be built or a message cannot be enqueued.
// Distinct from the decode errors the stream loop tolerates: a broken producer
// is not something to skip past, because skipping would drop events.
class PublishError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// librdkafka producer. Messages are enqueued asynchronously and acknowledged
// out of band, so `flush` is what turns "handed to librdkafka" into "durably
// on the broker" — the stream loop calls it before advancing the replication
// slot.
class KafkaPublisher {
public:
    explicit KafkaPublisher(const SinkConfig& config);
    ~KafkaPublisher();

    KafkaPublisher(const KafkaPublisher&) = delete;
    KafkaPublisher& operator=(const KafkaPublisher&) = delete;

    // Enqueues one event. Blocks while the local queue is full rather than
    // dropping, which is what propagates backpressure to the stream reader.
    void publish(const pgoutput::ChangeEvent& event);

    // Waits for every outstanding message to be acknowledged. Returns false if
    // the wait timed out or any delivery failed since the previous call, in
    // which case the caller must not confirm the corresponding WAL.
    bool flush(int timeout_ms = 10000);

private:
    static void delivery_report(rd_kafka_s* producer,
                                const rd_kafka_message_s* message, void* opaque);

    rd_kafka_s* producer_ = nullptr;
    rd_kafka_topic_s* topic_ = nullptr;
    std::atomic<std::uint64_t> failed_ = 0;
};
