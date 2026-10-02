#include "stream/publisher/kafka_publisher.hpp"

#include <librdkafka/rdkafka.h>

#include <cstdio>

namespace {

// How long to wait for the local queue to drain before retrying an enqueue.
constexpr int kQueueFullPollMs = 100;

// Give in-flight messages a chance to land before tearing the producer down.
constexpr int kShutdownFlushMs = 5000;

}

KafkaPublisher::KafkaPublisher(const SinkConfig& config) {
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    char errstr[512];

    for (const auto& [property, value] : config.properties) {
        if (rd_kafka_conf_set(conf, property.c_str(), value.c_str(), errstr,
                              sizeof errstr) != RD_KAFKA_CONF_OK) {
            rd_kafka_conf_destroy(conf);
            throw PublishError("kafka." + property + ": " + errstr);
        }
    }

    rd_kafka_conf_set_dr_msg_cb(conf, delivery_report);
    rd_kafka_conf_set_opaque(conf, this);

    // On success the producer takes ownership of conf; on failure it does not.
    producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof errstr);
    if (producer_ == nullptr) {
        rd_kafka_conf_destroy(conf);
        throw PublishError(std::string("could not create Kafka producer: ") +
                           errstr);
    }

    topic_ = rd_kafka_topic_new(producer_, config.topic.c_str(), nullptr);
    if (topic_ == nullptr) {
        const rd_kafka_resp_err_t err = rd_kafka_last_error();
        rd_kafka_destroy(producer_);
        producer_ = nullptr;
        throw PublishError("could not open topic '" + config.topic +
                           "': " + rd_kafka_err2str(err));
    }
}

KafkaPublisher::~KafkaPublisher() {
    if (producer_ != nullptr) {
        rd_kafka_flush(producer_, kShutdownFlushMs);
    }
    if (topic_ != nullptr) {
        rd_kafka_topic_destroy(topic_);
    }
    if (producer_ != nullptr) {
        rd_kafka_destroy(producer_);
    }
}

void KafkaPublisher::delivery_report(rd_kafka_s*,
                                     const rd_kafka_message_s* message,
                                     void* opaque) {
    if (message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
        return;
    }
    auto* self = static_cast<KafkaPublisher*>(opaque);
    self->failed_.fetch_add(1, std::memory_order_relaxed);
    std::fprintf(stderr, "kafka delivery failed: %s\n",
                 rd_kafka_err2str(message->err));
}

void KafkaPublisher::publish(const pgoutput::ChangeEvent& event) {
    const std::string key = event.key();
    const std::string payload = event.json();

    // rd_kafka_produce rather than rd_kafka_producev: the variadic form builds
    // its arguments out of GNU statement expressions, which this project's
    // -Wpedantic rightly objects to. Partitioning is left to librdkafka's
    // default partitioner, which hashes the key we supply.
    for (;;) {
        const int rc = rd_kafka_produce(
            topic_, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
            const_cast<char*>(payload.data()), payload.size(), key.data(),
            key.size(), nullptr);

        if (rc == 0) {
            // Serve any delivery reports that came back while we were decoding.
            rd_kafka_poll(producer_, 0);
            return;
        }

        const rd_kafka_resp_err_t err = rd_kafka_last_error();
        if (err != RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            throw PublishError(std::string("could not enqueue event: ") +
                               rd_kafka_err2str(err));
        }
        rd_kafka_poll(producer_, kQueueFullPollMs);
    }
}

bool KafkaPublisher::flush(int timeout_ms) {
    const rd_kafka_resp_err_t err = rd_kafka_flush(producer_, timeout_ms);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        std::fprintf(stderr, "kafka flush incomplete: %s\n",
                     rd_kafka_err2str(err));
        return false;
    }
    return failed_.exchange(0, std::memory_order_relaxed) == 0;
}
