#pragma once

#include <string>
#include <utility>
#include <vector>

using KeyValues = std::vector<std::pair<std::string, std::string>>;

// libpq connection keywords, forwarded verbatim to PQconnectdbParams.
struct SourceConfig {
    KeyValues params;
};

// Kafka producer settings. `properties` are librdkafka configuration
// properties, forwarded verbatim to rd_kafka_conf_set; recordflow does not
// interpret them, so the whole of librdkafka's CONFIGURATION.md is reachable
// from the config file without a code change.
struct SinkConfig {
    std::string topic;
    KeyValues properties;
};

struct Config {
    SourceConfig source;
    SinkConfig sink;
};

Config load_config(const std::string& path = "recordflow.conf");
