#include "pg/conninfo.hpp"

#include <libpq-fe.h>

#include <algorithm>
#include <cctype>
#include <format>
#include <optional>
#include <sstream>

#include "runtime/errors.hpp"

namespace recordflow::pg {

namespace {

std::string lower(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return text;
}

// GUC assignments inside a libpq `options` value: "-c name=value",
// "-cname=value" or "--name=value".
std::vector<std::pair<std::string, std::string>> parse_options(const std::string& options) {
    std::vector<std::string> tokens;
    std::istringstream in(options);
    for (std::string token; in >> token;) tokens.push_back(token);

    std::vector<std::pair<std::string, std::string>> settings;
    auto add = [&](std::string assignment) {
        const auto eq = assignment.find('=');
        if (eq == std::string::npos) return;
        std::string name = assignment.substr(0, eq);
        std::replace(name.begin(), name.end(), '-', '_');
        settings.emplace_back(lower(name), assignment.substr(eq + 1));
    };
    for (std::size_t i = 0; i < tokens.size(); ++i) {
        const std::string& token = tokens[i];
        if (token == "-c" && i + 1 < tokens.size()) {
            add(tokens[++i]);
        } else if (token.starts_with("-c")) {
            add(token.substr(2));
        } else if (token.starts_with("--")) {
            add(token.substr(2));
        }
    }
    return settings;
}

std::string merge_options(const std::optional<std::string>& user_options) {
    std::string required;
    for (const auto& [name, value] : required_gucs()) {
        required += std::format("{}-c {}={}", required.empty() ? "" : " ", name, value);
    }
    if (!user_options || user_options->empty()) return required;

    for (const auto& [name, value] : parse_options(*user_options)) {
        for (const auto& [required_name, required_value] : required_gucs()) {
            if (name == lower(required_name) && lower(value) != lower(required_value)) {
                throw ConfigError(std::format(
                    "source.conninfo: options set {}={}, but Recordflow requires {}={}", name, value,
                    required_name, required_value));
            }
        }
    }
    return *user_options + " " + required;
}

}  // namespace

KeyValues build_connection_params(const std::string& conninfo, ConnectionKind kind) {
    char* error = nullptr;
    PQconninfoOption* parsed = PQconninfoParse(conninfo.c_str(), &error);
    if (parsed == nullptr) {
        std::string message = error != nullptr ? error : "out of memory";
        if (error != nullptr) PQfreemem(error);
        while (!message.empty() && (message.back() == '\n' || message.back() == ' ')) message.pop_back();
        throw ConfigError("source.conninfo: " + message);
    }

    KeyValues params;
    std::optional<std::string> options;
    bool has_application_name = false;
    bool has_client_encoding = false;
    for (PQconninfoOption* option = parsed; option->keyword != nullptr; ++option) {
        if (option->val == nullptr || option->val[0] == '\0') continue;
        const std::string keyword = option->keyword;
        const std::string value = option->val;
        if (keyword == "replication") {
            PQconninfoFree(parsed);
            throw ConfigError("source.conninfo must not set 'replication'; Recordflow opens the "
                              "replication connection itself");
        }
        if (keyword == "options") {
            options = value;
            continue;
        }
        if (keyword == "client_encoding") {
            const std::string encoding = lower(value);
            if (encoding != "utf8" && encoding != "utf-8") {
                PQconninfoFree(parsed);
                throw ConfigError("source.conninfo: client_encoding must be UTF8, got " + value);
            }
            has_client_encoding = true;
        }
        if (keyword == "application_name") has_application_name = true;
        params.emplace_back(keyword, value);
    }
    PQconninfoFree(parsed);

    params.emplace_back("options", merge_options(options));
    if (!has_client_encoding) params.emplace_back("client_encoding", "UTF8");
    if (!has_application_name) params.emplace_back("application_name", "recordflow");
    if (kind == ConnectionKind::Replication) params.emplace_back("replication", "database");
    return params;
}

}  // namespace recordflow::pg
