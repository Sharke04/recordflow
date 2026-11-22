#include "config/describe.hpp"

#include <algorithm>
#include <format>
#include <regex>

#include "config/fields.hpp"

namespace recordflow::config {

namespace {

std::string quoted(const std::string& value) {
    std::string out = "\"";
    for (char ch : value) {
        if (ch == '"' || ch == '\\') out += '\\';
        out += ch;
    }
    return out + "\"";
}

std::string shown_value(const Config& config, const Field& field) {
    std::string value = field.show(config);
    if (field.secret) return value.empty() ? quoted("") : quoted("****");
    if (field.section == "source" && field.key == "conninfo") value = mask_conninfo(value);
    return field.kind == FieldKind::Integer ? value : quoted(value);
}

}  // namespace

std::string mask_conninfo(std::string_view conninfo) {
    // key=value form, plain or single-quoted value; also matches URI query parameters
    static const std::regex keyword(R"((password\s*=\s*)('(?:[^'\\]|\\.)*'|[^\s&]*))",
                                    std::regex::icase);
    // URI form: postgresql://user:secret@host
    static const std::regex uri(R"((postgres(?:ql)?://[^:/@\s]*:)[^@\s]*(@))", std::regex::icase);

    std::string text(conninfo);
    text = std::regex_replace(text, keyword, "$1****");
    text = std::regex_replace(text, uri, "$1****$2");
    return text;
}

std::string describe(const Config& config) {
    std::size_t key_width = 0;
    for (const Field& field : fields()) key_width = std::max(key_width, field.key.size());

    std::vector<std::string> lines;
    std::size_t value_width = 0;
    for (const Field& field : fields()) {
        const std::string line = std::format("{:<{}} = {}", field.key, key_width, shown_value(config, field));
        value_width = std::max(value_width, line.size());
        lines.push_back(line);
    }

    std::string out = "effective configuration:";
    std::string_view section;
    for (std::size_t i = 0; i < fields().size(); ++i) {
        const Field& field = fields()[i];
        if (field.section != section) {
            section = field.section;
            out += std::format("\n  [{}]", section);
        }
        out += std::format("\n    {:<{}}  # {}", lines[i], value_width,
                           source_name(field.source(config)));
    }
    return out;
}

}  // namespace recordflow::config
