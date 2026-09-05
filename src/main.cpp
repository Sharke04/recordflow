#include "pg/replication_stream.hpp"

#include <cstdio>

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: %s <slot> <publication>\n", argv[0]);
        std::fprintf(stderr,
                     "  the slot and publication must already exist on the "
                     "source database.\n");
        return 2;
    }

    return stream(argv[1], argv[2]);
}
