#include <seastar/core/sleep.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/file.hh>
#include "seastar/core/reactor.hh"
#include "seastar/core/sstring.hh"
// #include "seastar/core/aligned_buffer.hh"
#include <seastar/core/temporary_buffer.hh>

#include <iostream>
#include <stdexcept>
#include <algorithm>

using namespace seastar;
using namespace std;


static unsigned int RECORD_MAX_SIZE = 4096;


static const char* FNAME = "test.cpp";

class ExternalSorter {
private:
    static const uint64_t RECORD_SIZE = 4096;
    //static const size_t RAM_AVAILABLE = RECORD_MAX_SIZE * 256 * 1024 * 4; // chunk size (4GB)
    static const size_t RAM_AVAILABLE = 4; // TODO nie to
    static vector<string> records; // each of size RECORD_SIZE

    static future<> read(file &f, uint64_t fsize) {
        if (fsize < RECORD_SIZE)
            return make_ready_future();
        // there should be nothing left, if it is we just discard it.
        return f.dma_read_exactly<char>(0, RECORD_SIZE).then([&f, fsize] (temporary_buffer<char> buf) {
            ExternalSorter::records.emplace_back(move(string(buf.get())));
            return read(f, fsize - RECORD_SIZE);
        });
    }

public:
    static future<> f() {
        static sstring fname(FNAME);
        static uint64_t i = 0;
        return open_file_dma(fname, open_flags::rw).then([] (file f) {
            return f.size().then([f] (uint64_t fsize) mutable {
                cout << fsize;
                return read(f, fsize);
            });
        });
    }

    int compute(int argc, char **argv) {
        app_template app;
        ExternalSorter::records = std::vector<string> {};
        try {
            app.run(argc, argv, f);
        } catch (...) {
            std::cerr << "Couldn't start application: ";
            return 1;
        }
        cout << ExternalSorter::records << endl;
        return 0;
    }
};

int main(int argc, char **argv) {
    static ExternalSorter exsort;
    exsort.compute(argc, argv);
}

