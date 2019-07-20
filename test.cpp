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

#include <boost/format.hpp>

using namespace seastar;
using namespace std;

static const string FNAME = string("./lol");
static const string TMP_DIR = string("./tmp/");


// Dummy parameter-pack expander
template<class T>
void expand(std::initializer_list<T>) {}

// Fun
template<class Fun, class... Args>
typename std::result_of<Fun &&(Args &&...)>::type
call(Fun &&f, Args &&... args) {

    // Print all parameters
    std::cout << "Params : ";
    expand({(std::cout << args << ' ', 0)...});
    std::cout << '\n';

    // Forward the call
    return std::forward<Fun>(f)(std::forward<Args>(args)...);
}



static const uint64_t RECORD_SIZE = 4096;
//static const size_t RECORD_SIZE = 4;
//static const size_t RAM_AVAILABLE = RECORD_MAX_SIZE * 256 * 1024 * 4; // preprocessing chunk size (4GB)
//static const size_t RAM_AVAILABLE = 8; // TODO nie to
static const size_t RAM_AVAILABLE = RECORD_SIZE * 4; // TODO nie to

static const uint64_t MERGED_BLOCK_SIZE = RAM_AVAILABLE / 10 / RECORD_SIZE; // number of records in merge-results chunks

using recordContainer = vector <string>;

static recordContainer RECORDS{}; // used in splitting-into-chunks preproccessing phase for keeping records to be dumped

// możemy czytać bezpiecznie do `read_to - 1`
future<> read_records(file f, recordContainer *save_to, uint64_t start_from, uint64_t read_to) {
    if (read_to < start_from + RECORD_SIZE) {
//        cerr << "impossibleshit\n";
        return make_ready_future();
    }
    // there should be nothing left, if it is we just discard it.
    return f.dma_read_exactly<char>(start_from, RECORD_SIZE).then([=](temporary_buffer<char> buf) {
        // zrobiłem to kopiowanie, bo zarówno buf.prefix jak i buf.trim
        // jedyne co robią to _size = len, nie zmieniają bufora.
        temporary_buffer trimmed = temporary_buffer<char>(buf.get(), RECORD_SIZE);
//        cerr << "wczytalem " << trimmed.get() << endl;
        save_to->emplace_back(move(string(trimmed.get())));
        return read_records(f, save_to, start_from + RECORD_SIZE, read_to);
    });
}


//future<> read_records(file f, uint64_t start_from, uint64_t read_to) {
//    if (read_to < start_from + RECORD_SIZE) {
////        cerr << "impossibleshit\n";
//        return make_ready_future();
//    }
//    // there should be nothing left, if it is we just discard it.
//    return f.dma_read_exactly<char>(start_from, RECORD_SIZE).then([=](temporary_buffer<char> buf) mutable {
//        // zrobiłem to kopiowanie, bo zarówno buf.prefix jak i buf.trim
//        // jedyne co robią to _size = len, nie zmieniają bufora.
//        temporary_buffer trimmed = temporary_buffer<char>(buf.get(), RECORD_SIZE);
////        cerr << "wczytalem " << trimmed.get() << endl;
//        RECORDS.emplace_back(move(string(trimmed.get())));
//        return read_records(f, start_from + RECORD_SIZE, read_to);
//    });
//}

// pos - pozycja od której zaczynamy czytanie
// po przeczytaniu znowu będziemy wołać read_chunk dla pos2 := pos + RAM_AVAILABLE,
// zatem read_to w funkcji wyżej musi być pos + RAM_AVAILABLE (bo read() czyta do read_to - 1)
future<> read_chunk(file f, uint64_t pos, uint64_t fsize) {
    assert(RECORDS.empty());
    uint64_t read_to = read_to = min(pos + RAM_AVAILABLE, fsize);
//    pos + RAM_AVAILABLE;
//    if (read_to > fsize) {
//        return make_ready_future();
//    }
//    return call(read_records, f, pos, read_to);
    return read_records(f, &RECORDS, pos, read_to);
}


future<> dump_records(file f, uint64_t num_record = 0) {
    uint64_t write_pos = num_record * RECORD_SIZE;
    if (num_record > RECORDS.size() - 1)
        return make_ready_future();
    return f.dma_write<char>(write_pos, RECORDS[num_record].c_str(), RECORD_SIZE).then([=](size_t num) {
        assert(num == RECORD_SIZE);
        return dump_records(f, num_record + 1);
    });
}


inline sstring chunk_name(uint64_t num_chunk) {
    return sstring(TMP_DIR + string("chunk") + to_string(num_chunk));
}

future<> dump_sorted_chunk(uint64_t num_chunk) {
    sstring fname = chunk_name(num_chunk);
//    cerr << records.size() << "\n";
    return open_file_dma(fname, open_flags::rw | open_flags::create).then([](file f) {
        return dump_records(f);
    });
}


// returns number of created chunks, ie. 7 for chunk0, chunk1, ..., chunk6 created
future<uint64_t> handle_chunks(file f, uint64_t fsize, uint64_t num_chunk = 0) {
    RECORDS.clear();
    uint64_t start_from = num_chunk * RAM_AVAILABLE;
//    if (start_from + RAM_AVAILABLE > fsize)
//        return make_ready_future();
    if (start_from >= fsize)
        return make_ready_future<uint64_t>(num_chunk);
    return read_chunk(f, start_from, fsize).then([=](){
        sort(RECORDS.begin(), RECORDS.end());
        return dump_sorted_chunk(num_chunk).then([=] () {
//            cerr << "chunk nr " << num_chunk << ": zdumpowano " << records.size() << " rekordów.\n";
            return handle_chunks(f, fsize, num_chunk + 1);
        });
    });
}

/**
 * Plik wejśćiowy mamy podzielony na bloki o rozmiarze maksymalnie RAM_AVAILABLE (./tmp/chunkN),
 * w fazie mergowania bierzemy po kawałku każdego z bloków, następnie szukamy elementów
 * globalnie najmniejszych, które dołączamy do wynikowego bloku, który następnie zapiszemy na dysk
 * (wynikowy plik jest konkatenacją tychże bloków)
 * @return wektor nazw plików, które należy skonkatenować, by otrzymać posortowany plik wejściowy.
 */
future<> merge_phase(uint64_t chunks) {
    uint64_t recs_per_chunk =
    return make_ready_future<>();
}

future<> f() {
    static sstring fname(FNAME);
    static uint64_t i = 0;
    return open_file_dma(fname, open_flags::rw).then([](file f) {
        return f.size().then([f](uint64_t fsize) mutable {
            return handle_chunks(f, fsize).then([](uint64_t chunks){
                cerr << boost::format("stworzono %1% chunkow") % chunks;
                return merge_phase(chunks);
            });
        });
    });
}

int compute(int argc, char **argv) {
    app_template app;
    try {
        app.run(argc, argv, f);
    } catch (...) {
        std::cerr << "Couldn't start application: ";
        return 1;
    }
    return 0;
}

int main(int argc, char **argv) {
    static_assert(RAM_AVAILABLE % RECORD_SIZE == 0,
            "Dostępny RAM musi być wielokrotnością rozmiaru rekordu!");
    static_assert(MERGED_BLOCK_SIZE * RECORD_SIZE < RAM_AVAILABLE / 2,
            "Dokup RAMu albo zmniejsz MERGED_BLOCK_SIZE!");
    compute(argc, argv);
}

