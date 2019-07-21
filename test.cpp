#include <seastar/core/sleep.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/file.hh>
#include "seastar/core/reactor.hh"
#include "seastar/core/sstring.hh"
// #include "seastar/core/aligned_buffer.hh"
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/future-util.hh>

#include <iostream>
#include <stdexcept>
#include <algorithm>
#include <deque>
#include <vector>

#include <boost/format.hpp>

using namespace seastar;
using namespace std;

static const string FNAME = string("./lol");
static const string TMP_DIR = string("./tmp/");
static const string OUT_DIR = string("./out/");


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

using recordVector = vector<string>;

static const uint64_t RECORD_SIZE = 4096;
//static const size_t RECORD_SIZE = 4;
//static const size_t RAM_AVAILABLE = RECORD_MAX_SIZE * 256 * 1024 * 4; // preprocessing chunk size (4GB)
//static const size_t RAM_AVAILABLE = 8; // TODO nie to
static const size_t RAM_AVAILABLE = RECORD_SIZE * 60; // TODO nie to

static const uint64_t MERGING_BLOCK_NUM_RECORDS = RAM_AVAILABLE / 10 / RECORD_SIZE; // number of records in merge-results chunks

static recordVector INPUT_RECORDS{}; // used in splitting-into-chunks preproccessing phase for keeping records to be dumped

static vector<uint64_t > CHUNK_SIZES; // CHUNK_SIZES[i] contains number of records in ./tmp/chunk<i> file

static vector<recordVector> CHUNKS; // used in merging phase, CHUNKS[i] contains

                                       // vector of some records from tmp/chunk<i> file

static recordVector OUTPUT_BATCH; // contains part of resulting file (it is to-be-concatenate)

static vector<uint64_t> TO_BE_TAKEN; // number of record from CHUNKS[i] to be taken in next disc load

static vector<string> OUT_FNAMES; // output (to-be-merged) filenames

// możemy czytać bezpiecznie do `read_to - 1`
future<> read_records(file f, recordVector *save_to, uint64_t start_from, uint64_t read_to) {
    if (read_to < start_from + RECORD_SIZE) {
        return make_ready_future();
    }
    // there should be nothing left, if it is we just discard it.
    return f.dma_read_exactly<char>(start_from, RECORD_SIZE).then([=](temporary_buffer<char> buf) {
        // zrobiłem to kopiowanie, bo zarówno buf.prefix jak i buf.trim
        // jedyne co robią to _size = len, nie zmieniają bufora.
        string trimmed(buf.get(), RECORD_SIZE);
        assert(trimmed.size() == RECORD_SIZE);
        assert(trimmed.length() == RECORD_SIZE);
//        cerr << "wczytalem " << trimmed.get() << endl;
        save_to->emplace_back(move(trimmed));
        return read_records(f, save_to, start_from + RECORD_SIZE, read_to);
    });
}

// pos - pozycja od której zaczynamy czytanie
// po przeczytaniu znowu będziemy wołać read_chunk dla pos2 := pos + RAM_AVAILABLE,
// zatem read_to w funkcji wyżej musi być pos + RAM_AVAILABLE (bo read() czyta do read_to - 1)
future<> read_chunk(file f, uint64_t pos, uint64_t fsize) {
    assert(INPUT_RECORDS.empty());
    uint64_t read_to = min(pos + RAM_AVAILABLE, fsize);
//    pos + RAM_AVAILABLE;
//    if (read_to > fsize) {
//        return make_ready_future();
//    }
//    return call(read_records, f, pos, read_to);
    return read_records(f, &INPUT_RECORDS, pos, read_to);
}


future<> dump_records(file f, recordVector * records, uint64_t num_record = 0) {
    uint64_t write_pos = num_record * RECORD_SIZE;
    if (num_record > records->size() - 1)
        return make_ready_future();
    return f.dma_write<char>(write_pos, (*records)[num_record].c_str(), RECORD_SIZE).then([=](size_t num) {
        assert(num == RECORD_SIZE);
        return dump_records(f, records, num_record + 1);
    });
}


inline sstring tmp_chunk_name(uint64_t num_chunk) {
    return sstring(TMP_DIR + string("chunk") + to_string(num_chunk));
}

future<> dump_sorted_chunk(uint64_t num_chunk) {
    sstring fname = tmp_chunk_name(num_chunk);
//    cerr << records.size() << "\n";
    return open_file_dma(fname, open_flags::rw | open_flags::create).then([](file f) {
        CHUNK_SIZES.emplace_back(INPUT_RECORDS.size());
        return dump_records(f, &INPUT_RECORDS);
    });
}


// returns number of created chunks, ie. 7 for chunk0, chunk1, ..., chunk6 created
future<uint64_t> handle_chunks(file f, uint64_t fsize, uint64_t num_chunk = 0) {
    INPUT_RECORDS.clear();
    uint64_t start_from = num_chunk * RAM_AVAILABLE;
//    if (start_from + RAM_AVAILABLE > fsize)
//        return make_ready_future();
    if (start_from >= fsize)
        return make_ready_future<uint64_t>(num_chunk);
    return read_chunk(f, start_from, fsize).then([=](){
        // sorting order is reversed, because it is easier to
        // handle merge sort in further steps
        sort(INPUT_RECORDS.begin(), INPUT_RECORDS.end());
        return dump_sorted_chunk(num_chunk).then([=] () {
//            cerr << "chunk nr " << num_chunk << ": zdumpowano " << records.size() << " rekordów.\n";
            return handle_chunks(f, fsize, num_chunk + 1);
        });
    });
}


// może wczytać 0 lub więcej rekordów, maksymalnie `num_rec`
future<> read_specific_record_num(file f, uint64_t fsize, recordVector *save_to,
        uint64_t num_rec, uint64_t start_reading_from_rec = 0) {

    uint64_t start_pos = start_reading_from_rec * RECORD_SIZE;
    uint64_t read_to = min(fsize, start_pos + num_rec * RECORD_SIZE);
    return read_records(f, save_to, start_pos, read_to);
}

/**
 * @param chunk number of chunk we read from
 * @param num_records number of records to be read
 * @param starting_record record we start reading from
 */
future<> read_chunk_file(uint64_t chunk, uint64_t num_records) {
    cerr << "#########zostalem zawolany dla chunk = " << chunk << ", num_rec=" << num_records << endl;
    cout << TO_BE_TAKEN << endl;
    sstring fname = tmp_chunk_name(chunk);
    return open_file_dma(fname, open_flags::rw).then([=](file f) mutable {
        return f.size().then([=](uint64_t fsize) mutable {
            assert(CHUNKS[chunk].empty());
            return read_specific_record_num(f, fsize, &CHUNKS[chunk], num_records, TO_BE_TAKEN[chunk]).then([=](){
                reverse(CHUNKS[chunk].begin(), CHUNKS[chunk].end());
                TO_BE_TAKEN[chunk] += CHUNKS[chunk].size();
                cout << "auuuu" << TO_BE_TAKEN << endl;
                return make_ready_future<>();
            });
        });
    });
}


inline sstring out_chunk_name(uint64_t num_chunk) {
    return sstring(OUT_DIR + string("chunk") + to_string(num_chunk));
}


future<stop_iteration> dump_output() {
    static uint64_t chunk_num = 0;
    sstring fname = out_chunk_name(chunk_num);
    cout << "####### -> nazbieralo sie " << OUTPUT_BATCH.size() << " elementow, dumpuje je.\n";
    OUT_FNAMES.push_back(fname);
    return open_file_dma(fname, open_flags::rw | open_flags::create).then([](file f) {
        return dump_records(f, &OUTPUT_BATCH).then([](){
            OUTPUT_BATCH.clear();
            chunk_num++;
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    });
}

future<stop_iteration> extract_min(uint64_t records_per_chunk) {
    int64_t min_idx = -1;
    bool sth_left = false;
    for (uint64_t i = 0; i < CHUNKS.size(); i++) {
        if (!CHUNKS[i].empty()) {
            sth_left = true;
            if (min_idx < 0) {
                min_idx = i;
            }
            else if (CHUNKS[i].back() < CHUNKS[min_idx].back()) {
                min_idx = i;
            }
        }
    }
    if (!sth_left) {
        cout << "1111\n";
        if (!OUTPUT_BATCH.empty()) {
            return dump_output();
        }
        return make_ready_future<stop_iteration>(stop_iteration::yes);
    }

    OUTPUT_BATCH.push_back(CHUNKS[min_idx].back());
    CHUNKS[min_idx].pop_back();
    if (CHUNKS[min_idx].empty()) {
        // need to load more data from disk
        cout << "2222\n";
        return read_chunk_file(min_idx, records_per_chunk).then([](){
            if (OUTPUT_BATCH.size() == MERGING_BLOCK_NUM_RECORDS) {
                cout << "2137\n";
                return dump_output();
            }
            cout << "3333\n";
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    }
    if (OUTPUT_BATCH.size() == MERGING_BLOCK_NUM_RECORDS) {
        cout << "4444\n";
        return dump_output();
    }
    cout << "5555\n";
    return make_ready_future<stop_iteration>(stop_iteration::no);
}

future<> init_merge_phase(uint64_t chunks, uint64_t records_per_chunk) {
    vector<uint64_t> tmp(chunks);
    uint64_t i = 0;
    for(uint64_t &el : tmp)
        el = i++;
    if (chunks > 1)
        assert(tmp[1] == 1);
    return parallel_for_each(tmp.begin(), tmp.end(), [=](uint64_t num_chunk){
        return read_chunk_file(num_chunk, records_per_chunk);
    });
}

/**
 * Plik wejśćiowy mamy podzielony na bloki o rozmiarze maksymalnie RAM_AVAILABLE (./tmp/chunkN),
 * w fazie mergowania bierzemy po kawałku każdego z bloków, następnie szukamy elementów
 * globalnie najmniejszych, które dołączamy do wynikowego bloku, który następnie zapiszemy na dysk
 * (wynikowy plik jest konkatenacją tychże bloków)
 * @param chunks ilośc stworzonych plików chunksN
 * @return wektor nazw plików, które należy skonkatenować, by otrzymać posortowany plik wejściowy.
 */
future<vector<string>> merge_phase(uint64_t chunks) {
    uint64_t RAM_FOR_CHUNKS = RAM_AVAILABLE - MERGING_BLOCK_NUM_RECORDS * RECORD_SIZE;
    uint64_t RECS_PER_CHUNK = RAM_FOR_CHUNKS / chunks / RECORD_SIZE;
    assert(RECS_PER_CHUNK > 0);
    CHUNKS = vector<recordVector>(chunks);
    TO_BE_TAKEN = vector<uint64_t>(chunks);
    OUTPUT_BATCH = recordVector();
    OUT_FNAMES = vector<string>();
    return init_merge_phase(chunks, RECS_PER_CHUNK).then([=](){
//        for (auto el : CHUNKS) {
//            cout << el << endl;
//        }
        return repeat([=] () {return extract_min(RECS_PER_CHUNK);}).then([](){
            return make_ready_future<vector<string>>(OUT_FNAMES);
        });
//        return make_ready_future<>();

    });
//     TODO vector<string>
}


future<> external_sort() {
    static sstring fname(FNAME);
    static uint64_t i = 0;
    return open_file_dma(fname, open_flags::rw).then([](file f) {
        return f.size().then([f](uint64_t fsize) mutable {
            return handle_chunks(f, fsize).then([](uint64_t chunks){
                cerr << boost::format("stworzono %1% chunkow") % chunks;
                return merge_phase(chunks).then([](vector<string>){
                    return make_ready_future<>();
                });
            });
        });
    });
}

int compute(int argc, char **argv) {
    app_template app;
    try {
        app.run(argc, argv, external_sort);
    } catch (...) {
        std::cerr << "Couldn't start application: ";
        return 1;
    }
    return 0;
}


/**
 * TODO:
 * assert fsize % RECORD_SIZE == 0
 * read_specific_record_num powinno zwracać ile przeczytało
 */
int main(int argc, char **argv) {
    static_assert(RAM_AVAILABLE % RECORD_SIZE == 0,
            "Dostępny RAM musi być wielokrotnością rozmiaru rekordu!");
    static_assert(MERGING_BLOCK_NUM_RECORDS * RECORD_SIZE < RAM_AVAILABLE / 2,
            "Dokup RAMu albo zmniejsz MERGING_BLOCK_NUM_RECORDS!");
    compute(argc, argv);
}

