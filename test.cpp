#include <seastar/core/app-template.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/file.hh>
#include "seastar/core/reactor.hh"
#include "seastar/core/sstring.hh"
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/future-util.hh>

#include <iostream>
#include <algorithm>
#include <vector>



/*!
 *  \author    Mateusz Biegański
 *
 *  OPIS ROZWIĄZANIA:
 *
 *  Moja implementacja sortowania składa się z trzech faz:
 *  * wczytywanie i sortowanie mniejszych kawałków (PREPROCESSING)
 *  * sortowanie mniejszych bloków i zapisywanie ich do pomniejszych plików (MERGE)
 *  * scalanie wynikowych plików w jeden (OUTPUT MERGING)
 *
 *  Wejściowy plik wczytywany i sortowany jest w kawałkach na tyle dużych, żeby zmaksymalizować
 *  zużycie pamięci, nie przekraczając jednak wartości `RAM_AVAILABLE`. W następnej fazie pliki
 *  są ładowane z dysku, a następnie zapisywane w ostatecznej kolejności, uprzednio sortowane przez scalanie
 *  (k-way mergesort). W ostatniej fazie następuje scalanie posortowanych bloków w plkik wynikowy
 *  (wielowątkowe scalanie parami w czasie N*logN)
 *
 *
 *  ZAŁOŻENIA:
 *  * rekordy w pliku wejściowym nie są oddzielone żadnym znakiem, tj
 *  plik zawierający dwa rekordy ma dokładnie 8192 znaki.
 *  * plik wejściowy jest dobrze uformowany, tj. zawiera pełne rekordy.
 */

using namespace seastar;
using namespace std;


static const string FNAME = string("./input.txt"); // TODO tu należy wpisać nazwę pliku wejściowego


static const string TMP_DIR = string("./tmp/");
static const string OUT_DIR = string("./out/");


using recordVector = std::vector<string>;

static const uint64_t RECORD_SIZE = 4096;

static const size_t RAM_AVAILABLE = RECORD_SIZE * 256 * 1024 * 4; // preprocessing chunk size (4GB)

static const uint64_t MERGING_BLOCK_NUM_RECORDS = RAM_AVAILABLE / RECORD_SIZE / 10; // number of records in merge-results chunks



static recordVector INPUT_RECORDS{}; // used in splitting-into-chunks preproccessing phase for keeping records to be dumped

static std::vector<uint64_t > CHUNK_SIZES; // CHUNK_SIZES[i] contains number of records in ./tmp/chunk<i> file

static std::vector<recordVector> CHUNKS; // used in merging phase, CHUNKS[i] contains

                                       // vector of some records from tmp/chunk<i> file

static recordVector OUTPUT_BATCH; // contains part of resulting file (it is to-be-concatenate)

static std::vector<uint64_t> TO_BE_TAKEN; // number of record from CHUNKS[i] to be taken in next disc load

static std::vector<string> OUT_FNAMES; // output (to-be-merged) filenames

static std::vector<recordVector> OUT_MERGE_BUFFER; // here are put records from to-be-merged output files

static uint64_t MAX_RECORDS_PER_WORKER = 0; // during output merging, how many records may I buffer




// możemy czytać bezpiecznie do `read_to - 1`
future<> read_records(file f, recordVector *save_to, uint64_t start_from, uint64_t read_to) {
    if (read_to < start_from + RECORD_SIZE) {
        return make_ready_future();
    }
    return f.dma_read_exactly<char>(start_from, RECORD_SIZE).then([=](temporary_buffer<char> buf) {
        // zrobiłem to kopiowanie, bo zarówno buf.prefix jak i buf.trim
        // jedyne co robią to _size = len, nie zmieniają bufora.
        string trimmed(buf.get(), RECORD_SIZE);
        assert(trimmed.size() == RECORD_SIZE);
        assert(trimmed.length() == RECORD_SIZE);
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
    return read_records(f, &INPUT_RECORDS, pos, read_to);
}

future<> dump_records_to_specific_pos(file f, recordVector * records, uint64_t starting_pos, uint64_t num_record = 0) {
    uint64_t write_pos = starting_pos + num_record * RECORD_SIZE;
    if (num_record > records->size() - 1)
        return make_ready_future();
    return f.dma_write<char>(write_pos, (*records)[num_record].c_str(), RECORD_SIZE).then([=](size_t num) {
        assert(num == RECORD_SIZE);
        return dump_records_to_specific_pos(f, records, starting_pos, num_record + 1);
    });
}

future<> dump_records(file f, recordVector * records) {
    return dump_records_to_specific_pos(f, records, 0, 0);
}


inline sstring tmp_chunk_name(uint64_t num_chunk) {
    return sstring(TMP_DIR + string("chunk") + to_string(num_chunk));
}

future<> dump_sorted_chunk(uint64_t num_chunk) {
    sstring fname = tmp_chunk_name(num_chunk);
    return open_file_dma(fname, open_flags::rw | open_flags::create).then([](file f) {
        CHUNK_SIZES.emplace_back(INPUT_RECORDS.size());
        return dump_records(f, &INPUT_RECORDS);
    });
}


// returns number of created chunks, ie. 7 for chunk0, chunk1, ..., chunk6 created
future<uint64_t> handle_chunks(file f, uint64_t fsize, uint64_t num_chunk = 0) {
    INPUT_RECORDS.clear();
    uint64_t start_from = num_chunk * RAM_AVAILABLE;
    if (start_from >= fsize)
        return make_ready_future<uint64_t>(num_chunk);
    return read_chunk(f, start_from, fsize).then([=](){
        sort(INPUT_RECORDS.begin(), INPUT_RECORDS.end());
        return dump_sorted_chunk(num_chunk).then([=] () {
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
    sstring fname = tmp_chunk_name(chunk);
    return open_file_dma(fname, open_flags::rw).then([=](file f) mutable {
        return f.size().then([=](uint64_t fsize) mutable {
            assert(CHUNKS[chunk].empty());
            return read_specific_record_num(f, fsize, &CHUNKS[chunk], num_records, TO_BE_TAKEN[chunk]).then([=](){
                reverse(CHUNKS[chunk].begin(), CHUNKS[chunk].end());
                TO_BE_TAKEN[chunk] += CHUNKS[chunk].size();
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
        if (!OUTPUT_BATCH.empty()) {
            return dump_output();
        }
        return make_ready_future<stop_iteration>(stop_iteration::yes);
    }

    OUTPUT_BATCH.push_back(CHUNKS[min_idx].back());
    CHUNKS[min_idx].pop_back();
    if (CHUNKS[min_idx].empty()) {
        // need to load more data from disk
        return read_chunk_file(min_idx, records_per_chunk).then([](){
            if (OUTPUT_BATCH.size() == MERGING_BLOCK_NUM_RECORDS) {
                return dump_output();
            }
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    }
    if (OUTPUT_BATCH.size() == MERGING_BLOCK_NUM_RECORDS) {
        return dump_output();
    }
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
        return repeat([=] () {return extract_min(RECS_PER_CHUNK);}).then([](){
            return make_ready_future<vector<string>>(OUT_FNAMES);
        });
    });
}


void prepareMergePhase(uint64_t num_workers) {
    MAX_RECORDS_PER_WORKER = RAM_AVAILABLE / RECORD_SIZE / num_workers;
    assert(MAX_RECORDS_PER_WORKER > 0 && "Cannot perform multithread merging! Not enough RAM amount");
    for (auto el : OUT_MERGE_BUFFER) {
        el.clear();
    }
    OUT_MERGE_BUFFER.clear();
    for (uint64_t i = 0; i < num_workers; i++) {
        OUT_MERGE_BUFFER.emplace_back(recordVector());
    }
    TO_BE_TAKEN.clear();
    for (uint64_t i = 0; i < num_workers; i++) {
        TO_BE_TAKEN.emplace_back(0);
    }
}


future<> read_and_dump_output_batch(file f1, uint64_t fsize1, file f2, uint64_t fsize2, uint64_t my_num_worker) {
    return read_specific_record_num(f2, fsize2, &OUT_MERGE_BUFFER[my_num_worker],
            MAX_RECORDS_PER_WORKER, TO_BE_TAKEN[my_num_worker]).then([=]() {
                uint64_t records_written = OUT_MERGE_BUFFER[my_num_worker].size();
                if (records_written == 0)
                    return make_ready_future<>();
                TO_BE_TAKEN[my_num_worker] += OUT_MERGE_BUFFER[my_num_worker].size();
                return dump_records_to_specific_pos(f1, &OUT_MERGE_BUFFER[my_num_worker], fsize1).then([=](){
                    OUT_MERGE_BUFFER[my_num_worker].clear();
                    return read_and_dump_output_batch(f1, fsize1 + records_written * RECORD_SIZE, f2, fsize2, my_num_worker);
                });
            });
}


// appends fname2 content to fname1
future<> merge2(string fname1, string fname2, uint64_t my_num_worker) {
    return open_file_dma(fname1, open_flags::rw).then([=](file f1) mutable {
        return f1.size().then([=](uint64_t fsize1) mutable {
            return open_file_dma(fname2, open_flags::rw).then([=](file f2) mutable {
                return f2.size().then([=](uint64_t fsize2) mutable {
                    assert(fsize1 % RECORD_SIZE == 0);
                    assert(fsize2 % RECORD_SIZE == 0);
                    return read_and_dump_output_batch(f1, fsize1, f2, fsize2, my_num_worker).then([=](){
                        return remove_file(fname2);
                    });
                });
            });
        });
    });
}



// Mergowanie wynikowych plików odbywa się równolegle, w każdej fazie
// dla N plików pracuje N / 2 wątków. (liczba plików zmniejsza się z czasem)
future<> do_merging(vector<string> fnames) {
    vector<uint64_t> first_indices;
    size_t out_files_num = fnames.size();
    assert (out_files_num > 1);

    for (uint64_t i = 0; i < out_files_num; i++) {
        if (i % 2 == 0 && i != out_files_num - 1)
            first_indices.push_back(i);
    }
    prepareMergePhase(first_indices.size());
    return parallel_for_each(first_indices.begin(), first_indices.end(), [=](uint64_t num_first){
        return merge2(fnames[num_first], fnames[num_first + 1], num_first / 2);
    });
}


future<> merge_output_files(vector<string> fnames) {
    if (fnames.size() == 1) {
        return make_ready_future<>();
    }
    return do_merging(fnames).then([=](){
        vector<string> trimmed_fnames;
        uint64_t i = 0;
        for (auto el : fnames) {
            if (i % 2 == 0) {
                trimmed_fnames.emplace_back(fnames[i]);
            }
            i++;
        }
        return merge_output_files(trimmed_fnames);
    });
}


future<> external_sort() {
    static sstring fname(FNAME);
    static uint64_t i = 0;
    return open_file_dma(fname, open_flags::rw).then([](file f) {
        return f.size().then([f](uint64_t fsize) mutable {
            return handle_chunks(f, fsize).then([](uint64_t chunks){
                return merge_phase(chunks).then([](vector<string> fnames){
                    return merge_output_files(fnames);
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
 * assert fsize % RECORD_SIZE == 0, gdzie fsize jest wejściowym plikiem
 */
int main(int argc, char **argv) {
    static_assert(RAM_AVAILABLE % RECORD_SIZE == 0,
            "Dostępny RAM musi być wielokrotnością rozmiaru rekordu!");
    static_assert(MERGING_BLOCK_NUM_RECORDS * RECORD_SIZE < RAM_AVAILABLE / 2,
            "Dokup RAMu albo zmniejsz MERGING_BLOCK_NUM_RECORDS!");
    compute(argc, argv);
    cout << "Posortowany plik znajduje się w katalogu " << OUT_DIR << " pod nazwą chunk0" << endl;
    return 0;
}

